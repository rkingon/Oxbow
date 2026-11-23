using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using Oxbow.Internal;

namespace Oxbow.Processing;

/// <summary>A state change for a job that is still running. Fields left null aren't updated.</summary>
internal sealed record SetStateParams(long Id, JobState State)
{
    public int? Attempt { get; init; }

    public string? Error { get; init; }

    public DateTimeOffset? FinalizedAt { get; init; }

    public string? MetadataUpdates { get; init; }

    public DateTimeOffset? ScheduledAt { get; init; }
}

/// <summary>
/// River's batch completer: attempt outcomes are queued and written in batches with the set-state-if-running query, so
/// finishing a job doesn't cost a round trip of its own. A job's slot is freed as soon as its outcome is queued; the job
/// stays running in the database until the batch is written, so it can't be fetched again in between. A node that dies
/// with queued outcomes leaves those jobs running for the rescuer.
/// </summary>
internal sealed class Completer(OxbowRuntime runtime, ILogger logger)
{
    private const int MaxBatch = 5_000;
    private const int MaxBacklog = 20_000;
    private const int MaxTries = 3;
    private static readonly TimeSpan FlushInterval = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    // Bounded so a database outage slows workers down instead of growing memory without limit.
    private readonly Channel<Pending> _queue = Channel.CreateBounded<Pending>(
        new BoundedChannelOptions(MaxBacklog) { SingleReader = true, FullMode = BoundedChannelFullMode.Wait });

    /// <summary>Queues an outcome. The task completes when it's written, with the job's resulting state.</summary>
    public async ValueTask<Task<JobState?>> EnqueueAsync(SetStateParams update)
    {
        var pending = new Pending(update, new TaskCompletionSource<JobState?>(TaskCreationOptions.RunContinuationsAsynchronously));
        await _queue.Writer.WriteAsync(pending).ConfigureAwait(false);
        return pending.Written.Task;
    }

    /// <summary>Writes queued outcomes until <see cref="Complete"/> is called and the queue is drained.</summary>
    public async Task RunAsync()
    {
        var reader = _queue.Reader;
        var batch = new List<Pending>(MaxBatch);
        while (await reader.WaitToReadAsync().ConfigureAwait(false))
        {
            var deadline = Stopwatch.GetTimestamp() + (long)(FlushInterval.TotalSeconds * Stopwatch.Frequency);
            while (batch.Count < MaxBatch)
            {
                if (reader.TryRead(out var pending))
                {
                    batch.Add(pending);
                    continue;
                }

                var remaining = deadline - Stopwatch.GetTimestamp();
                if (remaining <= 0)
                {
                    break;
                }

                using var wait = new CancellationTokenSource(TimeSpan.FromSeconds((double)remaining / Stopwatch.Frequency));
                try
                {
                    if (!await reader.WaitToReadAsync(wait.Token).ConfigureAwait(false))
                    {
                        break;
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            await WriteAsync(batch).ConfigureAwait(false);
            batch.Clear();
        }
    }

    /// <summary>Stops accepting outcomes; <see cref="RunAsync"/> returns once the queue is written.</summary>
    public void Complete() => _queue.Writer.TryComplete();

    private async Task WriteAsync(List<Pending> batch)
    {
        for (var attempt = 1; ; attempt++)
        {
            using var timeout = new CancellationTokenSource(Timeout);
            try
            {
                var (states, freedSequences) = await ExecuteAsync(batch, timeout.Token).ConfigureAwait(false);
                foreach (var pending in batch)
                {
                    pending.Written.TrySetResult(states.TryGetValue(pending.Update.Id, out var state) ? state : null);
                }

                await PromoteAsync(freedSequences).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt < MaxTries && ex is NpgsqlException or OperationCanceledException)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(200 * attempt * attempt), CancellationToken.None).ConfigureAwait(false);
            }
#pragma warning disable CA1031 // Losing a batch must not stop the completer; the rescuer recovers the jobs.
            catch (Exception ex)
#pragma warning restore CA1031
            {
                foreach (var pending in batch)
                {
                    Log.CompleteFailed(logger, ex, pending.Update.Id);
                    pending.Written.TrySetResult(null);
                }

                return;
            }
        }
    }

    /// <summary>A finished head frees its sequence; promoting here instead of at the leader's sweep keeps the gap short.</summary>
    private async Task PromoteAsync(HashSet<string> keys)
    {
        if (keys.Count == 0)
        {
            return;
        }

        using var timeout = new CancellationTokenSource(Timeout);
        try
        {
            await SequencePromoter.PromoteAsync(runtime, keys, timeout.Token).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // The leader's sweep promotes whatever this misses.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            Log.MaintenanceFailed(logger, ex, "sequencer");
        }
    }

    private async Task<(Dictionary<long, JobState> States, HashSet<string> FreedSequences)> ExecuteAsync(
        List<Pending> batch,
        CancellationToken cancellationToken)
    {
        var updates = batch.Select(p => p.Update).ToArray();
        var placeholderTime = DateTime.UnixEpoch;
        var command = runtime.DataSource.CreateCommand(runtime.Sql.SetStateIfRunning);
        await using (command.ConfigureAwait(false))
        {
            var p = command.Parameters;
            p.Add(new NpgsqlParameter<long[]>("id", updates.Select(u => u.Id).ToArray()));
            p.Add(new NpgsqlParameter<bool[]>("attempt_do_update", updates.Select(u => u.Attempt is not null).ToArray()));
            p.Add(new NpgsqlParameter<int[]>("attempt", updates.Select(u => u.Attempt ?? 0).ToArray()));
            p.Add(new NpgsqlParameter<bool[]>("errors_do_update", updates.Select(u => u.Error is not null).ToArray()));
            p.Add(OxbowClient.TextArray("errors", updates.Select(u => u.Error ?? "{}").ToArray()));
            p.Add(new NpgsqlParameter<bool[]>("finalized_at_do_update", updates.Select(u => u.FinalizedAt is not null).ToArray()));
            p.Add(new NpgsqlParameter("finalized_at", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)
            {
                Value = updates.Select(u => u.FinalizedAt?.UtcDateTime ?? placeholderTime).ToArray(),
            });
            p.Add(new NpgsqlParameter<bool[]>("metadata_do_merge", updates.Select(u => u.MetadataUpdates is not null).ToArray()));
            p.Add(OxbowClient.TextArray("metadata_updates", updates.Select(u => u.MetadataUpdates ?? "{}").ToArray()));
            p.Add(new NpgsqlParameter<bool[]>("scheduled_at_do_update", updates.Select(u => u.ScheduledAt is not null).ToArray()));
            p.Add(new NpgsqlParameter("scheduled_at", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)
            {
                Value = updates.Select(u => u.ScheduledAt?.UtcDateTime ?? placeholderTime).ToArray(),
            });
            p.Add(OxbowClient.TextArray("state", updates.Select(u => JobStates.ToDb(u.State)).ToArray()));

            var states = new Dictionary<long, JobState>(updates.Length);
            var freedSequences = new HashSet<string>(StringComparer.Ordinal);
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var state = JobStates.Parse(reader.GetString(1));
                    states[reader.GetInt64(0)] = state;
                    if ((state is JobState.Completed or JobState.Cancelled or JobState.Discarded) && !reader.IsDBNull(4))
                    {
                        freedSequences.Add(reader.GetString(4));
                    }
                }
            }

            return (states, freedSequences);
        }
    }

    private sealed record Pending(SetStateParams Update, TaskCompletionSource<JobState?> Written);
}

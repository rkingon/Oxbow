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

/// <summary>Records attempt outcomes with River's set-state-if-running query, retrying transient failures.</summary>
internal sealed class Completer(OxbowRuntime runtime, ILogger logger)
{
    private const int MaxTries = 3;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    public async Task<JobState?> SetStateAsync(SetStateParams update)
    {
        for (var attempt = 1; ; attempt++)
        {
            using var timeout = new CancellationTokenSource(Timeout);
            try
            {
                return await ExecuteAsync(update, timeout.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < MaxTries && ex is NpgsqlException or OperationCanceledException)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100 * attempt * attempt), CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.CompleteFailed(logger, ex, update.Id);
                return null;
            }
        }
    }

    private async Task<JobState?> ExecuteAsync(SetStateParams update, CancellationToken cancellationToken)
    {
        var placeholderTime = DateTime.UnixEpoch;
        var command = runtime.DataSource.CreateCommand(runtime.Sql.SetStateIfRunning);
        await using (command.ConfigureAwait(false))
        {
            var p = command.Parameters;
            p.Add(new NpgsqlParameter<long[]>("id", [update.Id]));
            p.Add(new NpgsqlParameter<bool[]>("attempt_do_update", [update.Attempt is not null]));
            p.Add(new NpgsqlParameter<int[]>("attempt", [update.Attempt ?? 0]));
            p.Add(new NpgsqlParameter<bool[]>("errors_do_update", [update.Error is not null]));
            p.Add(OxbowClient.TextArray("errors", [update.Error ?? "{}"]));
            p.Add(new NpgsqlParameter<bool[]>("finalized_at_do_update", [update.FinalizedAt is not null]));
            p.Add(new NpgsqlParameter("finalized_at", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)
            {
                Value = new[] { update.FinalizedAt?.UtcDateTime ?? placeholderTime },
            });
            p.Add(new NpgsqlParameter<bool[]>("metadata_do_merge", [update.MetadataUpdates is not null]));
            p.Add(OxbowClient.TextArray("metadata_updates", [update.MetadataUpdates ?? "{}"]));
            p.Add(new NpgsqlParameter<bool[]>("scheduled_at_do_update", [update.ScheduledAt is not null]));
            p.Add(new NpgsqlParameter("scheduled_at", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)
            {
                Value = new[] { update.ScheduledAt?.UtcDateTime ?? placeholderTime },
            });
            p.Add(OxbowClient.TextArray("state", [JobStates.ToDb(update.State)]));

            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
                    ? JobStates.Parse(reader.GetString(1))
                    : null;
            }
        }
    }
}

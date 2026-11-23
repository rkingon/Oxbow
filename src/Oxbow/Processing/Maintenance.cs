using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using Oxbow.Internal;

namespace Oxbow.Processing;

/// <summary>The leader's maintenance services, started on gaining leadership and stopped on losing it.</summary>
internal sealed class Maintenance(
    OxbowRuntime runtime,
    WorkerRegistry workers,
    OxbowClient client,
    IServiceScopeFactory scopes,
    IRetryPolicy retryPolicy,
    TimeProvider timeProvider,
    Action<string> onQueueAvailable,
    DiscardHooks discardHooks,
    ILogger logger,
    ChannelReader<string>? signalledSequences = null)
{
    private const int BatchSize = 1_000;
    private static readonly TimeSpan QueueCleanerInterval = TimeSpan.FromHours(1);
    private static readonly TimeSpan QueueRetention = TimeSpan.FromHours(24);
    private static readonly TimeSpan DepthInterval = TimeSpan.FromSeconds(10);
    private readonly PeriodicEnqueuer _periodic = new(runtime, client, timeProvider, logger);

    /// <summary>Periodic jobs changed; while leading, re-read them now.</summary>
    public void WakePeriodic() => _periodic.Wake();

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.WhenAll(
                EveryAsync("scheduler", runtime.Options.SchedulerInterval, ScheduleAsync, cancellationToken, runImmediately: true),
                EveryAsync("rescuer", runtime.Options.RescuerInterval, RescueAsync, cancellationToken),
                EveryAsync("cleaner", runtime.Options.CleanerInterval, CleanAsync, cancellationToken),
                EveryAsync("queue-cleaner", QueueCleanerInterval, CleanQueuesAsync, cancellationToken),
                EveryAsync("rate-limit-cleaner", runtime.Options.CleanerInterval, CleanRateLimitsAsync, cancellationToken),
                EveryAsync("batch-finisher", runtime.Options.BatchFinisherInterval, FinishBatchesAsync, cancellationToken, runImmediately: true),
                EveryAsync("batch-cleaner", runtime.Options.CleanerInterval, CleanBatchesAsync, cancellationToken),
                EveryAsync("queue-depth", DepthInterval, MeasureDepthAsync, cancellationToken, runImmediately: true),
                EveryAsync("sequencer", runtime.Options.SchedulerInterval, PromoteSequencesAsync, cancellationToken, runImmediately: true),
                PromoteSignalledSequencesAsync(cancellationToken),
                _periodic.RunAsync(cancellationToken)).ConfigureAwait(false);
        }
        finally
        {
            OxbowDiagnostics.SetQueueDepth([]);
        }
    }

    internal async Task ScheduleAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            var command = runtime.DataSource.CreateCommand(runtime.Sql.Schedule);
            var queues = new HashSet<string>(StringComparer.Ordinal);
            var discarded = new List<long>();
            var count = 0;
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.AddWithValue("max", BatchSize);
                var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        count++;
                        switch (reader.GetString(1))
                        {
                            case "available":
                                queues.Add(reader.GetString(0));
                                break;
                            case "discarded":
                                discarded.Add(reader.GetInt64(2));
                                break;
                        }
                    }
                }
            }

            // Only once the reader is disposed is the statement committed; handlers must not read it earlier.
            foreach (var id in discarded)
            {
                discardHooks.Enqueue(new Discard(id, JobDiscardReason.UniqueKeyConflict));
            }

            if (queues.Count > 0)
            {
                await NotifyQueuesAsync(queues, cancellationToken).ConfigureAwait(false);
            }

            if (count < BatchSize)
            {
                return;
            }
        }
    }

    private async Task NotifyQueuesAsync(IReadOnlyCollection<string> queues, CancellationToken cancellationToken)
    {
        foreach (var queue in queues)
        {
            onQueueAvailable(queue);
        }

        var command = runtime.DataSource.CreateCommand(runtime.Sql.Notify);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("channel", runtime.Sql.InsertChannel);
            command.Parameters.Add(OxbowClient.TextArray("payloads", queues
                .Select(q => JsonSerializer.Serialize(new InsertPayload(q), OxbowJsonContext.Default.InsertPayload))
                .ToArray()));
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The backstop for every path that frees a sequence without promoting its successor: missed notifications, the
    /// rescuer and scheduler, and nodes on releases without sequences.
    /// </summary>
    internal async Task PromoteSequencesAsync(CancellationToken cancellationToken)
    {
        var after = "";
        while (true)
        {
            var keys = new List<string>();
            var command = runtime.DataSource.CreateCommand(runtime.Sql.SequencePendingKeys);
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.AddWithValue("after", after);
                command.Parameters.AddWithValue("max", BatchSize);
                var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        keys.Add(reader.GetString(0));
                    }
                }
            }

            if (keys.Count == 0)
            {
                return;
            }

            await PromoteAsync(keys, cancellationToken).ConfigureAwait(false);
            if (keys.Count < BatchSize)
            {
                return;
            }

            after = keys[^1];
        }
    }

    /// <summary>Promotes sequences that inserts signalled, batching whatever arrives while a promotion runs.</summary>
    private async Task PromoteSignalledSequencesAsync(CancellationToken cancellationToken)
    {
        if (signalledSequences is null)
        {
            return;
        }

        var keys = new HashSet<string>(StringComparer.Ordinal);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (!await signalledSequences.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    return;
                }

                while (keys.Count < BatchSize && signalledSequences.TryRead(out var key))
                {
                    keys.Add(key);
                }

                await PromoteAsync(keys, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Log.MaintenanceFailed(logger, ex, "sequencer");
            }

            keys.Clear();
        }
    }

    private async Task PromoteAsync(IReadOnlyCollection<string> keys, CancellationToken cancellationToken)
    {
        foreach (var queue in await SequencePromoter.PromoteAsync(runtime, keys, cancellationToken).ConfigureAwait(false))
        {
            onQueueAvailable(queue);
        }
    }

    internal async Task RescueAsync(CancellationToken cancellationToken)
    {
        var now = timeProvider.GetUtcNow();
        var stuckHorizon = now - runtime.Options.RescueStuckJobsAfter;
        var afterId = 0L;
        var rescued = 0;
        while (true)
        {
            List<JobRow> stuck;
            var select = runtime.DataSource.CreateCommand(runtime.Sql.GetStuck);
            await using (select.ConfigureAwait(false))
            {
                select.Parameters.AddWithValue("stuck_horizon", stuckHorizon);
                select.Parameters.AddWithValue("after_id", afterId);
                select.Parameters.AddWithValue("max", BatchSize);
                stuck = await JobRowReader.ReadAllAsync(select, cancellationToken).ConfigureAwait(false);
            }

            if (stuck.Count == 0)
            {
                break;
            }

            afterId = stuck[^1].Id;
            var decisions = new List<(JobRow Job, JobState State, DateTimeOffset ScheduledAt, DateTimeOffset? FinalizedAt)>();
            foreach (var job in stuck)
            {
                if (await DecideAsync(job, now).ConfigureAwait(false) is { } decision)
                {
                    decisions.Add((job, decision.State, decision.ScheduledAt, decision.FinalizedAt));
                }
            }

            if (decisions.Count > 0)
            {
                var update = runtime.DataSource.CreateCommand(runtime.Sql.RescueMany);
                await using (update.ConfigureAwait(false))
                {
                    update.Parameters.Add(new NpgsqlParameter<long[]>("id", decisions.Select(d => d.Job.Id).ToArray()));
                    update.Parameters.Add(OxbowClient.TextArray("error", decisions
                        .Select(d => JsonSerializer.Serialize(
                            new AttemptError(now, d.Job.Attempt, "Stuck job rescued by Oxbow's rescuer.", null),
                            OxbowJsonContext.Default.AttemptError))
                        .ToArray()));
                    update.Parameters.Add(new NpgsqlParameter("finalized_at", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)
                    {
                        Value = decisions.Select(d => d.FinalizedAt is { } at ? (object)at.UtcDateTime : DBNull.Value).ToArray(),
                    });
                    update.Parameters.Add(new NpgsqlParameter("scheduled_at", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)
                    {
                        Value = decisions.Select(d => d.ScheduledAt.UtcDateTime).ToArray(),
                    });
                    update.Parameters.Add(OxbowClient.TextArray("state", decisions.Select(d => JobStates.ToDb(d.State)).ToArray()));
                    update.Parameters.AddWithValue("stuck_horizon", stuckHorizon);
                    foreach (var row in await JobRowReader.ReadAllAsync(update, cancellationToken).ConfigureAwait(false))
                    {
                        rescued++;
                        if (row.State is JobState.Discarded or JobState.Cancelled)
                        {
                            discardHooks.Enqueue(new Discard(
                                row.Id,
                                row.State == JobState.Discarded ? JobDiscardReason.AttemptsExhausted : JobDiscardReason.CancelRequested,
                                Row: row));
                        }
                    }
                }
            }

            if (stuck.Count < BatchSize)
            {
                break;
            }
        }

        if (rescued > 0)
        {
            Log.Rescued(logger, rescued);
        }
    }

    private async Task<(JobState State, DateTimeOffset ScheduledAt, DateTimeOffset? FinalizedAt)?> DecideAsync(JobRow job, DateTimeOffset now)
    {
        if (job.Metadata.ValueKind == JsonValueKind.Object && job.Metadata.TryGetProperty("cancel_attempted_at", out _))
        {
            return (JobState.Cancelled, job.ScheduledAt, now);
        }

        DateTimeOffset? nextRetry = null;
        if (workers.Find(job.Kind) is { } registration)
        {
            var scope = scopes.CreateAsyncScope();
            await using (scope.ConfigureAwait(false))
            {
                try
                {
                    var unit = registration.CreateUnit(job, scope.ServiceProvider, runtime.Json, runtime.ArgsProtector);
                    if (MayStillBeRunning(job, unit.GetTimeout() ?? runtime.Options.JobTimeout, now))
                    {
                        return null;
                    }

                    nextRetry = unit.GetNextRetry();
                }
                catch (JsonException)
                {
                    // Args that can't be deserialized are retried by the retry policy, then discarded.
                }
                catch (JobArgsDecryptionException) when (MayStillBeRunning(job, runtime.Options.JobTimeout, now))
                {
                    // Only this node may lack the key, so the job may be running elsewhere: hold to the default timeout.
                    return null;
                }
                catch (JobArgsDecryptionException)
                {
                }
            }
        }

        if (job.Attempt >= job.MaxAttempts)
        {
            return (JobState.Discarded, job.ScheduledAt, now);
        }

        return (JobState.Retryable, nextRetry ?? retryPolicy.NextRetry(job), null);
    }

    /// <summary>
    /// River: with no timeout, or one that hasn't elapsed, the job may still be legitimately running. GetStuck returns an
    /// attempt that heartbeats only once its heartbeat has gone quiet, so its timeout no longer says that.
    /// </summary>
    private static bool MayStillBeRunning(JobRow job, TimeSpan timeout, DateTimeOffset now)
    {
        var heartbeating = job.HeartbeatAt is { } heartbeatAt && job.AttemptedAt is { } started && heartbeatAt >= started;
        return !heartbeating
            && (OxbowRuntime.IsInfinite(timeout) || (job.AttemptedAt is { } attemptedAt && now - attemptedAt < timeout));
    }

    internal async Task CleanAsync(CancellationToken cancellationToken)
    {
        var now = timeProvider.GetUtcNow();
        while (true)
        {
            var command = runtime.DataSource.CreateCommand(runtime.Sql.DeleteFinalized);
            await using (command.ConfigureAwait(false))
            {
                AddRetention(command, "cancelled", runtime.Options.CancelledJobRetention, now);
                AddRetention(command, "completed", runtime.Options.CompletedJobRetention, now);
                AddRetention(command, "discarded", runtime.Options.DiscardedJobRetention, now);
                command.Parameters.AddWithValue("max", BatchSize);
                if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) < BatchSize)
                {
                    return;
                }
            }
        }
    }

    private static void AddRetention(NpgsqlCommand command, string state, TimeSpan retention, DateTimeOffset now)
    {
        var keepForever = OxbowRuntime.IsInfinite(retention);
        command.Parameters.AddWithValue($"{state}_do_delete", !keepForever);
        command.Parameters.AddWithValue($"{state}_horizon", keepForever ? now : now - retention);
    }

    internal async Task FinishBatchesAsync(CancellationToken cancellationToken)
    {
        var afterId = 0L;
        while (true)
        {
            var candidates = new List<long>();
            var select = runtime.DataSource.CreateCommand(runtime.Sql.BatchGetFinishable);
            await using (select.ConfigureAwait(false))
            {
                select.Parameters.AddWithValue("after", afterId);
                select.Parameters.AddWithValue("max", BatchSize);
                var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        candidates.Add(reader.GetInt64(0));
                    }
                }
            }

            // One statement per batch, so a batch whose callback can't be inserted doesn't hold up the others.
            var queues = new HashSet<string>(StringComparer.Ordinal);
            var sequences = new HashSet<string>(StringComparer.Ordinal);
            foreach (var id in candidates)
            {
                try
                {
                    var finish = runtime.DataSource.CreateCommand(runtime.Sql.BatchFinish);
                    await using (finish.ConfigureAwait(false))
                    {
                        finish.Parameters.AddWithValue("id", id);
                        var reader = await finish.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                        await using (reader.ConfigureAwait(false))
                        {
                            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                            {
                                OxbowDiagnostics.JobsInserted.Add(1);
                                if (reader.GetString(1) == "available")
                                {
                                    queues.Add(reader.GetString(0));
                                }

                                if (!reader.IsDBNull(2))
                                {
                                    sequences.Add(reader.GetString(2));
                                }
                            }
                        }
                    }
                }
                catch (PostgresException ex)
                {
                    Log.BatchFinishFailed(logger, ex, id);
                }
            }

            if (queues.Count > 0)
            {
                await NotifyQueuesAsync(queues, cancellationToken).ConfigureAwait(false);
            }

            await PromoteAsync(sequences, cancellationToken).ConfigureAwait(false);
            if (candidates.Count < BatchSize)
            {
                return;
            }

            afterId = candidates[^1];
        }
    }

    internal async Task CleanBatchesAsync(CancellationToken cancellationToken)
    {
        if (OxbowRuntime.IsInfinite(runtime.Options.BatchRetention))
        {
            return;
        }

        var horizon = timeProvider.GetUtcNow() - runtime.Options.BatchRetention;
        while (true)
        {
            var command = runtime.DataSource.CreateCommand(runtime.Sql.BatchDeleteExpired);
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.AddWithValue("horizon", horizon);
                command.Parameters.AddWithValue("max", BatchSize);
                if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) < BatchSize)
                {
                    return;
                }
            }
        }
    }

    private async Task CleanQueuesAsync(CancellationToken cancellationToken)
    {
        var command = runtime.DataSource.CreateCommand(runtime.Sql.QueueDeleteExpired);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("horizon", timeProvider.GetUtcNow() - QueueRetention);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task CleanRateLimitsAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            var command = runtime.DataSource.CreateCommand(runtime.Sql.RateLimitDeleteExpired);
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.AddWithValue("max", BatchSize);
                if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) < BatchSize)
                {
                    return;
                }
            }
        }
    }

    private async Task MeasureDepthAsync(CancellationToken cancellationToken)
    {
        var depth = new List<(string, string, long)>();
        var command = runtime.DataSource.CreateCommand(runtime.Sql.CountByQueueAndState);
        await using (command.ConfigureAwait(false))
        {
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    depth.Add((reader.GetString(0), reader.GetString(1), reader.GetInt64(2)));
                }
            }
        }

        OxbowDiagnostics.SetQueueDepth(depth);
    }

    private async Task EveryAsync(
        string name,
        TimeSpan interval,
        Func<CancellationToken, Task> work,
        CancellationToken cancellationToken,
        bool runImmediately = false)
    {
        var first = true;
        while (!cancellationToken.IsCancellationRequested)
        {
            if (!(first && runImmediately))
            {
                await Backoff.DelayAsync(Backoff.Jitter(interval), cancellationToken).ConfigureAwait(false);
            }

            first = false;
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            try
            {
                await work(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Log.MaintenanceFailed(logger, ex, name);
            }
        }
    }
}

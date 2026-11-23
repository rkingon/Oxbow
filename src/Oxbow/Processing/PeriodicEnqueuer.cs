using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using Oxbow.Internal;

namespace Oxbow.Processing;

/// <summary>
/// Enqueues periodic jobs from <c>{p}periodic_job</c>: stored schedules, and rows persisting the run times of the
/// jobs registered in code. Each run is inserted in the transaction that advances its row's next run, guarded on the row
/// as read, so two leaders across a failover can't both enqueue it.
/// </summary>
internal sealed class PeriodicEnqueuer(OxbowRuntime runtime, OxbowClient client, TimeProvider timeProvider, ILogger logger)
{
    private const int BatchSize = 100;
    private static readonly TimeSpan SyncInterval = TimeSpan.FromMinutes(1);

    private readonly Channel<bool> _wake = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    public void Wake() => _wake.Writer.TryWrite(true);

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var codeJobs = ResolveCodeJobs();
        var codeNames = codeJobs.Keys.ToArray();
        var reportedConflicts = new HashSet<string>(StringComparer.Ordinal);
        long? lastSync = null;
        var woken = false;
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            TimeSpan delay;
            try
            {
                // On wake too, so a deleted row of a job still in code is recreated at once.
                if (woken || lastSync is null || Stopwatch.GetElapsedTime(lastSync.Value) >= SyncInterval)
                {
                    await SyncCodeJobsAsync(codeJobs, reportedConflicts, cancellationToken).ConfigureAwait(false);
                    lastSync = Stopwatch.GetTimestamp();
                }

                delay = await EnqueueDueAsync(codeJobs, codeNames, cancellationToken).ConfigureAwait(false);
                failures = 0;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Log.PeriodicFailed(logger, ex);
                delay = Backoff.For(++failures);
            }

            woken = await WaitAsync(delay, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<bool> WaitAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        if (delay <= TimeSpan.Zero)
        {
            return false;
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(delay);
        try
        {
            await _wake.Reader.ReadAsync(timeout.Token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private Dictionary<string, PeriodicJob> ResolveCodeJobs()
    {
        var jobs = new Dictionary<string, PeriodicJob>(StringComparer.Ordinal);
        foreach (var job in runtime.Options.PeriodicJobs.Where(j => j.Name is not null))
        {
            jobs[job.Name!] = job;
        }

        foreach (var job in runtime.Options.PeriodicJobs.Where(j => j.Name is null))
        {
            string name;
            try
            {
                name = PeriodicJobNames.Derive(JobKinds.Get(job.CreateArgs().GetType()).Kind, job.Schedule);
            }
#pragma warning disable CA1031 // A job whose args can't be created mustn't stop the others; the log names the fix.
            catch (Exception ex)
#pragma warning restore CA1031
            {
                Log.PeriodicUnnamed(logger, ex, null);
                continue;
            }

            var unique = name;
            for (var n = 2; jobs.ContainsKey(unique); n++)
            {
                unique = $"{name} #{n}";
            }

            if (!PeriodicJobNames.IsValid(unique))
            {
                Log.PeriodicUnnamed(logger, null, unique);
                continue;
            }

            jobs[unique] = job;
        }

        return jobs;
    }

    private async Task SyncCodeJobsAsync(
        Dictionary<string, PeriodicJob> jobs,
        HashSet<string> reportedConflicts,
        CancellationToken cancellationToken)
    {
        var now = timeProvider.GetUtcNow();
        var entries = jobs.ToArray();
        var command = runtime.DataSource.CreateCommand(runtime.Sql.PeriodicJobSyncCode);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.Add(OxbowClient.TextArray("name", entries.Select(e => e.Key).ToArray()));
            command.Parameters.Add(NullableArray("cron", NpgsqlDbType.Text, entries.Select(e => (object?)e.Value.Schedule.Expression)));
            command.Parameters.Add(NullableArray("time_zone", NpgsqlDbType.Text, entries.Select(e => (object?)e.Value.Schedule.TimeZone?.Id)));
            command.Parameters.Add(NullableArray("every_seconds", NpgsqlDbType.Double, entries.Select(e => (object?)e.Value.Schedule.Interval?.TotalSeconds)));
            command.Parameters.Add(NullableArray("first_run_at", NpgsqlDbType.TimestampTz, entries.Select(e =>
                (object?)(e.Value.RunOnStart ? now : e.Value.Schedule.GetNextOccurrence(now))?.UtcDateTime)));
            command.Parameters.Add(NullableArray("next_occurrence", NpgsqlDbType.TimestampTz, entries.Select(e =>
                (object?)e.Value.Schedule.GetNextOccurrence(now)?.UtcDateTime)));
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var conflicts = runtime.DataSource.CreateCommand(runtime.Sql.PeriodicJobCodeConflicts);
        await using (conflicts.ConfigureAwait(false))
        {
            conflicts.Parameters.Add(OxbowClient.TextArray("name", entries.Select(e => e.Key).ToArray()));
            var reader = await conflicts.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (reportedConflicts.Add(reader.GetString(0)))
                    {
                        Log.PeriodicNameConflict(logger, reader.GetString(0));
                    }
                }
            }
        }
    }

    /// <summary>Enqueues every due run, then returns how long to sleep: until the next run, at most a scheduler interval.</summary>
    private async Task<TimeSpan> EnqueueDueAsync(
        Dictionary<string, PeriodicJob> codeJobs,
        string[] codeNames,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            var due = new List<DueRun>();
            var select = runtime.DataSource.CreateCommand(runtime.Sql.PeriodicJobGetDue);
            await using (select.ConfigureAwait(false))
            {
                select.Parameters.Add(OxbowClient.TextArray("code_names", codeNames));
                select.Parameters.AddWithValue("max", BatchSize);
                var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        due.Add(DueRun.Read(reader));
                    }
                }
            }

            foreach (var run in due)
            {
                await EnqueueAsync(run, codeJobs, cancellationToken).ConfigureAwait(false);
            }

            if (due.Count < BatchSize)
            {
                break;
            }
        }

        var interval = runtime.Options.SchedulerInterval;
        var next = runtime.DataSource.CreateCommand(runtime.Sql.PeriodicJobUntilNext);
        await using (next.ConfigureAwait(false))
        {
            next.Parameters.Add(OxbowClient.TextArray("code_names", codeNames));
            return await next.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is double seconds
                ? TimeSpan.FromSeconds(Math.Clamp(seconds, 0, interval.TotalSeconds))
                : interval;
        }
    }

    internal async Task EnqueueAsync(DueRun run, IReadOnlyDictionary<string, PeriodicJob> codeJobs, CancellationToken cancellationToken)
    {
        var code = run.RegisteredInCode ? codeJobs[run.Name] : null;
        PeriodicSchedule schedule;
        try
        {
            schedule = code?.Schedule ?? PeriodicSchedule.FromStored(run.Cron, run.TimeZone, run.EverySeconds);
        }
        catch (Exception ex) when (PeriodicSchedule.IsUnreadable(ex))
        {
            Log.PeriodicScheduleInvalid(logger, ex, run.Name);
            await AdvanceAsync(null, null, run, next: null, ran: false, kind: null, cancellationToken).ConfigureAwait(false);
            return;
        }

        // After downtime, one catch-up run, then the schedule resumes from now.
        var next = schedule.GetNextOccurrence(run.NextRunAt);
        if (next <= run.Now)
        {
            next = schedule.GetNextOccurrence(run.Now);
        }

        JobInsertParams job;
        try
        {
            job = code is not null
                ? client.PreparePeriodicRun(run.Name, code.CreateArgs(), code.Options)
                : client.PreparePeriodicRun(run.Name, run.Kind!, run.Args!, run.Options is null
                    ? null
                    : JsonSerializer.Deserialize(run.Options, PeriodicJsonContext.Default.InsertOptions));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Log.PeriodicRunFailed(logger, ex, run.Name);
            await AdvanceAsync(null, null, run, next, ran: false, kind: null, cancellationToken).ConfigureAwait(false);
            return;
        }

        var connection = await runtime.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                if (!await AdvanceAsync(connection, transaction, run, next, ran: true, code is null ? null : job.Kind, cancellationToken).ConfigureAwait(false))
                {
                    return;
                }

                try
                {
                    await client.InsertPreparedAsync(connection, transaction, [job], scope: null, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException and not NpgsqlException { IsTransient: true })
                {
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    Log.PeriodicRunFailed(logger, ex, run.Name);

                    // On this connection: asking the pool for another while holding it can deadlock a small pool.
                    await AdvanceAsync(connection, null, run, next, ran: false, kind: null, cancellationToken).ConfigureAwait(false);
                    return;
                }

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <returns>Whether the row was still as read, and so was advanced.</returns>
    private async Task<bool> AdvanceAsync(
        NpgsqlConnection? connection,
        NpgsqlTransaction? transaction,
        DueRun run,
        DateTimeOffset? next,
        bool ran,
        string? kind,
        CancellationToken cancellationToken)
    {
        var command = connection is null
            ? runtime.DataSource.CreateCommand(runtime.Sql.PeriodicJobAdvance)
            : new NpgsqlCommand(runtime.Sql.PeriodicJobAdvance, connection, transaction);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("name", run.Name);
            command.Parameters.Add(OxbowClient.Timestamp("next_run_at", next));
            command.Parameters.AddWithValue("ran", ran);
            command.Parameters.Add(new NpgsqlParameter("kind", NpgsqlDbType.Text) { Value = (object?)kind ?? DBNull.Value });
            command.Parameters.Add(OxbowClient.Timestamp("expected_next_run_at", run.NextRunAt));
            command.Parameters.Add(OxbowClient.Timestamp("expected_updated_at", run.UpdatedAt));
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
        }
    }

    private static NpgsqlParameter NullableArray(string name, NpgsqlDbType type, IEnumerable<object?> values) =>
        new(name, NpgsqlDbType.Array | type) { Value = values.Select(v => v ?? DBNull.Value).ToArray() };

    /// <summary>A due row as read, with the database's clock at the time.</summary>
    internal sealed record DueRun(
        string Name,
        bool RegisteredInCode,
        string? Kind,
        string? Args,
        string? Options,
        string? Cron,
        string? TimeZone,
        double? EverySeconds,
        DateTimeOffset NextRunAt,
        DateTimeOffset UpdatedAt,
        DateTimeOffset Now)
    {
        public static DueRun Read(NpgsqlDataReader reader) => new(
            reader.GetString(0),
            reader.GetBoolean(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetDouble(7),
            reader.GetFieldValue<DateTimeOffset>(9),
            reader.GetFieldValue<DateTimeOffset>(12),
            reader.GetFieldValue<DateTimeOffset>(13));
    }
}

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using System.Text.RegularExpressions;
using Npgsql;
using NpgsqlTypes;

namespace Oxbow.Internal;

internal sealed partial class OxbowClient(
    OxbowRuntime runtime,
    WorkerRegistry workers,
    IEnumerable<IJobInsertMiddleware> insertMiddleware,
    TimeProvider timeProvider) : IOxbowClient
{
    private readonly IJobInsertMiddleware[] _insertMiddleware = insertMiddleware.ToArray();
    private readonly ConcurrentDictionary<string, long> _lastNotified = new(StringComparer.Ordinal);

    public async Task<InsertResult> InsertAsync<TArgs>(TArgs args, InsertOptions? options = null, CancellationToken cancellationToken = default)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(args);
        var results = await InsertManyAsync([new InsertManyItem(args, options)], cancellationToken).ConfigureAwait(false);
        return results[0];
    }

    public async Task<InsertResult> InsertAsync<TArgs>(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        TArgs args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(args);
        var results = await InsertManyAsync(connection, transaction, [new InsertManyItem(args, options)], cancellationToken)
            .ConfigureAwait(false);
        return results[0];
    }

    public async Task<IReadOnlyList<InsertResult>> InsertManyAsync(IEnumerable<InsertManyItem> items, CancellationToken cancellationToken = default)
    {
        var jobs = Prepare(items, checkKinds: true);
        if (jobs.Count == 0)
        {
            return [];
        }

        var connection = await runtime.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                var results = await InsertPreparedAsync(connection, transaction, jobs, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return results;
            }
        }
    }

    public Task<IReadOnlyList<InsertResult>> InsertManyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IEnumerable<InsertManyItem> items,
        CancellationToken cancellationToken = default)
    {
        ValidateCallerConnection(connection, transaction);
        var jobs = Prepare(items, checkKinds: true);
        return jobs.Count == 0
            ? Task.FromResult<IReadOnlyList<InsertResult>>([])
            : InsertPreparedAsync(connection, transaction, jobs, cancellationToken);
    }

    public async Task<int> InsertManyFastAsync(IEnumerable<InsertManyItem> items, CancellationToken cancellationToken = default)
    {
        var jobs = PrepareFast(items);
        if (jobs.Count == 0)
        {
            return 0;
        }

        var connection = await runtime.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                var count = await CopyPreparedAsync(connection, transaction, jobs, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return count;
            }
        }
    }

    public Task<int> InsertManyFastAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IEnumerable<InsertManyItem> items,
        CancellationToken cancellationToken = default)
    {
        ValidateCallerConnection(connection, transaction);
        var jobs = PrepareFast(items);
        return jobs.Count == 0 ? Task.FromResult(0) : CopyPreparedAsync(connection, transaction, jobs, cancellationToken);
    }

    private List<JobInsertParams> PrepareFast(IEnumerable<InsertManyItem> items)
    {
        var jobs = Prepare(items, checkKinds: true);
        if (jobs.Any(j => j.UniqueKey is not null))
        {
            throw new ArgumentException(
                "InsertManyFastAsync can't insert unique jobs, because COPY can't skip duplicates. Use InsertManyAsync for them.",
                nameof(items));
        }

        return jobs;
    }

    private async Task<int> CopyPreparedAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        List<JobInsertParams> jobs,
        CancellationToken cancellationToken)
    {
        var count = 0;
        JobInsertDelegate copy = async context =>
        {
            count = await CopyAsync(connection, transaction, context.Jobs, context.CancellationToken).ConfigureAwait(false);
            return [];
        };
        await RunInsertPipelineAsync(jobs, copy, cancellationToken).ConfigureAwait(false);
        return count;
    }

    private async Task<int> CopyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IReadOnlyList<JobInsertParams> jobs,
        CancellationToken cancellationToken)
    {
        foreach (var job in jobs)
        {
            ValidateQueueName(job.Queue, allowWildcard: false);
        }

        // COPY can't fall back to the column default per row, so unscheduled jobs take the app's clock.
        var now = timeProvider.GetUtcNow().UtcDateTime;
        int count;
        var writer = await connection.BeginBinaryImportAsync(runtime.Sql.CopyJobs, cancellationToken).ConfigureAwait(false);
        await using (writer.ConfigureAwait(false))
        {
            foreach (var job in jobs)
            {
                await writer.StartRowAsync(cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(job.EncodedArgs!, NpgsqlDbType.Jsonb, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(job.Kind, NpgsqlDbType.Text, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync((short)job.MaxAttempts, NpgsqlDbType.Smallint, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(job.Metadata.ToJsonString(), NpgsqlDbType.Jsonb, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync((short)job.Priority, NpgsqlDbType.Smallint, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(job.Queue, NpgsqlDbType.Text, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(job.ScheduledAt?.UtcDateTime ?? now, NpgsqlDbType.TimestampTz, cancellationToken).ConfigureAwait(false);

                // Written by type name, so the consumer's data source needs no enum mapping.
                await writer.WriteAsync(job.Scheduled ? "scheduled" : "available", runtime.Sql.StateTypeName, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(job.Tags.ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Varchar, cancellationToken).ConfigureAwait(false);
            }

            count = (int)await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }

        await NotifyAsync(connection, transaction, jobs.Where(j => !j.Scheduled).Select(j => j.Queue), cancellationToken).ConfigureAwait(false);
        OxbowDiagnostics.JobsInserted.Add(count);
        return count;
    }

    /// <summary>Prepares a run of a periodic job registered in code, without the unknown-kind check.</summary>
    internal JobInsertParams PreparePeriodicRun(string name, IJobArgs args, InsertOptions? options)
    {
        var job = Prepare(args, options, timeProvider.GetUtcNow(), checkKinds: false);
        job.Metadata[PeriodicJobNames.MetadataKey] = name;
        return job;
    }

    /// <summary>
    /// Prepares a run of a stored periodic job, without the unknown-kind check. With a worker for the kind on this node,
    /// the args go through their type like any insert; otherwise they're inserted as stored.
    /// </summary>
    internal JobInsertParams PreparePeriodicRun(string name, string kind, string json, InsertOptions? options)
    {
        var now = timeProvider.GetUtcNow();
        JobInsertParams job;
        if (workers.Find(kind) is { } registration)
        {
            var args = JsonSerializer.Deserialize(json, runtime.Json.GetTypeInfo(registration.ArgsType)) as IJobArgs
                ?? throw new JsonException($"Args of periodic job '{name}' deserialized to null.");
            job = Prepare(args, options, now, checkKinds: false);
        }
        else
        {
            job = Prepare(new RawJobArgs(), kind, json, typeInfo: null, argsDefaults: null, options, now);
        }

        job.Metadata[PeriodicJobNames.MetadataKey] = name;
        return job;
    }

    public Task<PeriodicJobRow> CreatePeriodicJobAsync(
        string name,
        PeriodicSchedule schedule,
        IJobArgs args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var (kind, json) = SerializePeriodicArgs(args);
        return StorePeriodicJobAsync(replace: false, name, schedule, kind, json, options, cancellationToken);
    }

    public Task<PeriodicJobRow> CreatePeriodicJobAsync(
        string name,
        PeriodicSchedule schedule,
        string kind,
        JsonElement args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default) =>
        StorePeriodicJobAsync(replace: false, name, schedule, kind, RawPeriodicArgs(kind, args), options, cancellationToken);

    public Task<PeriodicJobRow> UpsertPeriodicJobAsync(
        string name,
        PeriodicSchedule schedule,
        IJobArgs args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var (kind, json) = SerializePeriodicArgs(args);
        return StorePeriodicJobAsync(replace: true, name, schedule, kind, json, options, cancellationToken);
    }

    public Task<PeriodicJobRow> UpsertPeriodicJobAsync(
        string name,
        PeriodicSchedule schedule,
        string kind,
        JsonElement args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default) =>
        StorePeriodicJobAsync(replace: true, name, schedule, kind, RawPeriodicArgs(kind, args), options, cancellationToken);

    private (string Kind, string Json) SerializePeriodicArgs(IJobArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);
        var argsType = args.GetType();
        return (JobKinds.Get(argsType).Kind, JsonSerializer.Serialize(args, runtime.Json.GetTypeInfo(argsType)));
    }

    private static string RawPeriodicArgs(string kind, JsonElement args)
    {
        if (string.IsNullOrWhiteSpace(kind) || kind.Length >= 128)
        {
            throw new ArgumentException($"Job kind '{kind}' must be 1 to 127 characters and not blank.", nameof(kind));
        }

        if (args.ValueKind == JsonValueKind.Undefined)
        {
            throw new ArgumentException("Args must be a JSON value.", nameof(args));
        }

        return args.GetRawText();
    }

    private async Task<PeriodicJobRow> StorePeriodicJobAsync(
        bool replace,
        string name,
        PeriodicSchedule schedule,
        string kind,
        string json,
        InsertOptions? options,
        CancellationToken cancellationToken)
    {
        ValidatePeriodicJobName(name);
        ArgumentNullException.ThrowIfNull(schedule);
        if (options?.ScheduledAt is not null)
        {
            throw new ArgumentException("A periodic job's options can't set ScheduledAt; each run is scheduled by the schedule.", nameof(options));
        }

        // Stored by ID, so the leader must be able to find it again.
        if (schedule.TimeZone is { } timeZone && !TimeZoneInfo.TryFindSystemTimeZoneById(timeZone.Id, out _))
        {
            throw new ArgumentException($"Time zone '{timeZone.Id}' isn't a system time zone, so it can't be stored.", nameof(schedule));
        }

        CheckKind(kind);
        try
        {
            // Fails now on anything that would fail every run: options, and args a local worker can't read.
            PreparePeriodicRun(name, kind, json, options);
        }
        catch (JsonException ex)
        {
            throw new ArgumentException($"The args don't deserialize to the args type of kind '{kind}': {ex.Message}", ex);
        }

        var command = runtime.DataSource.CreateCommand(replace ? runtime.Sql.PeriodicJobUpsert : runtime.Sql.PeriodicJobCreate);
        PeriodicJobRow? row;
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("name", name);
            command.Parameters.AddWithValue("kind", kind);
            command.Parameters.AddWithValue("args", json);
            command.Parameters.Add(Text("options", options is null
                ? null
                : JsonSerializer.Serialize(options, PeriodicJsonContext.Default.InsertOptions)));
            AddSchedule(command, schedule);
            command.Parameters.Add(Timestamp("next_run_at", schedule.GetNextOccurrence(timeProvider.GetUtcNow())));
            var rows = await PeriodicJobRowReader.ReadAllAsync(command, cancellationToken).ConfigureAwait(false);
            row = rows.Count == 0 ? null : rows[0];
        }

        if (row is null)
        {
            throw replace
                ? new InvalidOperationException(
                    $"Periodic job '{name}' is registered in code, so it can't be replaced at runtime. Choose another name.")
                : new PeriodicJobExistsException(name);
        }

        await NotifyPeriodicJobChangedAsync(cancellationToken).ConfigureAwait(false);
        return row;
    }

    public Task<PeriodicJobRow?> PausePeriodicJobAsync(string name, CancellationToken cancellationToken = default) =>
        ChangePeriodicJobAsync(runtime.Sql.PeriodicJobPause, name, cancellationToken);

    public async Task<PeriodicJobRow?> ResumePeriodicJobAsync(string name, CancellationToken cancellationToken = default)
    {
        ValidatePeriodicJobName(name);
        PeriodicJobRow? row;
        var connection = await runtime.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                var select = new NpgsqlCommand(runtime.Sql.PeriodicJobLock, connection, transaction);
                await using (select.ConfigureAwait(false))
                {
                    select.Parameters.AddWithValue("name", name);
                    var rows = await PeriodicJobRowReader.ReadAllAsync(select, cancellationToken).ConfigureAwait(false);
                    if (rows.Count == 0 || rows[0].PausedAt is null)
                    {
                        return rows.Count == 0 ? null : rows[0];
                    }

                    row = rows[0];
                }

                var update = new NpgsqlCommand(runtime.Sql.PeriodicJobResume, connection, transaction);
                await using (update.ConfigureAwait(false))
                {
                    update.Parameters.AddWithValue("name", name);
                    // A code-registered job's schedule lives in the leader's code (its time zone may not even be a
                    // system one), so the leader's sync fills in the next run.
                    update.Parameters.Add(Timestamp("next_run_at", row.RegisteredInCode
                        ? null
                        : ParseSchedule(row).GetNextOccurrence(timeProvider.GetUtcNow())));
                    row = (await PeriodicJobRowReader.ReadAllAsync(update, cancellationToken).ConfigureAwait(false))[0];
                }

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        await NotifyPeriodicJobChangedAsync(cancellationToken).ConfigureAwait(false);
        return row;
    }

    public Task<PeriodicJobRow?> DeletePeriodicJobAsync(string name, CancellationToken cancellationToken = default) =>
        ChangePeriodicJobAsync(runtime.Sql.PeriodicJobDelete, name, cancellationToken);

    public async Task<PeriodicJobRow?> GetPeriodicJobAsync(string name, CancellationToken cancellationToken = default)
    {
        ValidatePeriodicJobName(name);
        var command = runtime.DataSource.CreateCommand(runtime.Sql.PeriodicJobGet);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("name", name);
            var rows = await PeriodicJobRowReader.ReadAllAsync(command, cancellationToken).ConfigureAwait(false);
            return rows.Count == 0 ? null : rows[0];
        }
    }

    public async Task<PeriodicJobListResult> ListPeriodicJobsAsync(PeriodicJobListQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentOutOfRangeException.ThrowIfLessThan(query.Limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(query.Limit, 10_000);
        var command = runtime.DataSource.CreateCommand(runtime.Sql.PeriodicJobList);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.Add(Text("after", query.Cursor));
            command.Parameters.AddWithValue("limit", query.Limit);
            var jobs = await PeriodicJobRowReader.ReadAllAsync(command, cancellationToken).ConfigureAwait(false);
            return new PeriodicJobListResult(jobs, jobs.Count == query.Limit ? jobs[^1].Name : null);
        }
    }

    private static PeriodicSchedule ParseSchedule(PeriodicJobRow row)
    {
        try
        {
            return PeriodicSchedule.FromStored(row.CronExpression, row.TimeZoneId, row.Interval?.TotalSeconds);
        }
        catch (Exception ex) when (PeriodicSchedule.IsUnreadable(ex))
        {
            throw new InvalidOperationException($"Periodic job '{row.Name}' has a schedule this node can't read: {ex.Message}", ex);
        }
    }

    private async Task<PeriodicJobRow?> ChangePeriodicJobAsync(string sql, string name, CancellationToken cancellationToken)
    {
        ValidatePeriodicJobName(name);
        PeriodicJobRow? row;
        var command = runtime.DataSource.CreateCommand(sql);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("name", name);
            var rows = await PeriodicJobRowReader.ReadAllAsync(command, cancellationToken).ConfigureAwait(false);
            row = rows.Count == 0 ? null : rows[0];
        }

        if (row is not null)
        {
            await NotifyPeriodicJobChangedAsync(cancellationToken).ConfigureAwait(false);
        }

        return row;
    }

    /// <summary>Wakes the leader's periodic enqueuer; without it, the change is seen within a scheduler interval.</summary>
    private async Task NotifyPeriodicJobChangedAsync(CancellationToken cancellationToken)
    {
        var command = runtime.DataSource.CreateCommand(runtime.Sql.Notify);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("channel", runtime.Sql.ControlChannel);
            command.Parameters.Add(TextArray("payloads", [JsonSerializer.Serialize(
                new ControlPayload(ControlPayload.PeriodicJobChanged, null, null, null),
                OxbowJsonContext.Default.ControlPayload)]));
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static void AddSchedule(NpgsqlCommand command, PeriodicSchedule schedule)
    {
        command.Parameters.Add(Text("cron", schedule.Expression));
        command.Parameters.Add(Text("time_zone", schedule.TimeZone?.Id));
        command.Parameters.Add(new NpgsqlParameter("every_seconds", NpgsqlDbType.Double)
        {
            Value = schedule.Interval is { } interval ? interval.TotalSeconds : DBNull.Value,
        });
    }

    internal static NpgsqlParameter Timestamp(string name, DateTimeOffset? value) =>
        new(name, NpgsqlDbType.TimestampTz) { Value = value is { } at ? at.UtcDateTime : DBNull.Value };

    private static NpgsqlParameter Text(string name, string? value) =>
        new(name, NpgsqlDbType.Text) { Value = (object?)value ?? DBNull.Value };

    private static void ValidatePeriodicJobName(string name)
    {
        if (!PeriodicJobNames.IsValid(name))
        {
            throw new ArgumentException($"Periodic job name '{name}' must be 1 to 255 characters and not blank.", nameof(name));
        }
    }

    public Task<JobRow?> CancelAsync(long id, CancellationToken cancellationToken = default) =>
        SingleRowAsync(runtime.Sql.Cancel, cancellationToken, ("id", id), ("channel", runtime.Sql.ControlChannel));

    public Task<JobRow?> RetryAsync(long id, CancellationToken cancellationToken = default) =>
        SingleRowAsync(runtime.Sql.Retry, cancellationToken, ("id", id));

    public Task<JobRow?> DeleteAsync(long id, CancellationToken cancellationToken = default) =>
        SingleRowAsync(runtime.Sql.Delete, cancellationToken, ("id", id));

    public Task<JobRow?> GetAsync(long id, CancellationToken cancellationToken = default) =>
        SingleRowAsync(runtime.Sql.Get, cancellationToken, ("id", id));

    public async Task<JobListResult> ListAsync(JobListQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentOutOfRangeException.ThrowIfLessThan(query.Limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(query.Limit, 10_000);

        long? after = null;
        if (query.Cursor is not null)
        {
            after = long.TryParse(query.Cursor, NumberStyles.None, CultureInfo.InvariantCulture, out var cursor)
                ? cursor
                : throw new ArgumentException("The cursor is not valid.", nameof(query));
        }

        var sql = string.Format(
            CultureInfo.InvariantCulture,
            runtime.Sql.ListTemplate,
            query.Descending ? "<" : ">",
            query.Descending ? "DESC" : "ASC");

        var command = runtime.DataSource.CreateCommand(sql);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.Add(TextArray("states", query.States?.Select(JobStates.ToDb).ToArray()));
            command.Parameters.Add(TextArray("queues", query.Queues?.ToArray()));
            command.Parameters.Add(TextArray("kinds", query.Kinds?.ToArray()));
            command.Parameters.Add(new NpgsqlParameter("after", NpgsqlDbType.Bigint) { Value = (object?)after ?? DBNull.Value });
            command.Parameters.AddWithValue("limit", query.Limit);
            var jobs = await JobRowReader.ReadAllAsync(command, cancellationToken).ConfigureAwait(false);
            var nextCursor = jobs.Count == query.Limit
                ? jobs[^1].Id.ToString(CultureInfo.InvariantCulture)
                : null;
            return new JobListResult(jobs, nextCursor);
        }
    }

    public Task PauseQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        QueueControlAsync(runtime.Sql.QueuePause, queue, cancellationToken);

    public Task ResumeQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        QueueControlAsync(runtime.Sql.QueueResume, queue, cancellationToken);

    private async Task QueueControlAsync(string sql, string queue, CancellationToken cancellationToken)
    {
        ValidateQueueName(queue, allowWildcard: true);
        var command = runtime.DataSource.CreateCommand(sql);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("name", queue);
            command.Parameters.AddWithValue("channel", runtime.Sql.ControlChannel);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<JobRow?> SingleRowAsync(string sql, CancellationToken cancellationToken, params (string Name, object Value)[] parameters)
    {
        var command = runtime.DataSource.CreateCommand(sql);
        await using (command.ConfigureAwait(false))
        {
            foreach (var (name, value) in parameters)
            {
                command.Parameters.AddWithValue(name, value);
            }

            return await JobRowReader.ReadSingleAsync(command, cancellationToken).ConfigureAwait(false);
        }
    }

    private List<JobInsertParams> Prepare(IEnumerable<InsertManyItem> items, bool checkKinds)
    {
        ArgumentNullException.ThrowIfNull(items);
        var now = timeProvider.GetUtcNow();
        var jobs = new List<JobInsertParams>();
        foreach (var item in items)
        {
            ArgumentNullException.ThrowIfNull(item);
            ArgumentNullException.ThrowIfNull(item.Args);
            jobs.Add(Prepare(item.Args, item.Options, now, checkKinds));
        }

        var duplicate = jobs
            .Where(j => j.UniqueKey is not null)
            .GroupBy(j => Convert.ToHexString(j.UniqueKey!))
            .FirstOrDefault(g => g.Count() > 1);
        if (duplicate is not null)
        {
            throw new ArgumentException(
                $"The batch contains {duplicate.Count()} jobs of kind '{duplicate.First().Kind}' with the same unique key. Insert them separately.",
                nameof(items));
        }

        return jobs;
    }

    private JobInsertParams Prepare(IJobArgs args, InsertOptions? options, DateTimeOffset now, bool checkKinds)
    {
        var argsType = args.GetType();
        var kind = JobKinds.Get(argsType).Kind;
        if (checkKinds)
        {
            CheckKind(kind);
        }

        var typeInfo = runtime.Json.GetTypeInfo(argsType);
        return Prepare(args, kind, JsonSerializer.Serialize(args, typeInfo), typeInfo, args.GetInsertOptions(), options, now);
    }

    private void CheckKind(string kind)
    {
        if (!workers.IsEmpty && !runtime.Options.SkipUnknownKindCheck && workers.Find(kind) is null)
        {
            throw new InvalidOperationException(
                $"Job kind '{kind}' has no worker registered on this client. Register one with AddWorker, or set " +
                "OxbowOptions.SkipUnknownKindCheck if another service works it.");
        }
    }

    // A null typeInfo means raw args, which are unique by the whole object.
    private JobInsertParams Prepare(
        IJobArgs args,
        string kind,
        string encodedArgs,
        JsonTypeInfo? typeInfo,
        InsertOptions? argsDefaults,
        InsertOptions? options,
        DateTimeOffset now)
    {
        var job = new JobInsertParams(args, kind)
        {
            Queue = options?.Queue ?? argsDefaults?.Queue ?? "default",
            Priority = options?.Priority ?? argsDefaults?.Priority ?? 1,
            MaxAttempts = options?.MaxAttempts ?? argsDefaults?.MaxAttempts ?? runtime.Options.MaxAttempts,
            ScheduledAt = (options?.ScheduledAt ?? argsDefaults?.ScheduledAt)?.ToUniversalTime(),
        };

        ValidateQueueName(job.Queue, allowWildcard: false);
        if (job.Priority is < 1 or > 4)
        {
            throw new ArgumentException("Priority must be between 1 and 4.", nameof(options));
        }

        if (job.MaxAttempts is < 1 or > short.MaxValue)
        {
            throw new ArgumentException($"MaxAttempts must be between 1 and {short.MaxValue}.", nameof(options));
        }

        foreach (var tag in options?.Tags ?? argsDefaults?.Tags ?? [])
        {
            if (tag.Length > 255 || !TagPattern().IsMatch(tag))
            {
                throw new ArgumentException($"Tag '{tag}' must match {TagPattern()} and be at most 255 characters.", nameof(options));
            }

            job.Tags.Add(tag);
        }

        if ((options?.Metadata ?? argsDefaults?.Metadata) is { } metadata)
        {
            foreach (var (key, value) in metadata)
            {
                job.Metadata[key] = value?.DeepClone();
            }
        }

        job.Scheduled = job.ScheduledAt > now;
        job.EncodedArgs = encodedArgs;

        var unique = options?.Unique ?? argsDefaults?.Unique;
        if (unique is not null && !UniqueKeys.IsEmpty(unique))
        {
            UniqueKeys.Validate(unique);
            job.UniqueKey = UniqueKeys.Build(unique, kind, job.EncodedArgs, typeInfo, job.Queue, job.ScheduledAt ?? now);
            job.UniqueStates = UniqueKeys.StatesBitmask(unique);
        }

        return job;
    }

    internal Task<IReadOnlyList<InsertResult>> InsertPreparedAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        List<JobInsertParams> jobs,
        CancellationToken cancellationToken) =>
        RunInsertPipelineAsync(
            jobs,
            context => ExecuteInsertAsync(connection, transaction, context.Jobs, context.CancellationToken),
            cancellationToken);

    private async Task<IReadOnlyList<InsertResult>> RunInsertPipelineAsync(
        List<JobInsertParams> jobs,
        JobInsertDelegate insert,
        CancellationToken cancellationToken)
    {
        using var activity = OxbowDiagnostics.ActivitySource.StartActivity("oxbow.insert", ActivityKind.Producer);
        activity?.SetTag("oxbow.jobs.count", jobs.Count);
        if (activity is not null)
        {
            foreach (var job in jobs)
            {
                job.Metadata["traceparent"] = activity.Id;
            }
        }

        var pipeline = insert;
        for (var i = _insertMiddleware.Length - 1; i >= 0; i--)
        {
            var middleware = _insertMiddleware[i];
            var next = pipeline;
            pipeline = context => middleware.InvokeAsync(context, next);
        }

        try
        {
            return await pipeline(new JobInsertContext(jobs, cancellationToken)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    private async Task<IReadOnlyList<InsertResult>> ExecuteInsertAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IReadOnlyList<JobInsertParams> jobs,
        CancellationToken cancellationToken)
    {
        foreach (var job in jobs)
        {
            ValidateQueueName(job.Queue, allowWildcard: false);
        }

        var results = new List<InsertResult>(jobs.Count);
        var command = new NpgsqlCommand(runtime.Sql.InsertMany, connection, transaction);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.Add(TextArray("args", jobs.Select(j => j.EncodedArgs!).ToArray()));
            command.Parameters.Add(TextArray("kind", jobs.Select(j => j.Kind).ToArray()));
            command.Parameters.Add(new NpgsqlParameter<short[]>("max_attempts", jobs.Select(j => (short)j.MaxAttempts).ToArray()));
            command.Parameters.Add(TextArray("metadata", jobs.Select(j => j.Metadata.ToJsonString()).ToArray()));
            command.Parameters.Add(new NpgsqlParameter<short[]>("priority", jobs.Select(j => (short)j.Priority).ToArray()));
            command.Parameters.Add(TextArray("queue", jobs.Select(j => j.Queue).ToArray()));
            command.Parameters.Add(new NpgsqlParameter("scheduled_at", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)
            {
                Value = jobs.Select(j => j.ScheduledAt is { } at ? (object)at.UtcDateTime : DBNull.Value).ToArray(),
            });
            command.Parameters.Add(TextArray("state", jobs.Select(j => j.Scheduled ? "scheduled" : "available").ToArray()));
            command.Parameters.Add(TextArray("tags", jobs.Select(j => string.Join(',', j.Tags)).ToArray()));
            command.Parameters.Add(new NpgsqlParameter("unique_key", NpgsqlDbType.Array | NpgsqlDbType.Bytea)
            {
                Value = jobs.Select(j => (object?)j.UniqueKey ?? DBNull.Value).ToArray(),
            });
            command.Parameters.Add(new NpgsqlParameter<int[]>("unique_states", jobs.Select(j => j.UniqueStates).ToArray()));

            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    results.Add(new InsertResult(JobRowReader.Read(reader), reader.GetBoolean(16)));
                }
            }
        }

        await NotifyAsync(
            connection,
            transaction,
            results.Where(r => !r.UniqueSkippedAsDuplicate && r.Job.State == JobState.Available).Select(r => r.Job.Queue),
            cancellationToken).ConfigureAwait(false);

        var inserted = results.Count(r => !r.UniqueSkippedAsDuplicate);
        if (inserted > 0)
        {
            OxbowDiagnostics.JobsInserted.Add(inserted);
        }

        return results;
    }

    private async Task NotifyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IEnumerable<string> availableQueues,
        CancellationToken cancellationToken)
    {
        var queues = availableQueues.Distinct(StringComparer.Ordinal).Where(ShouldNotify).ToArray();
        if (queues.Length == 0)
        {
            return;
        }

        var notify = new NpgsqlCommand(runtime.Sql.Notify, connection, transaction);
        await using (notify.ConfigureAwait(false))
        {
            notify.Parameters.AddWithValue("channel", runtime.Sql.InsertChannel);
            notify.Parameters.Add(TextArray("payloads", queues
                .Select(q => JsonSerializer.Serialize(new InsertPayload(q), OxbowJsonContext.Default.InsertPayload))
                .ToArray()));
            await notify.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static void ValidateCallerConnection(NpgsqlConnection connection, NpgsqlTransaction? transaction)
    {
        ArgumentNullException.ThrowIfNull(connection);
        if (connection.FullState != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException("The connection must be open.");
        }

        if (transaction is not null && !ReferenceEquals(transaction.Connection, connection))
        {
            throw new InvalidOperationException("The transaction belongs to a different connection.");
        }
    }

    /// <summary>
    /// River's notify limiter: one wake-up per queue per fetch cooldown is enough, since a woken producer fetches
    /// everything available. A wake-up lost to a rolled-back transaction is covered by polling.
    /// </summary>
    private bool ShouldNotify(string queue)
    {
        var now = timeProvider.GetTimestamp();
        var cooldown = (long)(runtime.Options.FetchCooldown.TotalSeconds * timeProvider.TimestampFrequency);
        while (true)
        {
            if (_lastNotified.TryGetValue(queue, out var last))
            {
                if (now - last < cooldown)
                {
                    return false;
                }

                if (_lastNotified.TryUpdate(queue, now, last))
                {
                    return true;
                }
            }
            else if (_lastNotified.TryAdd(queue, now))
            {
                return true;
            }
        }
    }

    internal static NpgsqlParameter TextArray(string name, string[]? values) =>
        new(name, NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = (object?)values ?? DBNull.Value };

    private static void ValidateQueueName(string queue, bool allowWildcard)
    {
        if (string.IsNullOrWhiteSpace(queue) || queue.Length >= 128 || (!allowWildcard && queue == "*"))
        {
            throw new ArgumentException($"Queue name '{queue}' must be 1 to 127 characters and not blank.", nameof(queue));
        }
    }

    [GeneratedRegex(@"^\w[\w\-]+\w$")]
    private static partial Regex TagPattern();
}

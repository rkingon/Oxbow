using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
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
        ArgumentNullException.ThrowIfNull(connection);
        if (connection.FullState != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException("The connection must be open.");
        }

        if (transaction is not null && !ReferenceEquals(transaction.Connection, connection))
        {
            throw new InvalidOperationException("The transaction belongs to a different connection.");
        }

        var jobs = Prepare(items, checkKinds: true);
        return jobs.Count == 0
            ? Task.FromResult<IReadOnlyList<InsertResult>>([])
            : InsertPreparedAsync(connection, transaction, jobs, cancellationToken);
    }

    /// <summary>Inserts on a connection of its own without the unknown-kind check; used by the periodic enqueuer.</summary>
    internal async Task InsertUncheckedAsync(IEnumerable<InsertManyItem> items, CancellationToken cancellationToken)
    {
        var jobs = Prepare(items, checkKinds: false);
        var connection = await runtime.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                await InsertPreparedAsync(connection, transaction, jobs, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
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
        if (checkKinds && !workers.IsEmpty && !runtime.Options.SkipUnknownKindCheck && workers.Find(kind) is null)
        {
            throw new InvalidOperationException(
                $"Job kind '{kind}' has no worker registered on this client. Register one with AddWorker, or set " +
                "OxbowOptions.SkipUnknownKindCheck if another service works it.");
        }

        var argsDefaults = args.GetInsertOptions();
        var typeInfo = runtime.Json.GetTypeInfo(argsType);
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
        job.EncodedArgs = JsonSerializer.Serialize(args, typeInfo);

        var unique = options?.Unique ?? argsDefaults?.Unique;
        if (unique is not null && !UniqueKeys.IsEmpty(unique))
        {
            UniqueKeys.Validate(unique);
            job.UniqueKey = UniqueKeys.Build(unique, kind, job.EncodedArgs, typeInfo, job.Queue, job.ScheduledAt ?? now);
            job.UniqueStates = UniqueKeys.StatesBitmask(unique);
        }

        return job;
    }

    private async Task<IReadOnlyList<InsertResult>> InsertPreparedAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        List<JobInsertParams> jobs,
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

        JobInsertDelegate pipeline = context => ExecuteInsertAsync(connection, transaction, context.Jobs, context.CancellationToken);
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

        var queuesToNotify = results
            .Where(r => !r.UniqueSkippedAsDuplicate && r.Job.State == JobState.Available)
            .Select(r => r.Job.Queue)
            .Distinct(StringComparer.Ordinal)
            .Where(ShouldNotify)
            .ToArray();
        if (queuesToNotify.Length > 0)
        {
            var notify = new NpgsqlCommand(runtime.Sql.Notify, connection, transaction);
            await using (notify.ConfigureAwait(false))
            {
                notify.Parameters.AddWithValue("channel", runtime.Sql.InsertChannel);
                notify.Parameters.Add(TextArray("payloads", queuesToNotify
                    .Select(q => JsonSerializer.Serialize(new InsertPayload(q), OxbowJsonContext.Default.InsertPayload))
                    .ToArray()));
                await notify.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        var inserted = results.Count(r => !r.UniqueSkippedAsDuplicate);
        if (inserted > 0)
        {
            OxbowDiagnostics.JobsInserted.Add(inserted);
        }

        return results;
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

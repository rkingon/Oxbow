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

    public async Task<BatchInsertResult> InsertBatchAsync(
        IEnumerable<InsertManyItem> items,
        BatchOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var batch = PrepareBatch(items, options);
        var connection = await runtime.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                var result = await InsertBatchPreparedAsync(connection, transaction, batch, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return result;
            }
        }
    }

    public Task<BatchInsertResult> InsertBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IEnumerable<InsertManyItem> items,
        BatchOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ValidateCallerConnection(connection, transaction);
        var batch = PrepareBatch(items, options);
        return InsertBatchPreparedAsync(connection, transaction, batch, cancellationToken);
    }

    public async Task<Batch?> GetBatchAsync(long id, CancellationToken cancellationToken = default)
    {
        var command = runtime.DataSource.CreateCommand(runtime.Sql.BatchGet);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("id", id);
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    return null;
                }

                return new Batch
                {
                    Id = reader.GetInt64(0),
                    CreatedAt = reader.GetFieldValue<DateTimeOffset>(1),
                    FinalizedAt = reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2),
                    Total = reader.GetInt32(3),
                    Completed = reader.GetInt32(4),
                    Discarded = reader.GetInt32(5),
                    Cancelled = reader.GetInt32(6),
                    OnCompleteJobId = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                    OnSuccessJobId = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                };
            }
        }
    }

    private PreparedBatch PrepareBatch(IEnumerable<InsertManyItem> items, BatchOptions? options)
    {
        var jobs = Prepare(items, checkKinds: true);
        var now = timeProvider.GetUtcNow();
        var onComplete = PrepareCallback(options?.OnComplete, now);
        var onSuccess = PrepareCallback(options?.OnSuccess, now);
        if (onComplete?.UniqueKey is { } completeKey && onSuccess?.UniqueKey is { } successKey && completeKey.AsSpan().SequenceEqual(successKey))
        {
            throw new ArgumentException("OnComplete and OnSuccess have the same unique key, so only one could ever be inserted.", nameof(options));
        }

        return new PreparedBatch(jobs, onComplete, onSuccess);
    }

    private JobInsertParams? PrepareCallback(BatchCallback? callback, DateTimeOffset now)
    {
        if (callback is null)
        {
            return null;
        }

        ArgumentNullException.ThrowIfNull(callback.Args);
        return Prepare(callback.Args, callback.Options, now, checkKinds: true);
    }

    private async Task<BatchInsertResult> InsertBatchPreparedAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        PreparedBatch batch,
        CancellationToken cancellationToken)
    {
        long batchId;
        var next = new NpgsqlCommand(runtime.Sql.BatchNextId, connection, transaction);
        await using (next.ConfigureAwait(false))
        {
            batchId = (long)(await next.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }

        foreach (var job in batch.Jobs)
        {
            job.Metadata["oxbow:batch_id"] = batchId;
        }

        // Each callback goes through insert middleware now, in the caller's context, with an empty result list as for
        // COPY; the leader inserts it later as prepared. Middleware that doesn't call next drops it.
        async Task<string?> PrepareSpecAsync(JobInsertParams? callback, string name)
        {
            if (callback is null)
            {
                return null;
            }

            callback.Metadata["oxbow:batch_id"] = batchId;
            callback.Metadata["oxbow:batch_callback"] = name;
            var forwarded = false;
            await RunInsertPipelineAsync([callback], _ =>
            {
                forwarded = true;
                return Task.FromResult<IReadOnlyList<InsertResult>>([]);
            }, cancellationToken).ConfigureAwait(false);
            return forwarded ? CallbackSpec(callback) : null;
        }

        var insert = new BatchInsert(
            batchId,
            await PrepareSpecAsync(batch.OnComplete, "complete").ConfigureAwait(false),
            await PrepareSpecAsync(batch.OnSuccess, "success").ConfigureAwait(false));
        var inserted = false;
        var results = await RunInsertPipelineAsync(
            batch.Jobs,
            context =>
            {
                inserted = true;
                foreach (var job in context.Jobs)
                {
                    job.Metadata["oxbow:batch_id"] = batchId;
                }

                return ExecuteInsertAsync(connection, transaction, context.Jobs, context.CancellationToken, insert);
            },
            cancellationToken).ConfigureAwait(false);
        if (!inserted)
        {
            throw new InvalidOperationException("Insert middleware didn't insert the batch's jobs, so the batch wasn't inserted either.");
        }

        return new BatchInsertResult(batchId, results);
    }

    private static string? CallbackSpec(JobInsertParams? job)
    {
        if (job is null)
        {
            return null;
        }

        // Checked again after middleware: a value the job table rejects would fail every finisher pass instead of now.
        ValidateQueueName(job.Queue, allowWildcard: false);
        if (job.Priority is < 1 or > 4 || job.MaxAttempts is < 1 or > short.MaxValue
            || job.Tags.Any(t => t.Length > 255 || !TagPattern().IsMatch(t)))
        {
            throw new InvalidOperationException(
                $"Insert middleware left batch callback '{job.Kind}' with an invalid priority, max attempts or tag.");
        }

        var tags = new JsonArray();
        foreach (var tag in job.Tags)
        {
            tags.Add(tag);
        }

        return new JsonObject
        {
            ["kind"] = job.Kind,
            ["args"] = JsonNode.Parse(job.EncodedArgs!),
            ["queue"] = job.Queue,
            ["priority"] = job.Priority,
            ["max_attempts"] = job.MaxAttempts,
            ["metadata"] = job.Metadata.DeepClone(),
            ["tags"] = tags,
            ["scheduled_at"] = job.ScheduledAt?.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture),
            ["unique_key"] = job.UniqueKey is null ? null : Convert.ToBase64String(job.UniqueKey),
            ["unique_states"] = job.UniqueStates,
        }.ToJsonString();
    }

    private sealed record PreparedBatch(List<JobInsertParams> Jobs, JobInsertParams? OnComplete, JobInsertParams? OnSuccess);

    private sealed record BatchInsert(long Id, string? OnComplete, string? OnSuccess);

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
            command.Parameters.Add(new NpgsqlParameter("batch_id", NpgsqlDbType.Bigint) { Value = (object?)query.BatchId ?? DBNull.Value });
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

        var protector = runtime.Options.EncryptAllArgs || ArgsEnvelope.IsMarked(argsType)
            ? runtime.ArgsProtector ?? throw new InvalidOperationException(
                $"Job kind '{kind}' encrypts its args, but no IJobArgsProtector is registered. Call AddArgsEncryption() from Oxbow.DataProtection.")
            : null;

        var unique = options?.Unique ?? argsDefaults?.Unique;
        if (unique is not null && !UniqueKeys.IsEmpty(unique))
        {
            UniqueKeys.Validate(unique);
            job.UniqueKey = UniqueKeys.Build(
                unique, kind, job.EncodedArgs, typeInfo, job.Queue, job.ScheduledAt ?? now, unique.ByArgs ? protector : null);
            job.UniqueStates = UniqueKeys.StatesBitmask(unique);
        }

        if (protector is not null)
        {
            // Envelope safety on older nodes relies on args being an object: a string-reading converter would accept it.
            if (!job.EncodedArgs.StartsWith('{'))
            {
                throw new ArgumentException($"Job kind '{kind}' encrypts its args, so they must serialize to a JSON object.", nameof(args));
            }

            job.EncodedArgs = ArgsEnvelope.Seal(protector.Protect(kind, job.EncodedArgs));
        }

        return job;
    }

    private Task<IReadOnlyList<InsertResult>> InsertPreparedAsync(
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
        CancellationToken cancellationToken,
        BatchInsert? batch = null)
    {
        foreach (var job in jobs)
        {
            ValidateQueueName(job.Queue, allowWildcard: false);
        }

        var results = new List<InsertResult>(jobs.Count);
        var command = new NpgsqlCommand(batch is null ? runtime.Sql.InsertMany : runtime.Sql.InsertBatch, connection, transaction);
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
            command.Parameters.Add(new NpgsqlParameter("batch_id", NpgsqlDbType.Bigint) { Value = (object?)batch?.Id ?? DBNull.Value });
            if (batch is not null)
            {
                command.Parameters.Add(new NpgsqlParameter("on_complete", NpgsqlDbType.Text) { Value = (object?)batch.OnComplete ?? DBNull.Value });
                command.Parameters.Add(new NpgsqlParameter("on_success", NpgsqlDbType.Text) { Value = (object?)batch.OnSuccess ?? DBNull.Value });
            }

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

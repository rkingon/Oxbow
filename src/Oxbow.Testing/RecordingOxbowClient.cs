using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Oxbow.Testing;

/// <summary>
/// An in-memory <see cref="IOxbowClient"/> that records inserts, so code that enqueues can be unit tested without a
/// database. Inserts run the given insert middleware and are stored as Oxbow would store them; reads and job management
/// act on what's stored. Batch callbacks are recorded in <see cref="Batches"/>, not enqueued. Nothing is worked, there
/// are no transactions, and unique keys, sequence ordering, and keys
/// derived from <see cref="UniqueKeyAttribute"/>, <see cref="PartitionKeyAttribute"/> and
/// <see cref="SequenceKeyAttribute"/> aren't applied.
/// </summary>
public sealed class RecordingOxbowClient : IOxbowClient
{
    private const int DefaultMaxAttempts = 25;

    private readonly List<RecordedJob> _jobs = [];
    private readonly Dictionary<long, RecordedBatch> _batches = [];
    private readonly HashSet<string> _pausedQueues = new(StringComparer.Ordinal);
    private readonly IJobInsertMiddleware[] _insertMiddleware;
    private readonly JsonSerializerOptions _json;
    private readonly TimeProvider _timeProvider;
    private long _lastJobId;
    private long _lastBatchId;

    /// <summary>Creates a client.</summary>
    /// <param name="insertMiddleware">Insert middleware to run, outermost first, e.g. the app's.</param>
    /// <param name="jsonSerializerOptions">Serializer options for args. Default: <see cref="JsonSerializerOptions.Web"/>.</param>
    /// <param name="timeProvider">The clock for created and scheduled times. Default: the system clock.</param>
    public RecordingOxbowClient(
        IEnumerable<IJobInsertMiddleware>? insertMiddleware = null,
        JsonSerializerOptions? jsonSerializerOptions = null,
        TimeProvider? timeProvider = null)
    {
        _insertMiddleware = insertMiddleware?.ToArray() ?? [];
        var json = new JsonSerializerOptions(jsonSerializerOptions ?? JsonSerializerOptions.Web);
        json.TypeInfoResolver ??= new DefaultJsonTypeInfoResolver();
        json.MakeReadOnly();
        _json = json;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Every job inserted and not deleted, in insert order, as currently stored.</summary>
    public IReadOnlyList<RecordedJob> Jobs
    {
        get
        {
            lock (_jobs)
            {
                return [.. _jobs];
            }
        }
    }

    /// <summary>
    /// Every batch inserted, in insert order, with the callbacks it was given. Callbacks are recorded as passed: they
    /// don't go through insert middleware here, and are never enqueued.
    /// </summary>
    public IReadOnlyList<RecordedBatch> Batches
    {
        get
        {
            lock (_jobs)
            {
                return [.. _batches.Values.OrderBy(b => b.Id)];
            }
        }
    }

    /// <summary>Queues paused with <see cref="PauseQueueAsync"/> and not resumed.</summary>
    public IReadOnlyCollection<string> PausedQueues
    {
        get
        {
            lock (_jobs)
            {
                return [.. _pausedQueues];
            }
        }
    }

    /// <summary>The stored jobs whose args are <typeparamref name="TArgs"/>, in insert order.</summary>
    /// <typeparam name="TArgs">The args type.</typeparam>
    /// <returns>The jobs, with their typed args.</returns>
    public IReadOnlyList<Job<TArgs>> Inserted<TArgs>()
        where TArgs : IJobArgs =>
        [.. Jobs.Where(j => j.Args is TArgs).Select(j => new Job<TArgs>(j.Row, (TArgs)j.Args))];

    /// <summary>Forgets every job, batch and paused queue.</summary>
    public void Clear()
    {
        lock (_jobs)
        {
            _jobs.Clear();
            _batches.Clear();
            _pausedQueues.Clear();
        }
    }

    /// <inheritdoc/>
    public async Task<InsertResult> InsertAsync<TArgs>(TArgs args, InsertOptions? options = null, CancellationToken cancellationToken = default)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(args);
        var results = await InsertManyAsync([new InsertManyItem(args, options)], cancellationToken).ConfigureAwait(false);
        return results[0];
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<InsertResult>> InsertManyAsync(IEnumerable<InsertManyItem> items, CancellationToken cancellationToken = default) =>
        InsertCoreAsync(Prepare(items), batchId: null, cancellationToken);

    /// <inheritdoc/>
    public async Task<int> InsertManyFastAsync(IEnumerable<InsertManyItem> items, CancellationToken cancellationToken = default)
    {
        var jobs = Prepare(items);
        if (jobs.Any(j => j.Options.Unique is not null))
        {
            throw new ArgumentException(
                "InsertManyFastAsync can't insert unique jobs, because COPY can't skip duplicates. Use InsertManyAsync for them.",
                nameof(items));
        }

        var count = 0;
        await InsertCoreAsync(jobs, batchId: null, cancellationToken, recorded =>
        {
            // As Oxbow's COPY path: middleware sees an empty result list, and the count is tracked separately.
            count = recorded.Count;
            return [];
        }).ConfigureAwait(false);
        return count;
    }

    /// <inheritdoc/>
    public async Task<BatchInsertResult> InsertBatchAsync(
        IEnumerable<InsertManyItem> items,
        BatchOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var jobs = Prepare(items);
        var batchId = Interlocked.Increment(ref _lastBatchId);
        var results = await InsertCoreAsync(jobs, batchId, cancellationToken).ConfigureAwait(false);
        lock (_jobs)
        {
            _batches[batchId] = new RecordedBatch(batchId, _timeProvider.GetUtcNow(), [.. results.Select(r => r.Job.Id)], options);
        }

        return new BatchInsertResult(batchId, results);
    }

    /// <inheritdoc/>
    public Task<Batch?> GetBatchAsync(long id, CancellationToken cancellationToken = default)
    {
        lock (_jobs)
        {
            if (!_batches.TryGetValue(id, out var batch))
            {
                return Task.FromResult<Batch?>(null);
            }

            var members = _jobs.Where(j => j.Row.BatchId == id).Select(j => j.Row.State).ToList();
            return Task.FromResult<Batch?>(new Batch
            {
                Id = id,
                CreatedAt = batch.CreatedAt,
                Total = batch.JobIds.Count,
                Completed = members.Count(s => s == JobState.Completed),
                Discarded = members.Count(s => s == JobState.Discarded),
                Cancelled = members.Count(s => s == JobState.Cancelled) + batch.JobIds.Count - members.Count,
            });
        }
    }

    /// <inheritdoc/>
    public Task<JobRow?> CancelAsync(long id, CancellationToken cancellationToken = default) =>
        Task.FromResult(Update(id, Cancel));

    /// <inheritdoc/>
    public Task<JobRow?> RetryAsync(long id, CancellationToken cancellationToken = default) =>
        Task.FromResult(Update(id, Retry));

    /// <inheritdoc/>
    public Task<JobRow?> DeleteAsync(long id, CancellationToken cancellationToken = default)
    {
        lock (_jobs)
        {
            var index = _jobs.FindIndex(j => j.Row.Id == id);
            if (index < 0)
            {
                return Task.FromResult<JobRow?>(null);
            }

            var row = _jobs[index].Row;
            _jobs.RemoveAt(index);
            return Task.FromResult<JobRow?>(row);
        }
    }

    /// <inheritdoc/>
    public Task<CancelManyResult> CancelManyAsync(JobFilter filter, CancellationToken cancellationToken = default) =>
        Task.FromResult(new CancelManyResult(UpdateMany(filter, Cancel), 0));

    /// <inheritdoc/>
    public Task<RetryManyResult> RetryManyAsync(JobFilter filter, CancellationToken cancellationToken = default) =>
        Task.FromResult(new RetryManyResult(UpdateMany(filter, Retry), 0));

    /// <inheritdoc/>
    public Task<long> DeleteManyAsync(JobFilter filter, CancellationToken cancellationToken = default)
    {
        ValidateBulkFilter(filter);
        lock (_jobs)
        {
            return Task.FromResult((long)_jobs.RemoveAll(j => Matches(j.Row, filter)));
        }
    }

    /// <inheritdoc/>
    public Task<JobRow?> GetAsync(long id, CancellationToken cancellationToken = default)
    {
        lock (_jobs)
        {
            return Task.FromResult(_jobs.Find(j => j.Row.Id == id)?.Row);
        }
    }

    /// <inheritdoc/>
    public Task<JobListResult> ListAsync(JobListQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentOutOfRangeException.ThrowIfLessThan(query.Limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(query.Limit, 10_000);
        long? after = query.Cursor is null ? null : long.Parse(query.Cursor, CultureInfo.InvariantCulture);
        lock (_jobs)
        {
            var matches = _jobs.Select(j => j.Row).Where(r => Matches(r, query));
            matches = query.Descending
                ? matches.Where(r => after is null || r.Id < after).OrderByDescending(r => r.Id)
                : matches.Where(r => after is null || r.Id > after).OrderBy(r => r.Id);
            var page = matches.Take(query.Limit + 1).ToList();
            var next = page.Count > query.Limit ? page[query.Limit - 1].Id.ToString(CultureInfo.InvariantCulture) : null;
            return Task.FromResult(new JobListResult([.. page.Take(query.Limit)], next));
        }
    }

    /// <inheritdoc/>
    public Task PauseQueueAsync(string queue, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        lock (_jobs)
        {
            _pausedQueues.Add(queue);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task ResumeQueueAsync(string queue, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        lock (_jobs)
        {
            if (queue == "*")
            {
                _pausedQueues.Clear();
            }
            else
            {
                _pausedQueues.Remove(queue);
            }
        }

        return Task.CompletedTask;
    }

    private List<PreparedJob> Prepare(IEnumerable<InsertManyItem> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        var now = _timeProvider.GetUtcNow();
        var jobs = new List<PreparedJob>();
        foreach (var item in items)
        {
            ArgumentNullException.ThrowIfNull(item);
            ArgumentNullException.ThrowIfNull(item.Args);
            var kind = item.Args.GetType().GetCustomAttribute<JobKindAttribute>(inherit: false)?.Kind
                ?? throw new InvalidOperationException($"Job args type '{item.Args.GetType().FullName}' has no [JobKind] attribute.");
            var options = Merge(item.Options, item.Args.GetInsertOptions());
            var job = new JobInsertParams(item.Args, kind)
            {
                Queue = options.Queue ?? "default",
                Priority = options.Priority ?? 1,
                MaxAttempts = options.MaxAttempts ?? DefaultMaxAttempts,
                ScheduledAt = options.ScheduledAt?.ToUniversalTime(),
                PartitionKey = options.PartitionKey,
            };

            if (string.IsNullOrWhiteSpace(job.Queue) || job.Priority is < 1 or > 4 || job.MaxAttempts < 1)
            {
                throw new ArgumentException($"Job of kind '{kind}' has an invalid queue, priority or max attempts.", nameof(items));
            }

            foreach (var tag in options.Tags ?? [])
            {
                job.Tags.Add(tag);
            }

            foreach (var (key, value) in options.Metadata ?? [])
            {
                job.Metadata[key] = value?.DeepClone();
            }

            jobs.Add(new PreparedJob(job, options, now));
        }

        return jobs;
    }

    private static InsertOptions Merge(InsertOptions? options, InsertOptions? defaults) => new()
    {
        Queue = options?.Queue ?? defaults?.Queue,
        Priority = options?.Priority ?? defaults?.Priority,
        MaxAttempts = options?.MaxAttempts ?? defaults?.MaxAttempts,
        ScheduledAt = options?.ScheduledAt ?? defaults?.ScheduledAt,
        Tags = options?.Tags ?? defaults?.Tags,
        Metadata = options?.Metadata ?? defaults?.Metadata,
        Unique = options?.Unique ?? defaults?.Unique,
        SequenceKey = options?.SequenceKey ?? defaults?.SequenceKey,
        SequenceContinueOn = options?.SequenceContinueOn ?? defaults?.SequenceContinueOn,
        PartitionKey = options?.PartitionKey ?? defaults?.PartitionKey,
    };

    private async Task<IReadOnlyList<InsertResult>> InsertCoreAsync(
        List<PreparedJob> jobs,
        long? batchId,
        CancellationToken cancellationToken,
        Func<List<InsertResult>, IReadOnlyList<InsertResult>>? toMiddleware = null)
    {
        if (jobs.Count == 0)
        {
            return [];
        }

        if (batchId is { } id)
        {
            foreach (var job in jobs)
            {
                job.Params.Metadata["oxbow:batch_id"] = id;
            }
        }

        var byParams = jobs.ToDictionary(j => j.Params);
        JobInsertDelegate pipeline = context =>
        {
            var recorded = Record(context.Jobs, byParams);
            return Task.FromResult(toMiddleware is null ? recorded : toMiddleware(recorded));
        };
        for (var i = _insertMiddleware.Length - 1; i >= 0; i--)
        {
            var middleware = _insertMiddleware[i];
            var next = pipeline;
            pipeline = context => middleware.InvokeAsync(context, next);
        }

        return await pipeline(new JobInsertContext([.. jobs.Select(j => j.Params)], cancellationToken)).ConfigureAwait(false);
    }

    private List<InsertResult> Record(IReadOnlyList<JobInsertParams> jobs, Dictionary<JobInsertParams, PreparedJob> byParams)
    {
        var results = new List<InsertResult>(jobs.Count);
        lock (_jobs)
        {
            foreach (var job in jobs)
            {
                var prepared = byParams.GetValueOrDefault(job);
                var now = prepared?.Now ?? _timeProvider.GetUtcNow();
                var sequenceKey = prepared?.Options.SequenceKey;
                var row = new JobRow
                {
                    Id = ++_lastJobId,
                    State = sequenceKey is not null ? JobState.Pending
                        : job.ScheduledAt > now ? JobState.Scheduled
                        : JobState.Available,
                    Attempt = 0,
                    MaxAttempts = job.MaxAttempts,
                    AttemptedBy = [],
                    CreatedAt = now,
                    ScheduledAt = job.ScheduledAt ?? now,
                    Priority = job.Priority,
                    Args = JsonSerializer.SerializeToElement(job.Args, _json.GetTypeInfo(job.Args.GetType())),
                    Errors = [],
                    Kind = job.Kind,
                    Metadata = JsonSerializer.SerializeToElement(job.Metadata, _json.GetTypeInfo(typeof(JsonObject))),
                    Queue = job.Queue,
                    Tags = [.. job.Tags],
                    SequenceKey = sequenceKey,
                    PartitionKey = job.PartitionKey,
                };
                _jobs.Add(new RecordedJob(row, job.Args, prepared?.Options));
                results.Add(new InsertResult(row, UniqueSkippedAsDuplicate: false));
            }
        }

        return results;
    }

    private JobRow? Update(long id, Func<JobRow, JobRow?> change)
    {
        lock (_jobs)
        {
            var index = _jobs.FindIndex(j => j.Row.Id == id);
            if (index < 0)
            {
                return null;
            }

            if (change(_jobs[index].Row) is { } updated)
            {
                _jobs[index] = _jobs[index] with { Row = updated };
            }

            return _jobs[index].Row;
        }
    }

    private long UpdateMany(JobFilter filter, Func<JobRow, JobRow?> change)
    {
        ValidateBulkFilter(filter);
        lock (_jobs)
        {
            long count = 0;
            for (var i = 0; i < _jobs.Count; i++)
            {
                if (Matches(_jobs[i].Row, filter) && change(_jobs[i].Row) is { } updated)
                {
                    _jobs[i] = _jobs[i] with { Row = updated };
                    count++;
                }
            }

            return count;
        }
    }

    private JobRow? Cancel(JobRow row) =>
        row.FinalizedAt is not null ? null : Copy(row, JobState.Cancelled, row.ScheduledAt, _timeProvider.GetUtcNow());

    private JobRow? Retry(JobRow row) =>
        row.State == JobState.Running ? null : Copy(row, JobState.Available, _timeProvider.GetUtcNow(), finalizedAt: null);

    private static JobRow Copy(JobRow row, JobState state, DateTimeOffset scheduledAt, DateTimeOffset? finalizedAt) => new()
    {
        Id = row.Id,
        State = state,
        Attempt = row.Attempt,
        MaxAttempts = row.MaxAttempts,
        AttemptedAt = row.AttemptedAt,
        AttemptedBy = row.AttemptedBy,
        CreatedAt = row.CreatedAt,
        FinalizedAt = finalizedAt,
        ScheduledAt = scheduledAt,
        Priority = row.Priority,
        Args = row.Args,
        Errors = row.Errors,
        Kind = row.Kind,
        Metadata = row.Metadata,
        Queue = row.Queue,
        Tags = row.Tags,
        SequenceKey = row.SequenceKey,
        PartitionKey = row.PartitionKey,
    };

    private static void ValidateBulkFilter(JobFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        if (filter is JobListQuery)
        {
            throw new ArgumentException("Bulk operations take a JobFilter, not a JobListQuery.", nameof(filter));
        }

        if (!filter.All && filter.States is null && filter.Queues is null && filter.Kinds is null && filter.BatchId is null
            && filter.SequenceKey is null && filter.PartitionKey is null && (filter.Metadata is null || filter.Metadata.Count == 0)
            && (filter.Tags is null || filter.Tags.Count == 0))
        {
            throw new ArgumentException("The filter matches every job. Set JobFilter.All to confirm.", nameof(filter));
        }
    }

    private static bool Matches(JobRow row, JobFilter filter) =>
        (filter.States is null || filter.States.Contains(row.State))
        && (filter.Queues is null || filter.Queues.Contains(row.Queue))
        && (filter.Kinds is null || filter.Kinds.Contains(row.Kind))
        && (filter.BatchId is null || row.BatchId == filter.BatchId)
        && (filter.SequenceKey is null || row.SequenceKey == filter.SequenceKey)
        && (filter.PartitionKey is null || row.PartitionKey == filter.PartitionKey)
        && (filter.Tags is null || filter.Tags.All(row.Tags.Contains))
        && (filter.Metadata is null || Contains(JsonNode.Parse(row.Metadata.GetRawText()), filter.Metadata));

    // Postgres jsonb containment (@>): objects contain a subset of keys, arrays contain every element of the other.
    private static bool Contains(JsonNode? value, JsonNode? contained) => (value, contained) switch
    {
        (JsonObject o, JsonObject c) => c.All(p => o.TryGetPropertyValue(p.Key, out var v) && Contains(v, p.Value)),
        (JsonArray a, JsonArray c) => c.All(e => a.Any(v => Contains(v, e))),
        _ => JsonNode.DeepEquals(value, contained),
    };

    private sealed record PreparedJob(JobInsertParams Params, InsertOptions Options, DateTimeOffset Now);
}

/// <summary>A job stored by <see cref="RecordingOxbowClient"/>.</summary>
/// <param name="Row">The job as stored, after insert middleware.</param>
/// <param name="Args">The args it was inserted with.</param>
/// <param name="Options">
/// The insert options in effect: those passed, falling back to the args' <see cref="IJobArgs.GetInsertOptions"/>.
/// <see langword="null"/> if insert middleware added the job.
/// </param>
public sealed record RecordedJob(JobRow Row, IJobArgs Args, InsertOptions? Options)
{
    /// <summary>The job kind.</summary>
    public string Kind => Row.Kind;
}

/// <summary>A batch stored by <see cref="RecordingOxbowClient"/>.</summary>
/// <param name="Id">The batch's ID.</param>
/// <param name="CreatedAt">When it was inserted.</param>
/// <param name="JobIds">Its member jobs.</param>
/// <param name="Options">The callbacks it was given.</param>
public sealed record RecordedBatch(long Id, DateTimeOffset CreatedAt, IReadOnlyList<long> JobIds, BatchOptions? Options);

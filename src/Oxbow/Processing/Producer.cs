using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using Oxbow.Internal;

namespace Oxbow.Processing;

/// <summary>Fetches and runs jobs for one queue, up to its MaxWorkers at once.</summary>
internal sealed class Producer(
    string queue,
    QueueOptions queueOptions,
    OxbowRuntime runtime,
    JobExecutor executor,
    TimeProvider timeProvider,
    ILogger logger)
{
    private static readonly TimeSpan QueueRefreshInterval = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan PendingCancelLifetime = TimeSpan.FromMinutes(1);

    // How many candidates a limited fetch scans per free slot; the extra covers keys that only have room for some.
    private const int LimitedScanFactor = 4;

    // Timers and delays top out near 49 days; a longer rate limit wait just fetches again, finds nothing, and re-arms.
    private static readonly TimeSpan MaxRateWait = TimeSpan.FromHours(1);

    private readonly Channel<bool> _wake = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    private readonly ConcurrentDictionary<long, RunningJob> _active = new();
    private readonly ConcurrentDictionary<long, DateTimeOffset> _pendingCancels = new();
    private volatile bool _paused;

    // A database read of the pause applies only if no notification arrived while it ran and no read issued after it
    // has applied, so neither a notification nor a reconnect's resync is undone by an older read.
    private readonly Lock _pauseLock = new();
    private int _pauseVersion;
    private long _pauseReads;
    private long _pauseReadApplied;
    private int _resyncRequested;

    private readonly FetchLimits? _limits = FetchLimits.For(queueOptions);

    // When the queue's rate limit next allows a start, as of the last fetch; no fetch before then can start anything.
    // Only the fetch loop touches it.
    private DateTimeOffset _throttledUntil = DateTimeOffset.MinValue;

    // Running jobs per partition key, held until each outcome is written, so this node never counts fewer jobs for
    // a key than the database does. Guarded by itself.
    private readonly Dictionary<string, int> _keyRunning = new(StringComparer.Ordinal);

    public string Queue => queue;

    public int ActiveCount => _active.Count;

    public void TriggerFetch() => _wake.Writer.TryWrite(true);

    public void SetPaused(bool paused)
    {
        lock (_pauseLock)
        {
            _pauseVersion++;
            _paused = paused;
        }

        if (!paused)
        {
            TriggerFetch();
        }
    }

    /// <summary>Re-reads the pause before the next fetch, for when notifications may have been missed.</summary>
    public void Resync()
    {
        Volatile.Write(ref _resyncRequested, 1);
        TriggerFetch();
    }

    public void CancelJob(long jobId)
    {
        if (_active.TryGetValue(jobId, out var running))
        {
            running.Cancel();
        }
        else
        {
            // The cancel can arrive while the job is being fetched; apply it once the job starts.
            _pendingCancels[jobId] = timeProvider.GetUtcNow();
        }
    }

    public async Task WhenIdleAsync()
    {
        while (!_active.IsEmpty)
        {
            await Task.WhenAll(_active.Values.Select(r => r.Task)).ConfigureAwait(false);
        }
    }

    // listening completes once pause notifications are being received. Reading the pause only after it means a pause
    // committed around startup is either read or notified. It's waited for at most a settings poll, which catches a
    // pause regardless.
    public async Task RunAsync(Task listening, CancellationToken fetchToken, CancellationToken workToken)
    {
        try
        {
            try
            {
                await listening.WaitAsync(SettingsPollInterval, fetchToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
            }

            await ExecuteWithRetryAsync(ct => RefreshPausedAsync(UpsertQueueAsync, ct), fetchToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (fetchToken.IsCancellationRequested)
        {
            return;
        }

        using var pollSource = CancellationTokenSource.CreateLinkedTokenSource(fetchToken);
        var polls = Task.WhenAll(
            EveryAsync(runtime.Options.FetchPollInterval, _ => { TriggerFetch(); return Task.CompletedTask; }, pollSource.Token),
            EveryAsync(SettingsPollInterval, ct => RefreshPausedAsync(ReadPausedAsync, ct), pollSource.Token),
            EveryAsync(CancelPollInterval, PollCancellationsAsync, pollSource.Token),
            EveryAsync(QueueRefreshInterval, ct => RefreshPausedAsync(UpsertQueueAsync, ct), pollSource.Token));

        using var rateTimer = _limits is { Rated: true }
            ? timeProvider.CreateTimer(_ => TriggerFetch(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan)
            : null;

        TriggerFetch();
        var lastFetch = DateTimeOffset.MinValue;
        try
        {
            while (true)
            {
                await _wake.Reader.ReadAsync(fetchToken).ConfigureAwait(false);
                if (_throttledUntil != DateTimeOffset.MinValue)
                {
                    // Fetch the moment the rate limit allows, not a cooldown later: rates above 1 / FetchCooldown
                    // depend on it, and wakes before then would find nothing to start.
                    var untilAllowed = _throttledUntil - timeProvider.GetUtcNow();
                    if (untilAllowed > TimeSpan.Zero)
                    {
                        await Task.Delay(untilAllowed, timeProvider, fetchToken).ConfigureAwait(false);
                    }
                }
                else
                {
                    var sinceLastFetch = timeProvider.GetUtcNow() - lastFetch;
                    if (sinceLastFetch < runtime.Options.FetchCooldown)
                    {
                        await Task.Delay(runtime.Options.FetchCooldown - sinceLastFetch, timeProvider, fetchToken).ConfigureAwait(false);
                    }
                }

                if (Interlocked.Exchange(ref _resyncRequested, 0) == 1)
                {
                    try
                    {
                        await RefreshPausedAsync(ReadPausedAsync, fetchToken).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (!fetchToken.IsCancellationRequested)
                    {
                        Volatile.Write(ref _resyncRequested, 1);
                        Log.PollFailed(logger, ex, queue);
                        await Backoff.DelayAsync(TimeSpan.FromSeconds(1), fetchToken).ConfigureAwait(false);
                        TriggerFetch();
                        continue;
                    }
                }

                var slots = queueOptions.MaxWorkers - _active.Count;
                if (_paused || slots <= 0)
                {
                    continue;
                }

                lastFetch = timeProvider.GetUtcNow();
                List<JobRow> jobs;
                try
                {
                    if (_limits is null)
                    {
                        jobs = await FetchAsync(slots, fetchToken).ConfigureAwait(false);
                    }
                    else
                    {
                        var fetch = await FetchLimitedAsync(_limits, slots, fetchToken).ConfigureAwait(false);
                        jobs = fetch.Jobs;
                        ScheduleRateWake(rateTimer, fetch);
                    }
                }
                catch (Exception ex) when (!fetchToken.IsCancellationRequested)
                {
                    Log.FetchFailed(logger, ex, queue);
                    await Backoff.DelayAsync(TimeSpan.FromSeconds(1), fetchToken).ConfigureAwait(false);
                    continue;
                }

                foreach (var row in jobs)
                {
                    Start(row, workToken);
                }

                // A short limited fetch may have skipped jobs another transaction held locked.
                if (_limits is not null && jobs.Count > 0 && jobs.Count < slots)
                {
                    TriggerFetch();
                }
            }
        }
        catch (OperationCanceledException) when (fetchToken.IsCancellationRequested)
        {
        }

        await pollSource.CancelAsync().ConfigureAwait(false);
        await polls.ConfigureAwait(false);
    }

    private void ScheduleRateWake(ITimer? timer, LimitedFetch fetch)
    {
        _throttledUntil = DateTimeOffset.MinValue;
        if (timer is null)
        {
            return;
        }

        // The waits are measured from the database's clock while the fetch ran, which predates this, so the wake is
        // never early.
        if (fetch.RateWait > TimeSpan.Zero)
        {
            var wait = Min(fetch.RateWait.Value, MaxRateWait);
            _throttledUntil = timeProvider.GetUtcNow() + wait;
            timer.Change(wait, Timeout.InfiniteTimeSpan);
        }
        else if (fetch.PartitionRateWait > TimeSpan.Zero)
        {
            timer.Change(Min(fetch.PartitionRateWait.Value, MaxRateWait), Timeout.InfiniteTimeSpan);
        }

        static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;
    }

    private TimeSpan SettingsPollInterval => runtime.Options.PollOnly ? TimeSpan.FromSeconds(2) : TimeSpan.FromSeconds(30);

    private TimeSpan CancelPollInterval => runtime.Options.PollOnly ? TimeSpan.FromSeconds(2) : TimeSpan.FromMinutes(1);

    private void Start(JobRow row, CancellationToken workToken)
    {
        var key = _limits?.Concurrency is null ? null : row.PartitionKey;
        if (key is not null)
        {
            lock (_keyRunning)
            {
                _keyRunning[key] = _keyRunning.GetValueOrDefault(key) + 1;
            }
        }

        var running = new RunningJob(row);
        _active[row.Id] = running;
        if (_pendingCancels.TryRemove(row.Id, out _))
        {
            running.Cancel();
        }

        running.Task = Task.Run(
            async () =>
            {
                try
                {
                    await executor.ExecuteAsync(running, workToken).ConfigureAwait(false);
                }
                finally
                {
                    _active.TryRemove(row.Id, out _);
                    running.Dispose();
                    TriggerFetch();
                    if (key is not null)
                    {
                        _ = ReleaseKeyAsync(key, running, workToken);
                    }
                }
            },
            CancellationToken.None);
    }

    private async Task ReleaseKeyAsync(string key, RunningJob running, CancellationToken cancellationToken)
    {
        if (await running.Written.ConfigureAwait(false) is null)
        {
            // The row may still be running (until the rescuer moves it), so it keeps its slot until it isn't.
            try
            {
                while (await IsRunningAttemptAsync(running.Row, cancellationToken).ConfigureAwait(false) is not false)
                {
                    await Task.Delay(runtime.Options.FetchPollInterval, timeProvider, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
        }

        lock (_keyRunning)
        {
            if (--_keyRunning[key] == 0)
            {
                _keyRunning.Remove(key);
            }
        }

        TriggerFetch();
    }

    /// <summary>Whether the row is still running this attempt; null if that couldn't be checked.</summary>
    private async Task<bool?> IsRunningAttemptAsync(JobRow row, CancellationToken cancellationToken)
    {
        try
        {
            var command = runtime.DataSource.CreateCommand(runtime.Sql.IsRunningAttempt);
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.AddWithValue("id", row.Id);
                command.Parameters.AddWithValue("attempted_at", row.AttemptedAt!.Value);
                return (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
            }
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            Log.PollFailed(logger, ex, queue);
            return null;
        }
    }

    private async Task<List<JobRow>> FetchAsync(int max, CancellationToken cancellationToken)
    {
        var command = runtime.DataSource.CreateCommand(runtime.Sql.GetAvailable);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("queue", queue);
            command.Parameters.AddWithValue("max", max);
            command.Parameters.AddWithValue("client_id", runtime.ClientId);
            return await JobRowReader.ReadAllAsync(command, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<LimitedFetch> FetchLimitedAsync(FetchLimits limits, int max, CancellationToken cancellationToken)
    {
        string[] keys;
        int[] running;
        lock (_keyRunning)
        {
            keys = [.. _keyRunning.Keys];
            running = [.. _keyRunning.Values];
        }

        // One round trip and one implicit transaction: the queue lock is held until the fetch commits.
        var batch = runtime.DataSource.CreateBatch();
        await using (batch.ConfigureAwait(false))
        {
            return await FetchLimitedAsync(batch, runtime.Sql, queue, limits, max, runtime.ClientId, keys, running, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Adds the JIT opt-out, the queue lock (with a global or rate limit), the limited fetch and, with a rate limit, the
    /// wait query to <paramref name="batch"/>, and runs it.
    /// </summary>
    internal static async Task<LimitedFetch> FetchLimitedAsync(
        NpgsqlBatch batch,
        OxbowSql sql,
        string queue,
        FetchLimits limits,
        int max,
        string clientId,
        string[] localKeys,
        int[] localRunning,
        CancellationToken cancellationToken)
    {
        batch.BatchCommands.Add(new NpgsqlBatchCommand(sql.FetchWithoutJit));
        if (limits.Serialized)
        {
            var queueLock = new NpgsqlBatchCommand(sql.QueueLock);
            queueLock.Parameters.AddWithValue("queue", queue);
            batch.BatchCommands.Add(queueLock);
        }

        var fetch = new NpgsqlBatchCommand(sql.GetAvailableLimited);
        var p = fetch.Parameters;
        p.AddWithValue("queue", queue);
        p.AddWithValue("max", max);
        p.AddWithValue("scan", max * LimitedScanFactor);
        p.AddWithValue("client_id", clientId);
        p.Add(NullableInt("global_limit", limits.Concurrency?.Global));
        p.Add(NullableInt("local_limit", limits.Concurrency?.Local));
        p.Add(OxbowClient.TextArray("local_keys", localKeys));
        p.Add(new NpgsqlParameter<int[]>("local_running", localRunning));
        AddRateParameters(p, limits);
        batch.BatchCommands.Add(fetch);

        if (limits.Rated)
        {
            var wait = new NpgsqlBatchCommand(sql.RateLimitWait);
            wait.Parameters.AddWithValue("queue", queue);
            AddRateParameters(wait.Parameters, limits);
            batch.BatchCommands.Add(wait);
        }

        var rows = new List<JobRow>();
        TimeSpan? rateWait = null, partitionRateWait = null;
        var reader = await batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                rows.Add(JobRowReader.Read(reader));
            }

            if (limits.Rated && await reader.NextResultAsync(cancellationToken).ConfigureAwait(false)
                && await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                rateWait = reader.IsDBNull(0) ? null : TimeSpan.FromSeconds(reader.GetDouble(0));
                partitionRateWait = reader.IsDBNull(1) ? null : TimeSpan.FromSeconds(reader.GetDouble(1));
            }
        }

        return new LimitedFetch(rows, rateWait, partitionRateWait);
    }

    private static void AddRateParameters(NpgsqlParameterCollection p, FetchLimits limits)
    {
        p.Add(NullableLong("rate_interval", limits.Rate?.EmissionIntervalMicroseconds));
        p.Add(NullableInt("rate_burst", limits.Rate?.Burst));
        p.Add(NullableLong("key_rate_interval", limits.PartitionRate?.EmissionIntervalMicroseconds));
        p.Add(NullableInt("key_rate_burst", limits.PartitionRate?.Burst));
    }

    private static NpgsqlParameter NullableInt(string name, int? value) =>
        new(name, NpgsqlDbType.Integer) { Value = (object?)value ?? DBNull.Value };

    private static NpgsqlParameter NullableLong(string name, long? value) =>
        new(name, NpgsqlDbType.Bigint) { Value = (object?)value ?? DBNull.Value };

    private async Task<bool> UpsertQueueAsync(CancellationToken cancellationToken)
    {
        var command = runtime.DataSource.CreateCommand(runtime.Sql.QueueUpsert);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("name", queue);
            return (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }
    }

    private async Task<bool> ReadPausedAsync(CancellationToken cancellationToken)
    {
        var command = runtime.DataSource.CreateCommand(runtime.Sql.QueueIsPaused);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("name", queue);
            return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
        }
    }

    private async Task RefreshPausedAsync(Func<CancellationToken, Task<bool>> read, CancellationToken cancellationToken)
    {
        int version;
        long readId;
        lock (_pauseLock)
        {
            version = _pauseVersion;
            readId = ++_pauseReads;
        }

        var paused = await read(cancellationToken).ConfigureAwait(false);
        bool resumed;
        lock (_pauseLock)
        {
            if (version != _pauseVersion || readId < _pauseReadApplied)
            {
                return;
            }

            _pauseReadApplied = readId;
            resumed = _paused && !paused;
            _paused = paused;
        }

        if (resumed)
        {
            TriggerFetch();
        }
    }

    private async Task PollCancellationsAsync(CancellationToken cancellationToken)
    {
        var expired = timeProvider.GetUtcNow() - PendingCancelLifetime;
        foreach (var (id, at) in _pendingCancels)
        {
            if (at < expired)
            {
                _pendingCancels.TryRemove(id, out _);
            }
        }

        var ids = _active.Keys.ToArray();
        if (ids.Length == 0)
        {
            return;
        }

        var command = runtime.DataSource.CreateCommand(runtime.Sql.GetCancelRequested);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.Add(new NpgsqlParameter<long[]>("ids", ids));
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    CancelJob(reader.GetInt64(0));
                }
            }
        }
    }

    private async Task EveryAsync(TimeSpan interval, Func<CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Backoff.DelayAsync(Backoff.Jitter(interval), cancellationToken).ConfigureAwait(false);
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            try
            {
                await work(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                Log.PollFailed(logger, ex, queue);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task ExecuteWithRetryAsync(Func<CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        for (var failures = 1; ; failures++)
        {
            try
            {
                await work(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                Log.PollFailed(logger, ex, queue);
                await Backoff.DelayAsync(Backoff.For(failures), cancellationToken).ConfigureAwait(false);
            }
        }
    }
}

/// <summary>The limits a queue's fetch enforces; null for a queue with none.</summary>
internal sealed record FetchLimits(ConcurrencyLimit? Concurrency, RateLimit? Rate = null, RateLimit? PartitionRate = null)
{
    /// <summary>Whether fetches must be serialized behind the queue lock: limits that span nodes need it.</summary>
    public bool Serialized => Concurrency?.Global is not null || Rated;

    public bool Rated => Rate is not null || PartitionRate is not null;

    public static FetchLimits? For(QueueOptions options)
    {
        var concurrency = options.ConcurrencyLimit is { Global: not null } or { Local: not null } ? options.ConcurrencyLimit : null;
        return concurrency is null && options.RateLimit is null && options.PartitionRateLimit is null
            ? null
            : new FetchLimits(concurrency, options.RateLimit, options.PartitionRateLimit);
    }
}

/// <summary>
/// A limited fetch's jobs, and how long until the queue's and the first throttled key's rate limits next allow a start.
/// </summary>
internal sealed record LimitedFetch(List<JobRow> Jobs, TimeSpan? RateWait, TimeSpan? PartitionRateWait);

using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Npgsql;
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

    private readonly Channel<bool> _wake = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    private readonly ConcurrentDictionary<long, RunningJob> _active = new();
    private readonly ConcurrentDictionary<long, DateTimeOffset> _pendingCancels = new();
    private volatile bool _paused;

    public string Queue => queue;

    public int ActiveCount => _active.Count;

    public void TriggerFetch() => _wake.Writer.TryWrite(true);

    public void SetPaused(bool paused)
    {
        _paused = paused;
        if (!paused)
        {
            TriggerFetch();
        }
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

    public async Task RunAsync(CancellationToken fetchToken, CancellationToken workToken)
    {
        try
        {
            _paused = await ExecuteWithRetryAsync(UpsertQueueAsync, fetchToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (fetchToken.IsCancellationRequested)
        {
            return;
        }

        using var pollSource = CancellationTokenSource.CreateLinkedTokenSource(fetchToken);
        var polls = Task.WhenAll(
            EveryAsync(runtime.Options.FetchPollInterval, _ => { TriggerFetch(); return Task.CompletedTask; }, pollSource.Token),
            EveryAsync(SettingsPollInterval, PollSettingsAsync, pollSource.Token),
            EveryAsync(CancelPollInterval, PollCancellationsAsync, pollSource.Token),
            EveryAsync(QueueRefreshInterval, async ct => _paused = await UpsertQueueAsync(ct).ConfigureAwait(false), pollSource.Token));

        TriggerFetch();
        var lastFetch = DateTimeOffset.MinValue;
        try
        {
            while (true)
            {
                await _wake.Reader.ReadAsync(fetchToken).ConfigureAwait(false);
                var sinceLastFetch = timeProvider.GetUtcNow() - lastFetch;
                if (sinceLastFetch < runtime.Options.FetchCooldown)
                {
                    await Task.Delay(runtime.Options.FetchCooldown - sinceLastFetch, timeProvider, fetchToken).ConfigureAwait(false);
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
                    jobs = await FetchAsync(slots, fetchToken).ConfigureAwait(false);
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
            }
        }
        catch (OperationCanceledException) when (fetchToken.IsCancellationRequested)
        {
        }

        await pollSource.CancelAsync().ConfigureAwait(false);
        await polls.ConfigureAwait(false);
    }

    private TimeSpan SettingsPollInterval => runtime.Options.PollOnly ? TimeSpan.FromSeconds(2) : TimeSpan.FromSeconds(30);

    private TimeSpan CancelPollInterval => runtime.Options.PollOnly ? TimeSpan.FromSeconds(2) : TimeSpan.FromMinutes(1);

    private void Start(JobRow row, CancellationToken workToken)
    {
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
                }
            },
            CancellationToken.None);
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

    private async Task<bool> UpsertQueueAsync(CancellationToken cancellationToken)
    {
        var command = runtime.DataSource.CreateCommand(runtime.Sql.QueueUpsert);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("name", queue);
            return (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }
    }

    private async Task PollSettingsAsync(CancellationToken cancellationToken)
    {
        var command = runtime.DataSource.CreateCommand(runtime.Sql.QueueIsPaused);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("name", queue);
            var paused = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
            if (paused != _paused)
            {
                SetPaused(paused);
            }
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

    private async Task<T> ExecuteWithRetryAsync<T>(Func<CancellationToken, Task<T>> work, CancellationToken cancellationToken)
    {
        for (var failures = 1; ; failures++)
        {
            try
            {
                return await work(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                Log.PollFailed(logger, ex, queue);
                await Backoff.DelayAsync(Backoff.For(failures), cancellationToken).ConfigureAwait(false);
            }
        }
    }
}

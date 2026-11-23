using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Oxbow.Internal;

namespace Oxbow.Processing;

/// <summary>
/// Checks the schema version, then runs the notifier, one producer per queue and leader election. On stop: no new
/// fetches, running jobs get until the host's shutdown deadline, then they're soft-stopped back to available.
/// </summary>
internal sealed class OxbowHostedService(
    OxbowRuntime runtime,
    WorkerRegistry workers,
    OxbowClient client,
    IServiceScopeFactory scopes,
    IRetryPolicy retryPolicy,
    TimeProvider timeProvider,
    ILoggerFactory loggerFactory) : IHostedService, IDisposable
{
    private static readonly TimeSpan SoftStopGrace = TimeSpan.FromSeconds(10);

    private readonly ILogger _logger = loggerFactory.CreateLogger("Oxbow");
    private readonly CancellationTokenSource _fetchSource = new();
    private readonly CancellationTokenSource _workSource = new();
    private readonly CancellationTokenSource _notifierSource = new();
    private readonly Dictionary<string, Producer> _producers = new(StringComparer.Ordinal);
    private readonly List<Task> _loops = [];
    private Task? _stopping;
    private bool _disposed;
    private Task _notifierLoop = Task.CompletedTask;
    private Task _completerLoop = Task.CompletedTask;
    private Completer? _completer;

    internal Elector? Elector { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await CheckSchemaVersionAsync(cancellationToken).ConfigureAwait(false);

        var options = runtime.Options;
        if (options.Queues.Count == 0 && options.PeriodicJobs.Count == 0)
        {
            return;
        }

        _completer = new Completer(runtime, _logger);
        _completerLoop = Task.Run(_completer.RunAsync, CancellationToken.None);
        var executor = new JobExecutor(runtime, workers, scopes, retryPolicy, _completer, timeProvider, _logger);
        foreach (var (queue, queueOptions) in options.Queues)
        {
            _producers[queue] = new Producer(queue, queueOptions, runtime, executor, timeProvider, _logger);
        }

        Elector = new Elector(runtime, _logger);
        var maintenance = new Maintenance(runtime, workers, client, scopes, retryPolicy, timeProvider, TriggerFetch, _logger);

        if (!options.PollOnly)
        {
            var notifier = new Notifier(runtime, _logger);
            notifier.Subscribe(runtime.Sql.InsertChannel, OnInsertNotification);
            notifier.Subscribe(runtime.Sql.ControlChannel, OnControlNotification);
            notifier.Subscribe(runtime.Sql.LeadershipChannel, Elector.OnLeadershipNotification);
            notifier.Connected += () =>
            {
                foreach (var producer in _producers.Values)
                {
                    producer.TriggerFetch();
                }
            };
            _notifierLoop = Task.Run(() => notifier.RunAsync(_notifierSource.Token), CancellationToken.None);
        }

        foreach (var producer in _producers.Values)
        {
            _loops.Add(Task.Run(() => producer.RunAsync(_fetchSource.Token, _workSource.Token), CancellationToken.None));
        }

        _loops.Add(Task.Run(() => Elector.RunAsync(maintenance.RunAsync, _fetchSource.Token), CancellationToken.None));
        Log.Started(_logger, runtime.ClientId, string.Join(", ", _producers.Keys));
    }

    // Graceful shutdown isn't reliable once disposal has begun. Callers can overlap (RunAsync's shutdown waiter and an
    // explicit host.StopAsync), so the first one claims the shutdown and later ones share it.
    public Task StopAsync(CancellationToken cancellationToken)
    {
        if (_disposed)
        {
            return Task.CompletedTask;
        }

        var claim = new Task<Task>(() => StopCoreAsync(cancellationToken));
        var stopping = claim.Unwrap();
        if (Interlocked.CompareExchange(ref _stopping, stopping, null) is { } existing)
        {
            return existing;
        }

        claim.RunSynchronously(TaskScheduler.Default);
        return stopping;
    }

    // Cancels rather than disposes: the sources own no timers or links, so disposing buys nothing and would race an
    // in-flight stop. Cancelling keeps a never-stopped service's loops from outliving the container.
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        if (_stopping is not null)
        {
            return;
        }

        // Worker cancellation callbacks run inline; one throwing mustn't skip the rest or fail container disposal.
        foreach (var source in (CancellationTokenSource[])[_fetchSource, _workSource, _notifierSource])
        {
            try
            {
                source.Cancel();
            }
            catch (AggregateException ex)
            {
                Log.CancelCallbackFailed(_logger, ex);
            }
        }

        _completer?.Complete();
    }

    private async Task StopCoreAsync(CancellationToken cancellationToken)
    {
        await _fetchSource.CancelAsync().ConfigureAwait(false);
        await Task.WhenAll(_loops).ConfigureAwait(false);

        var idle = Task.WhenAll(_producers.Values.Select(p => p.WhenIdleAsync()));
        try
        {
            await idle.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            Log.SoftStopping(_logger, _producers.Values.Sum(p => p.ActiveCount));
            await _workSource.CancelAsync().ConfigureAwait(false);
            try
            {
                await idle.WaitAsync(SoftStopGrace, CancellationToken.None).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                // Jobs that ignore their token stay running; the rescuer picks them up.
            }
        }

        _completer?.Complete();
        await _completerLoop.ConfigureAwait(false);
        await _notifierSource.CancelAsync().ConfigureAwait(false);
        await _notifierLoop.ConfigureAwait(false);
    }

    private async Task CheckSchemaVersionAsync(CancellationToken cancellationToken)
    {
        int version;
        try
        {
            var command = runtime.DataSource.CreateCommand(runtime.Sql.SchemaVersion);
            await using (command.ConfigureAwait(false))
            {
                version = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture);
            }
        }
        catch (Npgsql.PostgresException ex) when (ex.SqlState == Npgsql.PostgresErrorCodes.UndefinedTable)
        {
            version = 0;
        }

        if (version < OxbowSchema.LatestVersion)
        {
            throw new InvalidOperationException(
                $"Oxbow's schema in '{runtime.Options.Schema}' with prefix '{runtime.Options.TablePrefix}' is at version {version}, " +
                $"but this release needs version {OxbowSchema.LatestVersion}. Apply the SQL from " +
                $"OxbowSchema.GetMigrations({version}, {OxbowSchema.LatestVersion}) in your own migrations; Oxbow never runs DDL itself.");
        }
    }

    private void TriggerFetch(string queue)
    {
        if (_producers.TryGetValue(queue, out var producer))
        {
            producer.TriggerFetch();
        }
    }

    private void OnInsertNotification(string payload)
    {
        var insert = JsonSerializer.Deserialize(payload, OxbowJsonContext.Default.InsertPayload);
        if (insert is not null)
        {
            TriggerFetch(insert.Queue);
        }
    }

    private void OnControlNotification(string payload)
    {
        var control = JsonSerializer.Deserialize(payload, OxbowJsonContext.Default.ControlPayload);
        if (control?.Queue is not { } queue)
        {
            return;
        }

        switch (control.Action)
        {
            // CancelAsync sends job_id; CancelManyAsync sends job_ids, which alpha.3 nodes ignore and pick up by polling.
            case "cancel" when _producers.TryGetValue(queue, out var producer):
                if (control.JobId is { } jobId)
                {
                    producer.CancelJob(jobId);
                }

                foreach (var id in control.JobIds ?? [])
                {
                    producer.CancelJob(id);
                }

                break;
            case "pause" or "resume":
                foreach (var target in _producers.Values.Where(p => queue == "*" || p.Queue == queue))
                {
                    target.SetPaused(control.Action == "pause");
                }

                break;
        }
    }
}

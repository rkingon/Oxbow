using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Oxbow.Internal;

namespace Oxbow.Processing;

internal abstract class DiscardHandlerRegistration
{
    public abstract Type HandlerType { get; }

    public abstract IJobDiscardHandler Resolve(IServiceProvider services);
}

internal sealed class DiscardHandlerRegistration<THandler> : DiscardHandlerRegistration
    where THandler : class, IJobDiscardHandler
{
    public override Type HandlerType => typeof(THandler);

    public override IJobDiscardHandler Resolve(IServiceProvider services) => services.GetRequiredService<THandler>();
}

/// <summary>A discarded or cancelled job waiting for its handlers. <see cref="Row"/> is read by ID when absent.</summary>
internal sealed record Discard(long Id, JobState State, JobDiscardReason Reason, Exception? Exception = null, JobRow? Row = null);

/// <summary>
/// Runs <see cref="IJobDiscardHandler"/>s on a background loop, off every worker slot and maintenance loop. Calls are
/// queued in memory: best-effort, lost if the node dies, and dropped if shutdown's grace runs out.
/// </summary>
[SuppressMessage("Design", "CA1001", Justification = "The stop source owns no timer or links; handlers may still hold its token.")]
internal sealed class DiscardHooks(
    OxbowRuntime runtime,
    IReadOnlyList<DiscardHandlerRegistration> handlers,
    IServiceScopeFactory scopes,
    ILogger logger)
{
    private const int MaxBacklog = 10_000;
    private const int MaxBatch = 500;

    private readonly Channel<Discard> _queue = Channel.CreateBounded<Discard>(
        new BoundedChannelOptions(MaxBacklog) { SingleReader = true, FullMode = BoundedChannelFullMode.Wait });

    // Executor continuations still waiting for the completer; shutdown lets them queue before closing the channel.
    private readonly ConcurrentDictionary<Task, byte> _observing = new();
    private readonly CancellationTokenSource _stop = new();
    private Task _loop = Task.CompletedTask;

    public bool Enabled => handlers.Count > 0;

    /// <summary>The discard an executor outcome amounts to, given the state requested and the state written.</summary>
    public static JobDiscardReason? Classify(JobState requested, JobState? written) => (requested, written) switch
    {
        (JobState.Discarded, JobState.Discarded) => JobDiscardReason.AttemptsExhausted,
        (JobState.Cancelled, JobState.Cancelled) => JobDiscardReason.WorkerCancelled,

        // cancel_attempted_at turned a retry, snooze or soft stop into a cancellation.
        (JobState.Available or JobState.Retryable or JobState.Scheduled, JobState.Cancelled) => JobDiscardReason.CancelRequested,
        _ => null,
    };

    public void Start()
    {
        if (Enabled)
        {
            _loop = Task.Run(RunAsync, CancellationToken.None);
        }
    }

    /// <summary>Queues the discard, if any, once the completer has written <paramref name="requested"/>.</summary>
    public void Observe(Task<JobState?> written, long id, JobState requested, Exception? exception)
    {
        if (!Enabled || requested == JobState.Completed)
        {
            return;
        }

        var observing = ObserveAsync(written, id, requested, exception);
        _observing.TryAdd(observing, 0);
        _ = observing.ContinueWith(t => _observing.TryRemove(t, out _), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    public void Enqueue(Discard discard)
    {
        if (Enabled && !_queue.Writer.TryWrite(discard) && !_stop.IsCancellationRequested)
        {
            Log.DiscardHandlersDropped(logger, 1, discard.Id);
        }
    }

    /// <summary>Waits up to <paramref name="grace"/> for queued calls, then cancels the rest.</summary>
    public async Task StopAsync(TimeSpan grace)
    {
        if (!Enabled)
        {
            return;
        }

        using var deadline = new CancellationTokenSource(grace);
        try
        {
            await Task.WhenAll(_observing.Keys).WaitAsync(deadline.Token).ConfigureAwait(false);
            _queue.Writer.TryComplete();
            await _loop.WaitAsync(deadline.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            Cancel();
            try
            {
                await _loop.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                // A handler ignoring its token is abandoned.
            }
        }
    }

    public void Cancel()
    {
        _queue.Writer.TryComplete();
        _stop.Cancel();
    }

    private async Task ObserveAsync(Task<JobState?> written, long id, JobState requested, Exception? exception)
    {
        if (Classify(requested, await written.ConfigureAwait(false)) is { } reason)
        {
            Enqueue(new Discard(id, requested == JobState.Discarded ? JobState.Discarded : JobState.Cancelled, reason, exception));
        }
    }

    private async Task RunAsync()
    {
        var reader = _queue.Reader;
        var batch = new List<Discard>(MaxBatch);
        var token = _stop.Token;
        while (await reader.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
        {
            while (batch.Count < MaxBatch && reader.TryRead(out var discard))
            {
                batch.Add(discard);
            }

            if (token.IsCancellationRequested)
            {
                Log.DiscardHandlersDropped(logger, batch.Count + reader.Count, batch[0].Id);
                return;
            }

            var rows = await ReadRowsAsync(batch, token).ConfigureAwait(false);
            for (var i = 0; i < batch.Count; i++)
            {
                if (token.IsCancellationRequested)
                {
                    Log.DiscardHandlersDropped(logger, batch.Count - i + reader.Count, batch[i].Id);
                    return;
                }

                var discard = batch[i];
                if ((discard.Row ?? rows.GetValueOrDefault(discard.Id)) is { } row)
                {
                    await InvokeAsync(new JobDiscardContext(row, discard.State, discard.Reason, discard.Exception), token).ConfigureAwait(false);
                }
            }

            batch.Clear();
        }
    }

    private async Task<Dictionary<long, JobRow>> ReadRowsAsync(List<Discard> batch, CancellationToken cancellationToken)
    {
        var ids = batch.Where(d => d.Row is null).Select(d => d.Id).ToArray();
        if (ids.Length == 0)
        {
            return [];
        }

        try
        {
            var command = runtime.DataSource.CreateCommand(runtime.Sql.GetMany);
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.Add(new NpgsqlParameter<long[]>("ids", ids));
                var rows = await JobRowReader.ReadAllAsync(command, cancellationToken).ConfigureAwait(false);
                return rows.ToDictionary(r => r.Id);
            }
        }
#pragma warning disable CA1031 // Best-effort: a failed read drops these calls, never the loop.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            Log.DiscardReadFailed(logger, ex, ids.Length);
            return [];
        }
    }

    private async Task InvokeAsync(JobDiscardContext context, CancellationToken cancellationToken)
    {
        try
        {
            var scope = scopes.CreateAsyncScope();
            await using (scope.ConfigureAwait(false))
            {
                foreach (var registration in handlers)
                {
                    try
                    {
                        await registration.Resolve(scope.ServiceProvider).HandleAsync(context, cancellationToken).ConfigureAwait(false);
                    }
#pragma warning disable CA1031 // A handler must never break Oxbow's loops.
                    catch (Exception ex)
#pragma warning restore CA1031
                    {
                        Log.DiscardHandlerFailed(logger, ex, registration.HandlerType.Name, context.Job.Id);
                    }
                }
            }
        }
#pragma warning disable CA1031 // Scope disposal can throw from user services.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            Log.DiscardHandlerFailed(logger, ex, "scope", context.Job.Id);
        }
    }
}

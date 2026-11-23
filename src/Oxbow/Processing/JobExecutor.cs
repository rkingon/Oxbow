using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Oxbow.Internal;

namespace Oxbow.Processing;

/// <summary>A job this node is working.</summary>
internal sealed class RunningJob(JobRow row) : IDisposable
{
    private readonly CancellationTokenSource _remoteCancel = new();

    public JobRow Row { get; } = row;

    public CancellationToken RemoteCancelToken => _remoteCancel.Token;

    public Task Task { get; set; } = Task.CompletedTask;

    /// <summary>Completes once the attempt's outcome is written; the row stays running until then.</summary>
    public Task Written { get; set; } = Task.CompletedTask;

    public void Cancel()
    {
        try
        {
            _remoteCancel.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The job finished first.
        }
    }

    public void Dispose() => _remoteCancel.Dispose();
}

/// <summary>Which of a job's cancellation sources had fired when its attempt ended.</summary>
internal readonly record struct CancellationSources(bool Shutdown, bool Remote, bool TimedOut);

/// <summary>Works one job: scope, args, middleware, worker, then the outcome as a state change.</summary>
internal sealed class JobExecutor(
    OxbowRuntime runtime,
    WorkerRegistry workers,
    IServiceScopeFactory scopes,
    IRetryPolicy retryPolicy,
    Completer completer,
    DiscardHooks discardHooks,
    TimeProvider timeProvider,
    ILogger logger)
{
    private readonly DefaultRetryPolicy _fallbackRetryPolicy = new(timeProvider);

    public async Task ExecuteAsync(RunningJob running, CancellationToken workToken)
    {
        var row = running.Row;
        var start = timeProvider.GetUtcNow();
        var tags = new TagList { { "queue", row.Queue }, { "kind", row.Kind } };
        OxbowDiagnostics.JobQueueWait.Record(Math.Max(0, (start - row.ScheduledAt).TotalSeconds), tags);

        using var activity = StartActivity(row);
        WorkUnit? unit = null;
        Exception? error = null;
        string? output = null;
        var timeout = runtime.Options.JobTimeout;
        CancellationTokenSource? timeoutSource = null;
        try
        {
            var registration = workers.Find(row.Kind);
            if (registration is null)
            {
                Log.UnknownKind(logger, row.Id, row.Kind);
                throw new UnknownJobKindException(row.Kind);
            }

            var scope = scopes.CreateAsyncScope();
            await using (scope.ConfigureAwait(false))
            {
                unit = registration.CreateUnit(row, scope.ServiceProvider, runtime.Json, runtime.ArgsProtector);
                unit.Attach(new JobControl(runtime, row, timeProvider));
                timeout = unit.GetTimeout() ?? timeout;
                timeoutSource = timeout > TimeSpan.Zero && !OxbowRuntime.IsInfinite(timeout)
                    ? new CancellationTokenSource(timeout, timeProvider)
                    : new CancellationTokenSource();
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                    workToken, running.RemoteCancelToken, timeoutSource.Token);

                var context = new JobContext(row, unit.Args, scope.ServiceProvider, linked.Token);
                var workUnit = unit;
                JobWorkDelegate pipeline = ctx => workUnit.WorkAsync(ctx.CancellationToken);
                var middleware = scope.ServiceProvider.GetServices<IJobWorkMiddleware>().ToArray();
                for (var i = middleware.Length - 1; i >= 0; i--)
                {
                    var current = middleware[i];
                    var next = pipeline;
                    pipeline = ctx => current.InvokeAsync(ctx, next);
                }

                await pipeline(context).ConfigureAwait(false);
                if (unit.Output is { } value)
                {
                    output = JobOutput.ToMetadataUpdate(value, runtime.Json);
                }
            }
        }
#pragma warning disable CA1031 // A worker must never take the host down.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            error = ex;
        }

        var now = timeProvider.GetUtcNow();
        var cancellation = new CancellationSources(
            workToken.IsCancellationRequested,
            running.RemoteCancelToken.IsCancellationRequested,
            timeoutSource?.IsCancellationRequested == true);
        var outcome = await ReportAsync(running, unit, error, output, start, now, timeout, cancellation).ConfigureAwait(false);
        timeoutSource?.Dispose();

        tags.Add("outcome", outcome);
        OxbowDiagnostics.JobsFinished.Add(1, tags);
        OxbowDiagnostics.JobDuration.Record((now - start).TotalSeconds, tags);
        activity?.SetTag("oxbow.job.outcome", outcome);
        if (error is not null && outcome is "errored" or "discarded")
        {
            activity?.SetStatus(ActivityStatusCode.Error, error.Message);
        }
    }

    private async Task<string> ReportAsync(
        RunningJob running,
        WorkUnit? unit,
        Exception? error,
        string? output,
        DateTimeOffset start,
        DateTimeOffset now,
        TimeSpan timeout,
        CancellationSources cancellation)
    {
        var row = running.Row;
        if (error is null)
        {
            await EnqueueAsync(running, null, new SetStateParams(row.Id, JobState.Completed) { FinalizedAt = now, MetadataUpdates = output })
                .ConfigureAwait(false);
            return "completed";
        }

        if (error is JobSnoozeException snooze)
        {
            var snoozes = row.Metadata.TryGetProperty("snoozes", out var count) && count.ValueKind == JsonValueKind.Number
                ? count.GetInt64() + 1
                : 1;
            await EnqueueAsync(running, error, new SetStateParams(row.Id, StateFor(snooze.Duration, JobState.Scheduled))
            {
                Attempt = Math.Max(row.Attempt - 1, 0),
                ScheduledAt = now + snooze.Duration,
                MetadataUpdates = new JsonObject { ["snoozes"] = snoozes }.ToJsonString(),
            }).ConfigureAwait(false);
            return "snoozed";
        }

        if (cancellation.Shutdown && error is OperationCanceledException)
        {
            // Soft stop at shutdown: back to available without spending the attempt.
            await EnqueueAsync(running, error, new SetStateParams(row.Id, JobState.Available)
            {
                Attempt = Math.Max(row.Attempt - 1, 0),
                ScheduledAt = now,
            }).ConfigureAwait(false);
            return "interrupted";
        }

        var message = error switch
        {
            OperationCanceledException when cancellation.TimedOut => $"TimeoutException: Job timed out after {timeout}.",
            OperationCanceledException when cancellation.Remote => "JobCancelledException: Job was cancelled remotely.",
            _ => $"{error.GetType().Name}: {error.Message}",
        };
        Log.JobFailed(logger, row.Id, row.Kind, row.Attempt, message);
        var attemptError = JsonSerializer.Serialize(
            new AttemptError(start, row.Attempt, message, error.StackTrace),
            OxbowJsonContext.Default.AttemptError);

        if (error is JobCancelException)
        {
            await EnqueueAsync(running, error, new SetStateParams(row.Id, JobState.Cancelled) { Error = attemptError, FinalizedAt = now })
                .ConfigureAwait(false);
            return "cancelled";
        }

        if (row.Attempt >= row.MaxAttempts)
        {
            await EnqueueAsync(running, error, new SetStateParams(row.Id, JobState.Discarded) { Error = attemptError, FinalizedAt = now })
                .ConfigureAwait(false);
            return "discarded";
        }

        var nextRetry = unit?.GetNextRetry() ?? retryPolicy.NextRetry(row);
        if (nextRetry < now)
        {
            Log.RetryInPast(logger, nextRetry, row.Id);
            nextRetry = _fallbackRetryPolicy.NextRetry(row);
        }

        await EnqueueAsync(running, error, new SetStateParams(row.Id, StateFor(nextRetry - now, JobState.Retryable))
        {
            Error = attemptError,
            ScheduledAt = nextRetry,
        }).ConfigureAwait(false);

        // A cancel request turns the retry into a cancellation when the outcome is written.
        return cancellation.Remote ? "cancelled" : "errored";
    }

    /// <summary>
    /// Queues an attempt's outcome, and a failed one's discard handlers for when it's written. Never waits for the write.
    /// </summary>
    private async ValueTask EnqueueAsync(RunningJob running, Exception? error, SetStateParams update)
    {
        var written = await completer.EnqueueAsync(update).ConfigureAwait(false);
        running.Written = written;
        if (error is not null)
        {
            discardHooks.Observe(written, update.Id, update.State, error);
        }
    }

    /// <summary>Within one scheduler interval, a job goes straight to available instead of waiting for the scheduler.</summary>
    private JobState StateFor(TimeSpan delay, JobState otherwise) =>
        delay <= runtime.Options.SchedulerInterval ? JobState.Available : otherwise;

    private static Activity? StartActivity(JobRow row)
    {
        IEnumerable<ActivityLink>? links = null;
        if (row.Metadata.ValueKind == JsonValueKind.Object
            && row.Metadata.TryGetProperty("traceparent", out var traceParent)
            && traceParent.ValueKind == JsonValueKind.String
            && ActivityContext.TryParse(traceParent.GetString(), null, out var insertContext))
        {
            links = [new ActivityLink(insertContext)];
        }

        var activity = OxbowDiagnostics.ActivitySource.StartActivity("oxbow.work", ActivityKind.Consumer, default(ActivityContext), links: links);
        activity?.SetTag("oxbow.job.id", row.Id);
        activity?.SetTag("oxbow.job.kind", row.Kind);
        activity?.SetTag("oxbow.job.queue", row.Queue);
        activity?.SetTag("oxbow.job.attempt", row.Attempt);
        return activity;
    }
}

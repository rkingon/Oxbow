using Microsoft.Extensions.Logging;

namespace Oxbow.Processing;

internal static partial class Log
{
    [LoggerMessage(1, LogLevel.Information, "Oxbow started as {ClientId} with queues [{Queues}].")]
    public static partial void Started(ILogger logger, string clientId, string queues);

    [LoggerMessage(2, LogLevel.Error, "Oxbow notifier connection failed; reconnecting in {Delay}.")]
    public static partial void NotifierFailed(ILogger logger, Exception exception, TimeSpan delay);

    [LoggerMessage(3, LogLevel.Error, "Fetching jobs from queue '{Queue}' failed.")]
    public static partial void FetchFailed(ILogger logger, Exception exception, string queue);

    [LoggerMessage(4, LogLevel.Warning, "Job {JobId} ({Kind}) failed attempt {Attempt}: {Error}")]
    public static partial void JobFailed(ILogger logger, long jobId, string kind, int attempt, string error);

    [LoggerMessage(5, LogLevel.Error, "Recording the outcome of job {JobId} failed; the rescuer will pick it up.")]
    public static partial void CompleteFailed(ILogger logger, Exception exception, long jobId);

    [LoggerMessage(6, LogLevel.Information, "Gained leadership as {ClientId}.")]
    public static partial void GainedLeadership(ILogger logger, string clientId);

    [LoggerMessage(7, LogLevel.Information, "Lost leadership as {ClientId}.")]
    public static partial void LostLeadership(ILogger logger, string clientId);

    [LoggerMessage(8, LogLevel.Error, "Leader election failed.")]
    public static partial void ElectionFailed(ILogger logger, Exception exception);

    [LoggerMessage(9, LogLevel.Error, "Maintenance service '{Service}' failed.")]
    public static partial void MaintenanceFailed(ILogger logger, Exception exception, string service);

    [LoggerMessage(10, LogLevel.Error, "Job {JobId} has kind '{Kind}' with no registered worker; failing the attempt.")]
    public static partial void UnknownKind(ILogger logger, long jobId, string kind);

    [LoggerMessage(11, LogLevel.Information, "Rescued {Count} stuck jobs.")]
    public static partial void Rescued(ILogger logger, int count);

    [LoggerMessage(12, LogLevel.Error, "Enqueuing periodic jobs failed.")]
    public static partial void PeriodicFailed(ILogger logger, Exception exception);

    [LoggerMessage(13, LogLevel.Warning, "Shutdown deadline reached; cancelling {Count} running jobs.")]
    public static partial void SoftStopping(ILogger logger, int count);

    [LoggerMessage(14, LogLevel.Error, "Polling queue '{Queue}' failed.")]
    public static partial void PollFailed(ILogger logger, Exception exception, string queue);

    [LoggerMessage(15, LogLevel.Warning, "Retry policy returned {NextRetry}, which is in the past; using the default policy for job {JobId}.")]
    public static partial void RetryInPast(ILogger logger, DateTimeOffset nextRetry, long jobId);

    [LoggerMessage(16, LogLevel.Warning, "A cancellation callback threw while disposing Oxbow.")]
    public static partial void CancelCallbackFailed(ILogger logger, Exception exception);

    [LoggerMessage(40, LogLevel.Error, "Finishing batch {BatchId} failed; retrying on the next pass.")]
    public static partial void BatchFinishFailed(ILogger logger, Exception exception, long batchId);

    [LoggerMessage(50, LogLevel.Error, "Enqueuing a run of periodic job '{Name}' failed; the run is skipped.")]
    public static partial void PeriodicRunFailed(ILogger logger, Exception exception, string name);

    [LoggerMessage(51, LogLevel.Error, "Periodic job '{Name}' has a schedule this node can't read; it won't run until it's replaced.")]
    public static partial void PeriodicScheduleInvalid(ILogger logger, Exception exception, string name);

    [LoggerMessage(52, LogLevel.Error, "Periodic job '{Name}' is registered in code, but a periodic job created at runtime has that name; it won't run until one is renamed.")]
    public static partial void PeriodicNameConflict(ILogger logger, string name);

    [LoggerMessage(53, LogLevel.Error, "A periodic job registered without a name couldn't be named: its args failed to be created or its derived name '{Name}' is invalid. Give it a name.")]
    public static partial void PeriodicUnnamed(ILogger logger, Exception? exception, string? name);

    // 70–79: job output and discard handlers.
    [LoggerMessage(70, LogLevel.Error, "Discard handler {Handler} failed for job {JobId}.")]
    public static partial void DiscardHandlerFailed(ILogger logger, Exception exception, string handler, long jobId);

    [LoggerMessage(71, LogLevel.Warning, "Dropped {Count} discard handler calls, starting with job {JobId}.")]
    public static partial void DiscardHandlersDropped(ILogger logger, int count, long jobId);

    [LoggerMessage(72, LogLevel.Error, "Reading {Count} discarded jobs for their handlers failed; those calls are dropped.")]
    public static partial void DiscardReadFailed(ILogger logger, Exception exception, int count);

    [LoggerMessage(73, LogLevel.Warning, "A discard handler's cancellation callback threw.")]
    public static partial void DiscardCancelCallbackFailed(ILogger logger, Exception exception);
}

namespace Oxbow;

/// <summary>Thrown by a worker to reschedule its job without using up an attempt.</summary>
public sealed class JobSnoozeException : Exception
{
    /// <summary>Snoozes the job for <paramref name="duration"/>.</summary>
    /// <param name="duration">How long to wait before the next attempt.</param>
    public JobSnoozeException(TimeSpan duration)
        : base($"Job snoozed for {duration}.")
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(duration, TimeSpan.Zero);
        Duration = duration;
    }

    /// <summary>How long to wait before the next attempt.</summary>
    public TimeSpan Duration { get; }
}

/// <summary>
/// Thrown by a worker to fail the attempt and retry after a delay it learned while working, such as a
/// <c>Retry-After</c>. The attempt counts toward <see cref="JobRow.MaxAttempts"/> and the error is recorded; the delay
/// replaces the worker's <c>GetNextRetry</c> and the retry policy for this attempt only.
/// </summary>
public sealed class JobRetryException : Exception
{
    /// <summary>Fails the attempt and retries after <paramref name="delay"/>.</summary>
    /// <param name="delay">How long to wait before the next attempt; zero retries now.</param>
    /// <param name="message">The error, recorded in the job's errors.</param>
    public JobRetryException(TimeSpan delay, string message)
        : base(message)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        Delay = delay;
    }

    /// <summary>Fails the attempt and retries after <paramref name="delay"/>, recording the error that caused it.</summary>
    /// <param name="delay">How long to wait before the next attempt; zero retries now.</param>
    /// <param name="message">The error, recorded in the job's errors.</param>
    /// <param name="innerException">The error that caused the retry. Its type and stack trace are recorded.</param>
    public JobRetryException(TimeSpan delay, string message, Exception innerException)
        : base(message, innerException)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        Delay = delay;
    }

    /// <summary>How long to wait before the next attempt.</summary>
    public TimeSpan Delay { get; }
}

/// <summary>
/// Thrown to a worker whose attempt is no longer the job's running one: the rescuer took the job, or it was finished
/// or deleted. Let it propagate: Oxbow ignores the attempt's heartbeats, progress and outcome from here on.
/// </summary>
public sealed class JobAttemptSupersededException : Exception
{
    /// <summary>Reports that job <paramref name="jobId"/>'s attempt was superseded.</summary>
    /// <param name="jobId">The job's ID.</param>
    public JobAttemptSupersededException(long jobId)
        : base($"Job {jobId} is no longer running this attempt: it was rescued, finished or deleted.")
    {
        JobId = jobId;
    }

    /// <summary>The job's ID.</summary>
    public long JobId { get; }
}

/// <summary>Thrown by a worker to cancel its job permanently. The error is recorded and the job isn't retried.</summary>
public sealed class JobCancelException : Exception
{
    /// <summary>Cancels the job.</summary>
    public JobCancelException()
        : base("Job cancelled by its worker.")
    {
    }

    /// <summary>Cancels the job with a reason.</summary>
    /// <param name="message">The reason, recorded in the job's errors.</param>
    public JobCancelException(string message)
        : base(message)
    {
    }

    /// <summary>Cancels the job with a reason and the error that caused it.</summary>
    /// <param name="message">The reason, recorded in the job's errors.</param>
    /// <param name="innerException">The error that caused the cancellation.</param>
    public JobCancelException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

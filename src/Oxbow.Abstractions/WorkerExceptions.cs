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

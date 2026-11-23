namespace Oxbow;

/// <summary>
/// Called after a job is given up on: discarded or cancelled. Resolved from its own DI scope on the node that wrote the
/// outcome, after the write. Best-effort: a call is lost if the node dies first, and in rare races may be made twice,
/// so keep handlers idempotent. Register with <c>AddDiscardHandler&lt;T&gt;()</c>.
/// </summary>
public interface IJobDiscardHandler
{
    /// <summary>Handles a discarded or cancelled job. Exceptions are logged and otherwise ignored.</summary>
    /// <param name="context">The job and why it was given up on.</param>
    /// <param name="cancellationToken">Cancelled when shutdown stops waiting for handlers.</param>
    /// <returns>A task that completes when the handler is done.</returns>
    Task HandleAsync(JobDiscardContext context, CancellationToken cancellationToken);
}

/// <summary>A job passed to <see cref="IJobDiscardHandler"/>.</summary>
public sealed class JobDiscardContext
{
    /// <summary>Creates a context, for example to unit test a handler.</summary>
    /// <param name="job">The job, read after the outcome was written.</param>
    /// <param name="state">The state written: <see cref="JobState.Discarded"/> or <see cref="JobState.Cancelled"/>.</param>
    /// <param name="reason">Why.</param>
    /// <param name="exception">The exception that ended the last attempt, if this node's worker threw it.</param>
    public JobDiscardContext(JobRow job, JobState state, JobDiscardReason reason, Exception? exception = null)
    {
        ArgumentNullException.ThrowIfNull(job);
        Job = job;
        State = state;
        Reason = reason;
        Exception = exception;
    }

    /// <summary>
    /// The job, read after the outcome was written. Its <see cref="JobRow.State"/> can already differ from
    /// <see cref="State"/> if something retried it in between.
    /// </summary>
    public JobRow Job { get; }

    /// <summary>The state written: <see cref="JobState.Discarded"/> or <see cref="JobState.Cancelled"/>.</summary>
    public JobState State { get; }

    /// <summary>Why the job was given up on.</summary>
    public JobDiscardReason Reason { get; }

    /// <summary>
    /// The exception that ended the last attempt, when the outcome came from this node's worker; otherwise
    /// <see langword="null"/>.
    /// </summary>
    public Exception? Exception { get; }
}

/// <summary>Why a job was discarded or cancelled.</summary>
public enum JobDiscardReason
{
    /// <summary>The last attempt failed (or got stuck and was rescued): discarded.</summary>
    AttemptsExhausted,

    /// <summary>The worker threw <see cref="JobCancelException"/>: cancelled.</summary>
    WorkerCancelled,

    /// <summary><c>CancelAsync</c> was called while the job was running: cancelled.</summary>
    CancelRequested,

    /// <summary>
    /// The scheduler found another job holding the unique key when this one became due: discarded, with
    /// <c>metadata.unique_key_conflict</c> set.
    /// </summary>
    UniqueKeyConflict,
}

using System.Data.Common;

namespace Oxbow;

/// <summary>
/// Where a <see cref="Job{TArgs}"/>'s heartbeats, progress, steps and attempt locks go. Oxbow's executor supplies one
/// per attempt; pass your own to the <see cref="Job{TArgs}"/> constructor to observe them in a unit test.
/// </summary>
public interface IJobRuntime
{
    /// <summary>The current time, stamped on reported progress.</summary>
    DateTimeOffset UtcNow { get; }

    /// <summary>Records that the attempt is alive. May be throttled.</summary>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the heartbeat is written or skipped.</returns>
    Task HeartbeatAsync(CancellationToken cancellationToken);

    /// <summary>Records progress, already validated by the job.</summary>
    /// <param name="percent">How far along the work is, from 0 to 100.</param>
    /// <param name="message">A description of the current step, if any.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the progress is written.</returns>
    Task ReportProgressAsync(double percent, string? message, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the result recorded under <paramref name="name"/> for this job, or runs <paramref name="run"/> and records
    /// its result. The name is already validated and unique within the execution. The default throws
    /// <see cref="NotSupportedException"/>, so a runtime that predates steps can't silently skip recording them.
    /// </summary>
    /// <typeparam name="T">The step's result type.</typeparam>
    /// <param name="name">The step's name.</param>
    /// <param name="run">The step.</param>
    /// <param name="cancellationToken">Passed to <paramref name="run"/>, and cancels the reads and writes.</param>
    /// <returns>The recorded result.</returns>
    Task<T> StepAsync<T>(string name, Func<CancellationToken, Task<T>> run, CancellationToken cancellationToken) =>
        throw new NotSupportedException($"{GetType().Name} doesn't record steps. Use an Oxbow release with steps.");

    /// <summary>
    /// Locks the job's row in <paramref name="transaction"/> for as long as it's open, provided this attempt is still
    /// the one running; otherwise fails the transaction and throws. The default throws
    /// <see cref="NotSupportedException"/>, so a runtime that predates attempt locks can't report one it never took.
    /// </summary>
    /// <param name="transaction">The caller's open transaction.</param>
    /// <param name="cancellationToken">Cancels the lock.</param>
    /// <returns>A task that completes when the row is locked.</returns>
    /// <exception cref="JobAttemptSupersededException">This attempt is no longer the one running.</exception>
    Task LockAttemptAsync(DbTransaction transaction, CancellationToken cancellationToken) =>
        throw new NotSupportedException($"{GetType().Name} doesn't lock attempts. Use an Oxbow release with attempt locks.");
}

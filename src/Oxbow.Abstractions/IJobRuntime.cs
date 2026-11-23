namespace Oxbow;

/// <summary>
/// Where a <see cref="Job{TArgs}"/>'s heartbeats and progress go. Oxbow's executor supplies one per attempt; pass your
/// own to the <see cref="Job{TArgs}"/> constructor to observe them in a unit test.
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
}

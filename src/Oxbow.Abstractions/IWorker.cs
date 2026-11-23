namespace Oxbow;

/// <summary>Works jobs of one args type. Resolved from DI in a new scope for every job.</summary>
/// <typeparam name="TArgs">The args type.</typeparam>
public interface IWorker<TArgs>
    where TArgs : IJobArgs
{
    /// <summary>
    /// Works the job. Returning completes it; throwing fails the attempt. Throw <see cref="JobSnoozeException"/> or
    /// <see cref="JobCancelException"/> to snooze or cancel it.
    /// </summary>
    /// <param name="job">The job.</param>
    /// <param name="cancellationToken">Signalled on timeout, on a cancel request, and at shutdown.</param>
    /// <returns>A task that completes when the work is done.</returns>
    Task WorkAsync(Job<TArgs> job, CancellationToken cancellationToken);

    /// <summary>The timeout for this job, overriding <c>OxbowOptions.JobTimeout</c>.</summary>
    /// <param name="job">The job.</param>
    /// <returns>The timeout, <see cref="Timeout.InfiniteTimeSpan"/> for none, or <see langword="null"/> for the default.</returns>
    TimeSpan? GetTimeout(Job<TArgs> job) => null;

    /// <summary>When to retry after a failed attempt, overriding the retry policy.</summary>
    /// <param name="job">The job.</param>
    /// <returns>The retry time, or <see langword="null"/> for the retry policy's.</returns>
    DateTimeOffset? GetNextRetry(Job<TArgs> job) => null;
}

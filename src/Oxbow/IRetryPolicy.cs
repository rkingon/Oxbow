namespace Oxbow;

/// <summary>
/// Decides when a failed job is retried. Replace the default by registering your own in DI. Not called when the worker
/// threw <see cref="JobRetryException"/>.
/// </summary>
public interface IRetryPolicy
{
    /// <summary>When to retry <paramref name="job"/>, whose current attempt just failed.</summary>
    /// <param name="job">The job; <see cref="JobRow.Errors"/> doesn't yet include the current failure.</param>
    /// <returns>The retry time.</returns>
    DateTimeOffset NextRetry(JobRow job);
}

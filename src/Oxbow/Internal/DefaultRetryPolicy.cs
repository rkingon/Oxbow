namespace Oxbow.Internal;

/// <summary>River's default: attempt^4 seconds with ±10% jitter, where attempt counts prior errors plus this one.</summary>
internal sealed class DefaultRetryPolicy(TimeProvider timeProvider) : IRetryPolicy
{
    private const double JitterFraction = 0.1;
    private static readonly double MaxSeconds = TimeSpan.FromDays(365 * 100).TotalSeconds;

    public DateTimeOffset NextRetry(JobRow job)
    {
        var errorCount = job.Errors.Count + 1;
        var seconds = Math.Min(Math.Pow(errorCount, 4), MaxSeconds);
#pragma warning disable CA5394 // Jitter, not security.
        seconds += seconds * ((Random.Shared.NextDouble() * 2 * JitterFraction) - JitterFraction);
#pragma warning restore CA5394
        return timeProvider.GetUtcNow().AddSeconds(seconds);
    }
}

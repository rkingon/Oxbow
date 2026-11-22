namespace Oxbow.Processing;

internal static class Backoff
{
    public static TimeSpan For(int failures) =>
        TimeSpan.FromSeconds(Math.Min(Math.Pow(2, Math.Min(failures, 10)) * 0.5, 30));

    /// <summary>Delays, returning early (without throwing) when cancelled.</summary>
    public static async Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    public static TimeSpan Jitter(TimeSpan interval, double fraction = 0.1)
    {
#pragma warning disable CA5394 // Jitter, not security.
        return interval + (interval * fraction * Random.Shared.NextDouble());
#pragma warning restore CA5394
    }
}

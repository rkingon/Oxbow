namespace Oxbow;

/// <summary>
/// Caps how many jobs start per <paramref name="Period"/>, across every node working the queue. Starts are spaced at
/// <c>Period / Limit</c>; <see cref="Burst"/> lets up to that many start at once after an idle spell.
/// </summary>
/// <param name="Limit">Starts allowed per <paramref name="Period"/>.</param>
/// <param name="Period">The period <paramref name="Limit"/> applies to.</param>
public sealed record RateLimit(int Limit, TimeSpan Period)
{
    /// <summary>
    /// How many starts may happen at once when the limiter is idle. Default 1: starts are evenly spaced, so no window of
    /// <see cref="Period"/> sees more than <see cref="Limit"/>. A window that begins with a burst can see up to
    /// <c>Limit + Burst - 1</c>.
    /// </summary>
    public int Burst { get; init; } = 1;

    /// <summary>The time between starts in microseconds, rounded up so the limit is never exceeded.</summary>
    internal long EmissionIntervalMicroseconds =>
        (Period.Ticks / (Limit * 10L)) + (Period.Ticks % (Limit * 10L) == 0 ? 0 : 1);
}

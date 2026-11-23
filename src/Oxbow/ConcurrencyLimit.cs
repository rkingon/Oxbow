namespace Oxbow;

/// <summary>
/// Caps how many jobs with the same <see cref="InsertOptions.PartitionKey"/> run at once in a queue. Jobs without a
/// key aren't limited. <see langword="null"/> leaves that dimension unlimited.
/// </summary>
public sealed record ConcurrencyLimit
{
    /// <summary>
    /// The most jobs per key running at once across every node working the queue. Fetches for the queue are serialized
    /// to enforce it. Configure the same value on every node.
    /// </summary>
    public int? Global { get; init; }

    /// <summary>The most jobs per key running at once on this node.</summary>
    public int? Local { get; init; }
}

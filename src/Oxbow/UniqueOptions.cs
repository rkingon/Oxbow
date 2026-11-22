namespace Oxbow;

/// <summary>
/// Makes an insert unique: when a job with the same key exists in one of <see cref="ByState"/>, the insert returns
/// that job instead. The key always includes the kind unless <see cref="ExcludeKind"/> is set.
/// </summary>
public sealed record UniqueOptions
{
    /// <summary>
    /// Include the args: the properties marked <see cref="UniqueKeyAttribute"/>, or the whole args if none are.
    /// </summary>
    public bool ByArgs { get; init; }

    /// <summary>Include the period containing the job's scheduled time. At least one second.</summary>
    public TimeSpan? ByPeriod { get; init; }

    /// <summary>Include the queue.</summary>
    public bool ByQueue { get; init; }

    /// <summary>
    /// The states in which an existing job blocks the insert. Must include available, pending, running and scheduled.
    /// Default: every state except cancelled and discarded.
    /// </summary>
    public IReadOnlyList<JobState>? ByState { get; init; }

    /// <summary>Leave the kind out of the key, so different kinds can collide.</summary>
    public bool ExcludeKind { get; init; }
}

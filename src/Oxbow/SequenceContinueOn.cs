namespace Oxbow;

/// <summary>
/// Which failures a sequence moves past. By default a sequence job that ran and ended cancelled or discarded halts its
/// sequence until it's retried or deleted.
/// </summary>
[Flags]
public enum SequenceContinueOn
{
    /// <summary>Halt on both.</summary>
    None = 0,

    /// <summary>Move past a cancelled job.</summary>
    Cancelled = 1,

    /// <summary>Move past a discarded job.</summary>
    Discarded = 2,
}

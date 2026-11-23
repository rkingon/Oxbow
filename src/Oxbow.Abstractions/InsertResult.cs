namespace Oxbow;

/// <summary>The outcome of inserting one job.</summary>
/// <param name="Job">The inserted job, or the existing job a unique insert matched.</param>
/// <param name="UniqueSkippedAsDuplicate">Whether the insert was skipped because a matching unique job exists.</param>
public sealed record InsertResult(JobRow Job, bool UniqueSkippedAsDuplicate);

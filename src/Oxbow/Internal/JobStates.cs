namespace Oxbow.Internal;

internal static class JobStates
{
    public static string ToDb(JobState state) => state switch
    {
        JobState.Available => "available",
        JobState.Cancelled => "cancelled",
        JobState.Completed => "completed",
        JobState.Discarded => "discarded",
        JobState.Pending => "pending",
        JobState.Retryable => "retryable",
        JobState.Running => "running",
        JobState.Scheduled => "scheduled",
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, null),
    };

    public static JobState Parse(string state) => state switch
    {
        "available" => JobState.Available,
        "cancelled" => JobState.Cancelled,
        "completed" => JobState.Completed,
        "discarded" => JobState.Discarded,
        "pending" => JobState.Pending,
        "retryable" => JobState.Retryable,
        "running" => JobState.Running,
        "scheduled" => JobState.Scheduled,
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, null),
    };

    /// <summary>The bit for a state in <c>unique_states</c>, matching <c>job_state_in_bitmask</c> in the schema.</summary>
    public static int Bit(JobState state) => state switch
    {
        JobState.Available => 1 << 0,
        JobState.Cancelled => 1 << 1,
        JobState.Completed => 1 << 2,
        JobState.Discarded => 1 << 3,
        JobState.Pending => 1 << 4,
        JobState.Retryable => 1 << 5,
        JobState.Running => 1 << 6,
        JobState.Scheduled => 1 << 7,
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, null),
    };
}

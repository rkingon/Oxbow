namespace Oxbow;

/// <summary>The lifecycle state of a job.</summary>
public enum JobState
{
    /// <summary>Ready to be worked.</summary>
    Available,

    /// <summary>Cancelled; terminal.</summary>
    Cancelled,

    /// <summary>Worked successfully; terminal.</summary>
    Completed,

    /// <summary>Failed its last attempt or was discarded; terminal.</summary>
    Discarded,

    /// <summary>Inserted but not yet eligible to be scheduled. Reserved for future use.</summary>
    Pending,

    /// <summary>Failed an attempt and waiting for its retry time.</summary>
    Retryable,

    /// <summary>Being worked.</summary>
    Running,

    /// <summary>Waiting for its scheduled time.</summary>
    Scheduled,
}

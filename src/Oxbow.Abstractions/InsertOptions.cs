using System.Text.Json.Nodes;

namespace Oxbow;

/// <summary>Options for inserting a job. Unset values fall back to the args' defaults, then the client's.</summary>
public sealed record InsertOptions
{
    /// <summary>The queue. Default <c>default</c>.</summary>
    public string? Queue { get; init; }

    /// <summary>Priority from 1 (highest) to 4. Default 1.</summary>
    public int? Priority { get; init; }

    /// <summary>Attempts before a failing job is discarded. Default <c>OxbowOptions.MaxAttempts</c>.</summary>
    public int? MaxAttempts { get; init; }

    /// <summary>Run no earlier than this. A future time inserts the job as <see cref="JobState.Scheduled"/>.</summary>
    public DateTimeOffset? ScheduledAt { get; init; }

    /// <summary>Tags: letters, digits, <c>_</c> and <c>-</c>, up to 255 characters each.</summary>
    public IReadOnlyList<string>? Tags { get; init; }

    /// <summary>Metadata stored with the job.</summary>
    public JsonObject? Metadata { get; init; }

    /// <summary>Uniqueness, enforced by a unique index in the database.</summary>
    public UniqueOptions? Unique { get; init; }

    /// <summary>
    /// Runs the job in a sequence: jobs with the same key run one at a time, in insert order, across every node and
    /// queue. Overrides <see cref="SequenceKeyAttribute"/>. 1 to 255 characters.
    /// </summary>
    public string? SequenceKey { get; init; }

    /// <summary>
    /// Whether the sequence moves past this job if it ends cancelled or discarded. Default
    /// <see cref="Oxbow.SequenceContinueOn.None"/>: it halts the sequence.
    /// </summary>
    public SequenceContinueOn? SequenceContinueOn { get; init; }

    /// <summary>
    /// The partition key for the queue's <c>QueueOptions.ConcurrencyLimit</c>, up to 255 characters. Default: the
    /// args' <see cref="PartitionKeyAttribute"/> properties, or none (unlimited).
    /// </summary>
    public string? PartitionKey { get; init; }
}

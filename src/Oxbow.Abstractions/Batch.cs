namespace Oxbow;

/// <summary>Callbacks for a batch, enqueued by the leader once every job in it has finished.</summary>
public sealed record BatchOptions
{
    /// <summary>Enqueued once every job has finished, whatever the outcome.</summary>
    public BatchCallback? OnComplete { get; init; }

    /// <summary>Enqueued once every job has completed successfully; never if any was discarded, cancelled or deleted.</summary>
    public BatchCallback? OnSuccess { get; init; }
}

/// <summary>
/// A job to enqueue when a batch finishes. It's prepared when the batch is inserted (defaults, insert middleware, unique
/// key) and inserted as is by the leader, which needn't know its args type. Its metadata carries
/// <c>oxbow:batch_id</c>, read through <see cref="JobRow.BatchId"/>.
/// </summary>
/// <param name="Args">The callback job's args.</param>
/// <param name="Options">Insert options. <see cref="InsertOptions.ScheduledAt"/> is an absolute time.</param>
public sealed record BatchCallback(IJobArgs Args, InsertOptions? Options = null);

/// <summary>The outcome of inserting a batch.</summary>
/// <param name="BatchId">The batch's ID.</param>
/// <param name="Jobs">
/// The insert results, in order. A job skipped as a unique duplicate isn't a member: the existing job keeps its own batch.
/// </param>
public sealed record BatchInsertResult(long BatchId, IReadOnlyList<InsertResult> Jobs);

/// <summary>A batch and its progress.</summary>
public sealed class Batch
{
    /// <summary>The batch's ID.</summary>
    public required long Id { get; init; }

    /// <summary>When the batch was inserted.</summary>
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>
    /// When the leader found every job finished and enqueued the callbacks; <see langword="null"/> until then. The
    /// counts are frozen from this point on.
    /// </summary>
    public DateTimeOffset? FinalizedAt { get; init; }

    /// <summary>The number of member jobs.</summary>
    public required int Total { get; init; }

    /// <summary>Members that completed successfully.</summary>
    public required int Completed { get; init; }

    /// <summary>Members discarded after running out of attempts.</summary>
    public required int Discarded { get; init; }

    /// <summary>Members cancelled, or deleted before the batch finished.</summary>
    public required int Cancelled { get; init; }

    /// <summary>Members not finished yet.</summary>
    public int Remaining => Total - Completed - Discarded - Cancelled;

    /// <summary>The <see cref="BatchOptions.OnComplete"/> job, once enqueued.</summary>
    public long? OnCompleteJobId { get; init; }

    /// <summary>The <see cref="BatchOptions.OnSuccess"/> job, once enqueued.</summary>
    public long? OnSuccessJobId { get; init; }
}

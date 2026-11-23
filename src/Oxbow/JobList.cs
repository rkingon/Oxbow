using System.Text.Json.Nodes;

namespace Oxbow;

/// <summary>
/// Which jobs <see cref="IOxbowClient.ListAsync"/> and the bulk operations act on. Filters combine with AND; unset
/// filters match everything.
/// </summary>
public record JobFilter
{
    /// <summary>Only jobs in these states.</summary>
    public IReadOnlyList<JobState>? States { get; init; }

    /// <summary>Only jobs in these queues.</summary>
    public IReadOnlyList<string>? Queues { get; init; }

    /// <summary>Only jobs of these kinds.</summary>
    public IReadOnlyList<string>? Kinds { get; init; }

    /// <summary>Only members of this batch.</summary>
    public long? BatchId { get; init; }

    /// <summary>Only jobs in this sequence.</summary>
    public string? SequenceKey { get; init; }

    /// <summary>
    /// Only jobs whose metadata contains this object (JSON containment, <c>@&gt;</c>): <c>{"tenant": "acme"}</c>
    /// matches any job with that key and value, whatever else its metadata holds.
    /// </summary>
    public JsonObject? Metadata { get; init; }

    /// <summary>Only jobs carrying all of these tags.</summary>
    public IReadOnlyList<string>? Tags { get; init; }

    /// <summary>
    /// Confirms that a bulk operation with no other filter set should act on every job. Bulk operations reject a filter
    /// that matches everything unless this is set; <see cref="IOxbowClient.ListAsync"/> ignores it.
    /// </summary>
    public bool All { get; init; }
}

/// <summary>Filters and paging for <see cref="IOxbowClient.ListAsync"/>. Bulk operations reject it; pass a <see cref="JobFilter"/>.</summary>
public sealed record JobListQuery : JobFilter
{
    /// <summary>The page size, 1 to 10,000. Default 100.</summary>
    public int Limit { get; init; } = 100;

    /// <summary>The cursor from the previous page's <see cref="JobListResult.NextCursor"/>.</summary>
    public string? Cursor { get; init; }

    /// <summary>Newest first instead of oldest first.</summary>
    public bool Descending { get; init; }
}

/// <summary>A page of jobs.</summary>
/// <param name="Jobs">The jobs.</param>
/// <param name="NextCursor">The cursor for the next page, or <see langword="null"/> after the last page.</param>
public sealed record JobListResult(IReadOnlyList<JobRow> Jobs, string? NextCursor);

/// <summary>The outcome of <see cref="IOxbowClient.CancelManyAsync"/>.</summary>
/// <param name="Cancelled">Jobs that hadn't started and are now cancelled.</param>
/// <param name="CancelRequested">
/// Running jobs whose nodes were told to stop them. Each ends cancelled, or completed if its worker finishes anyway.
/// </param>
public sealed record CancelManyResult(long Cancelled, long CancelRequested);

/// <summary>The outcome of <see cref="IOxbowClient.RetryManyAsync"/>.</summary>
/// <param name="Retried">Jobs made available to run now.</param>
/// <param name="SkippedUniqueConflicts">
/// Jobs left as they were because retrying them would take a unique key another live job holds.
/// </param>
public sealed record RetryManyResult(long Retried, long SkippedUniqueConflicts);

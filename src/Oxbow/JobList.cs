namespace Oxbow;

/// <summary>Filters and paging for <see cref="IOxbowClient.ListAsync"/>. Empty filters match everything.</summary>
public sealed record JobListQuery
{
    /// <summary>Only jobs in these states.</summary>
    public IReadOnlyList<JobState>? States { get; init; }

    /// <summary>Only jobs in these queues.</summary>
    public IReadOnlyList<string>? Queues { get; init; }

    /// <summary>Only jobs of these kinds.</summary>
    public IReadOnlyList<string>? Kinds { get; init; }

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

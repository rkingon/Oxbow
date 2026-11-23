using System.Text.Json;

namespace Oxbow;

/// <summary>A periodic job as stored: a schedule created at runtime, or the persisted state of one registered in code.</summary>
public sealed class PeriodicJobRow
{
    /// <summary>The job's name.</summary>
    public required string Name { get; init; }

    /// <summary>
    /// Registered with <see cref="OxbowOptions.AddPeriodicJob(string, PeriodicSchedule, Func{IJobArgs}, InsertOptions?, bool)"/>
    /// rather than created at runtime. Its args and options live in code, and only its run times are stored.
    /// </summary>
    public required bool RegisteredInCode { get; init; }

    /// <summary>The job kind. For a job registered in code, the kind of its last run, if it has run.</summary>
    public string? Kind { get; init; }

    /// <summary>The args each run is inserted with; <see langword="null"/> for a job registered in code.</summary>
    public JsonElement? Args { get; init; }

    /// <summary>
    /// The insert options each run is inserted with; <see langword="null"/> for a job registered in code, or if this
    /// version can't read them.
    /// </summary>
    public InsertOptions? Options { get; init; }

    /// <summary>The cron expression, or <see langword="null"/> for an interval schedule. Stored as given, not parsed on read.</summary>
    public string? CronExpression { get; init; }

    /// <summary>The ID of the time zone the cron expression is evaluated in, or <see langword="null"/> for an interval schedule.</summary>
    public string? TimeZoneId { get; init; }

    /// <summary>The interval, or <see langword="null"/> for a cron schedule.</summary>
    public TimeSpan? Interval { get; init; }

    /// <summary>When it was paused, or <see langword="null"/> if it isn't.</summary>
    public DateTimeOffset? PausedAt { get; init; }

    /// <summary>When the next run is due, or <see langword="null"/> if the schedule has no further occurrence.</summary>
    public DateTimeOffset? NextRunAt { get; init; }

    /// <summary>When the last run was enqueued.</summary>
    public DateTimeOffset? LastRunAt { get; init; }

    /// <summary>When the row was created.</summary>
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>When the definition last changed, or for a job registered in code, when a leader last confirmed it.</summary>
    public required DateTimeOffset UpdatedAt { get; init; }
}

/// <summary>Paging for <see cref="IOxbowClient.ListPeriodicJobsAsync"/>, ordered by name.</summary>
public sealed record PeriodicJobListQuery
{
    /// <summary>The page size, 1 to 10,000. Default 100.</summary>
    public int Limit { get; init; } = 100;

    /// <summary>The cursor from the previous page's <see cref="PeriodicJobListResult.NextCursor"/>.</summary>
    public string? Cursor { get; init; }
}

/// <summary>A page of periodic jobs.</summary>
/// <param name="Jobs">The jobs.</param>
/// <param name="NextCursor">The cursor for the next page, or <see langword="null"/> after the last page.</param>
public sealed record PeriodicJobListResult(IReadOnlyList<PeriodicJobRow> Jobs, string? NextCursor);

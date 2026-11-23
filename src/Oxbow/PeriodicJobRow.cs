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

    /// <summary>The insert options each run is inserted with; <see langword="null"/> for a job registered in code.</summary>
    public InsertOptions? Options { get; init; }

    /// <summary>When it runs.</summary>
    public required PeriodicSchedule Schedule { get; init; }

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

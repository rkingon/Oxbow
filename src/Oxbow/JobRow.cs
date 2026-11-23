using System.Globalization;
using System.Text.Json;
using Oxbow.Internal;

namespace Oxbow;

/// <summary>A job as stored, with its args as raw JSON.</summary>
public sealed class JobRow
{
    /// <summary>The job's ID.</summary>
    public required long Id { get; init; }

    /// <summary>The job's state.</summary>
    public required JobState State { get; init; }

    /// <summary>The attempt number: 0 before the first attempt, incremented when an attempt starts.</summary>
    public required int Attempt { get; init; }

    /// <summary>The number of attempts after which a failing job is discarded.</summary>
    public required int MaxAttempts { get; init; }

    /// <summary>When the current or last attempt started.</summary>
    public DateTimeOffset? AttemptedAt { get; init; }

    /// <summary>The client IDs that worked the job, most recent last.</summary>
    public required IReadOnlyList<string> AttemptedBy { get; init; }

    /// <summary>When the job was inserted.</summary>
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>When the job reached a terminal state.</summary>
    public DateTimeOffset? FinalizedAt { get; init; }

    /// <summary>When the job becomes eligible to run.</summary>
    public required DateTimeOffset ScheduledAt { get; init; }

    /// <summary>Priority from 1 (highest) to 4.</summary>
    public required int Priority { get; init; }

    /// <summary>The serialized args.</summary>
    public required JsonElement Args { get; init; }

    /// <summary>One entry per failed attempt.</summary>
    public required IReadOnlyList<AttemptError> Errors { get; init; }

    /// <summary>The job kind.</summary>
    public required string Kind { get; init; }

    /// <summary>Arbitrary JSON metadata.</summary>
    public required JsonElement Metadata { get; init; }

    /// <summary>The queue the job runs in.</summary>
    public required string Queue { get; init; }

    /// <summary>Tags attached at insert.</summary>
    public required IReadOnlyList<string> Tags { get; init; }

    /// <summary>
    /// The last heartbeat (<c>oxbow:heartbeat_at</c> in <see cref="Metadata"/>), from any attempt: compare it with
    /// <see cref="AttemptedAt"/>.
    /// </summary>
    public DateTimeOffset? HeartbeatAt =>
        Metadata.ValueKind == JsonValueKind.Object
        && Metadata.TryGetProperty("oxbow:heartbeat_at", out var value)
        && value.ValueKind == JsonValueKind.String
        && DateTimeOffset.TryParse(value.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var at)
            ? at
            : null;

    /// <summary>
    /// The last progress reported (<c>oxbow:progress</c> in <see cref="Metadata"/>), from any attempt: compare its
    /// <see cref="JobProgress.At"/> with <see cref="AttemptedAt"/>.
    /// </summary>
    public JobProgress? Progress
    {
        get
        {
            if (Metadata.ValueKind != JsonValueKind.Object
                || !Metadata.TryGetProperty("oxbow:progress", out var value)
                || value.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            try
            {
                return value.Deserialize(OxbowJsonContext.Default.JobProgress);
            }
            catch (JsonException)
            {
                return null;
            }
        }
    }

    /// <summary>
    /// The batch this job is a member or callback of, from <c>metadata.oxbow:batch_id</c>; pass it to
    /// <see cref="IOxbowClient.GetBatchAsync"/>.
    /// </summary>
    public long? BatchId =>
        Metadata.ValueKind == JsonValueKind.Object
        && Metadata.TryGetProperty("oxbow:batch_id", out var id)
        && id.ValueKind == JsonValueKind.Number
        && id.TryGetInt64(out var value)
            ? value
            : null;
}

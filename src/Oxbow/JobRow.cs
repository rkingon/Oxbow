using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
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

    /// <summary>The largest output a job may record, in bytes of UTF-8 JSON (1 MiB).</summary>
    public const int MaxOutputSize = 1024 * 1024;

    /// <summary>The output the worker recorded when the job completed (<c>metadata.output</c>), if any.</summary>
    public JsonElement? Output =>
        Metadata.ValueKind == JsonValueKind.Object && Metadata.TryGetProperty("output", out var output) ? output : null;

    /// <summary>Deserializes <see cref="Output"/>.</summary>
    /// <typeparam name="T">The output type.</typeparam>
    /// <param name="options">
    /// Serializer options, typically the client's <see cref="OxbowOptions.JsonSerializerOptions"/>. Default:
    /// <see cref="JsonSerializerOptions.Web"/>.
    /// </param>
    /// <returns>The output, or <see langword="default"/> if none was recorded.</returns>
    public T? GetOutput<T>(JsonSerializerOptions? options = null)
    {
        if (Output is not { } output)
        {
            return default;
        }

        options ??= JsonSerializerOptions.Web;
        if (!options.IsReadOnly)
        {
            // As the client does: leave the caller's options mutable, and fill in a missing resolver.
            options = new JsonSerializerOptions(options);
            options.TypeInfoResolver ??= new DefaultJsonTypeInfoResolver();
        }

        return output.Deserialize((JsonTypeInfo<T>)options.GetTypeInfo(typeof(T)));
    }
}

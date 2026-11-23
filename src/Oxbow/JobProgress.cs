using System.Text.Json.Serialization;

namespace Oxbow;

/// <summary>Progress reported by a worker, stored under <c>oxbow:progress</c> in the job's metadata.</summary>
/// <param name="Percent">How far along the work is, from 0 to 100.</param>
/// <param name="Message">A description of the current step, if any.</param>
/// <param name="Attempt">The attempt that reported it.</param>
/// <param name="At">When it was reported.</param>
public sealed record JobProgress(
    [property: JsonPropertyName("percent")] double Percent,
    [property: JsonPropertyName("message")] string? Message,
    [property: JsonPropertyName("attempt")] int Attempt,
    [property: JsonPropertyName("at")] DateTimeOffset At);

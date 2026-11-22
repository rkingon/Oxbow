using System.Text.Json.Serialization;

namespace Oxbow;

/// <summary>One failed attempt in a job's error history.</summary>
/// <param name="At">When the attempt started.</param>
/// <param name="Attempt">The attempt number that failed.</param>
/// <param name="Error">The error, as <c>{ExceptionType}: {Message}</c>.</param>
/// <param name="Trace">The stack trace, if any.</param>
public sealed record AttemptError(
    [property: JsonPropertyName("at")] DateTimeOffset At,
    [property: JsonPropertyName("attempt")] int Attempt,
    [property: JsonPropertyName("error")] string Error,
    [property: JsonPropertyName("trace")] string? Trace);

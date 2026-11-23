using System.Text.Json.Serialization;

namespace Oxbow.Internal;

[JsonSerializable(typeof(AttemptError))]
[JsonSerializable(typeof(ControlPayload))]
[JsonSerializable(typeof(InsertPayload))]
internal sealed partial class OxbowJsonContext : JsonSerializerContext;

internal sealed record ControlPayload(
    [property: JsonPropertyName("action")] string Action,
    [property: JsonPropertyName("queue")] string? Queue,
    [property: JsonPropertyName("job_id")] long? JobId,
    [property: JsonPropertyName("job_ids")] long[]? JobIds,
    [property: JsonPropertyName("leader_id")] string? LeaderId);

internal sealed record InsertPayload([property: JsonPropertyName("queue")] string Queue);

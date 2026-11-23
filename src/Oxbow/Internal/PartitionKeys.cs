using System.Buffers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Oxbow.Internal;

/// <summary>Derives a partition key from the args' <see cref="PartitionKeyAttribute"/> properties.</summary>
internal static class PartitionKeys
{
    public const int MaxLength = 255;

    /// <summary>
    /// The marked properties as a JSON object sorted by name, or null when none are marked (or the args are raw JSON,
    /// with no type to mark). For encrypted args it's the protector's keyed hash of that, in hex, so the column doesn't
    /// reveal the values.
    /// </summary>
    public static string? Derive(string encodedArgs, JsonTypeInfo? typeInfo, IJobArgsProtector? protector = null)
    {
        if (typeInfo is null)
        {
            return null;
        }

        var marked = typeInfo.Properties
            .Where(p => p.AttributeProvider?.IsDefined(typeof(PartitionKeyAttribute), inherit: true) == true)
            .Select(p => p.Name)
            .Order(StringComparer.Ordinal)
            .ToList();
        if (marked.Count == 0)
        {
            return null;
        }

        using var document = JsonDocument.Parse(encodedArgs);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("Args with [PartitionKey] properties must serialize to a JSON object.");
        }

        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var name in marked)
            {
                if (document.RootElement.TryGetProperty(name, out var value))
                {
                    writer.WritePropertyName(name);
                    writer.WriteRawValue(value.GetRawText(), skipInputValidation: true);
                }
            }

            writer.WriteEndObject();
        }

        return protector is null
            ? Encoding.UTF8.GetString(buffer.WrittenSpan)
            : Convert.ToHexStringLower(protector.HashUniqueKey(buffer.WrittenSpan));
    }

    public static void Validate(string? key)
    {
        if (key is not null && (key.Length is 0 or > MaxLength))
        {
            throw new ArgumentException($"PartitionKey must be 1 to {MaxLength} characters; got {key.Length}.", nameof(key));
        }
    }
}

using System.Buffers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Oxbow.Internal;

/// <summary>
/// Resolves a job's sequence key: the explicit option, else <c>kind:{marked properties}</c>. For an encrypted kind the
/// marked values are replaced by the protector's keyed hash, so the column doesn't reveal them.
/// </summary>
internal static class SequenceKeys
{
    public const int MaxLength = 255;

    public static string? Resolve(
        string? explicitKey,
        string kind,
        string encodedArgs,
        JsonTypeInfo? argsTypeInfo,
        IJobArgsProtector? protector = null)
    {
        var key = explicitKey ?? FromAttributes(kind, encodedArgs, argsTypeInfo, protector);
        if (key is not null && (key.Length == 0 || key.Length > MaxLength))
        {
            throw new ArgumentException(
                $"Sequence key for kind '{kind}' must be 1 to {MaxLength} characters; it was {key.Length}.");
        }

        return key;
    }

    private static string? FromAttributes(string kind, string encodedArgs, JsonTypeInfo? typeInfo, IJobArgsProtector? protector)
    {
        // Raw args (stored periodic jobs) carry no attributes; only an explicit key applies.
        if (typeInfo is null || typeInfo.Kind != JsonTypeInfoKind.Object)
        {
            return null;
        }

        var marked = typeInfo.Properties
            .Where(p => p.AttributeProvider?.IsDefined(typeof(SequenceKeyAttribute), inherit: true) == true)
            .OrderBy(p => p.Name, StringComparer.Ordinal)
            .ToList();
        if (marked.Count == 0)
        {
            return null;
        }

        using var document = JsonDocument.Parse(encodedArgs);
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var property in marked)
            {
                writer.WritePropertyName(property.Name);
                if (document.RootElement.TryGetProperty(property.Name, out var value))
                {
                    value.WriteTo(writer);
                }
                else
                {
                    writer.WriteNullValue();
                }
            }

            writer.WriteEndObject();
        }

        return protector is null
            ? kind + ":" + Encoding.UTF8.GetString(buffer.WrittenSpan)
            : kind + ":hmac:" + Convert.ToHexStringLower(protector.HashUniqueKey([.. "sequence:"u8, .. buffer.WrittenSpan]));
    }
}

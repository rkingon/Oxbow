using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Oxbow.Internal;

/// <summary>Builds River-style unique keys: <c>&amp;kind=…&amp;args=…&amp;period=…&amp;queue=…</c>, hashed with SHA-256.</summary>
internal static class UniqueKeys
{
    private const int MaxDepth = 16;

    private static readonly JobState[] DefaultStates =
        [JobState.Available, JobState.Completed, JobState.Pending, JobState.Retryable, JobState.Running, JobState.Scheduled];

    private static readonly JobState[] RequiredStates =
        [JobState.Available, JobState.Pending, JobState.Running, JobState.Scheduled];

    public static bool IsEmpty(UniqueOptions options) =>
        !options.ByArgs && options.ByPeriod is null && !options.ByQueue && options.ByState is null && !options.ExcludeKind;

    public static void Validate(UniqueOptions options)
    {
        if (options.ByPeriod is { } period && period < TimeSpan.FromSeconds(1))
        {
            throw new ArgumentException("UniqueOptions.ByPeriod must be at least one second.", nameof(options));
        }

        if (options.ExcludeKind && !options.ByArgs && !options.ByQueue && options.ByPeriod is null)
        {
            throw new ArgumentException("UniqueOptions.ExcludeKind requires ByArgs, ByQueue or ByPeriod.", nameof(options));
        }

        if (options.ByState is { } states)
        {
            var missing = RequiredStates.Where(s => !states.Contains(s)).ToList();
            if (missing.Count > 0)
            {
                throw new ArgumentException(
                    $"UniqueOptions.ByState must include available, pending, running and scheduled; missing: {string.Join(", ", missing)}.",
                    nameof(options));
            }
        }
    }

    public static int StatesBitmask(UniqueOptions options)
    {
        var bitmask = 0;
        foreach (var state in options.ByState ?? DefaultStates)
        {
            bitmask |= JobStates.Bit(state);
        }

        return bitmask;
    }

    public static byte[] Build(
        UniqueOptions options,
        string kind,
        string encodedArgs,
        JsonTypeInfo argsTypeInfo,
        string queue,
        DateTimeOffset scheduledAt)
    {
        var key = new StringBuilder();
        if (!options.ExcludeKind)
        {
            key.Append("&kind=").Append(kind);
        }

        if (options.ByArgs)
        {
            key.Append("&args=").Append(UniqueArgs(encodedArgs, argsTypeInfo));
        }

        if (options.ByPeriod is { } period)
        {
            // Truncate since 0001-01-01, as Go's time.Truncate does, so keys match River's.
            var ticks = scheduledAt.UtcTicks;
            var lowerBound = new DateTime(ticks - (ticks % period.Ticks), DateTimeKind.Utc);
            key.Append("&period=").Append(lowerBound.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture));
        }

        if (options.ByQueue)
        {
            key.Append("&queue=").Append(queue);
        }

        return SHA256.HashData(Encoding.UTF8.GetBytes(key.ToString()));
    }

    private static string UniqueArgs(string encodedArgs, JsonTypeInfo typeInfo)
    {
        using var document = JsonDocument.Parse(encodedArgs);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("Unique args must serialize to a JSON object.");
        }

        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            if (!HasUniqueProperties(typeInfo, 0))
            {
                WriteSorted(writer, document.RootElement.EnumerateObject()
                    .DistinctBy(p => p.Name, StringComparer.Ordinal)
                    .Select(p => (p.Name, (Action)(() => writer.WriteRawValue(p.Value.GetRawText(), skipInputValidation: true)))));
            }
            else
            {
                WriteUnique(writer, typeInfo, document.RootElement, 0);
            }
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static void WriteUnique(Utf8JsonWriter writer, JsonTypeInfo typeInfo, JsonElement element, int depth)
    {
        var entries = new List<(string Name, Action Write)>();
        foreach (var property in typeInfo.Properties)
        {
            if (!element.TryGetProperty(property.Name, out var value))
            {
                continue;
            }

            var nested = NestedTypeInfo(typeInfo, property, depth);
            if (nested is not null && value.ValueKind == JsonValueKind.Object && HasUniqueProperties(nested, depth + 1))
            {
                entries.Add((property.Name, () => WriteUnique(writer, nested, value, depth + 1)));
            }
            else if (IsUnique(property))
            {
                entries.Add((property.Name, () => writer.WriteRawValue(value.GetRawText(), skipInputValidation: true)));
            }
        }

        WriteSorted(writer, entries);
    }

    private static void WriteSorted(Utf8JsonWriter writer, IEnumerable<(string Name, Action Write)> entries)
    {
        writer.WriteStartObject();
        foreach (var (name, write) in entries.OrderBy(e => e.Name, StringComparer.Ordinal))
        {
            writer.WritePropertyName(name);
            write();
        }

        writer.WriteEndObject();
    }

    private static bool HasUniqueProperties(JsonTypeInfo typeInfo, int depth)
    {
        if (depth > MaxDepth)
        {
            return false;
        }

        foreach (var property in typeInfo.Properties)
        {
            if (IsUnique(property))
            {
                return true;
            }

            if (NestedTypeInfo(typeInfo, property, depth) is { } nested && HasUniqueProperties(nested, depth + 1))
            {
                return true;
            }
        }

        return false;
    }

    private static JsonTypeInfo? NestedTypeInfo(JsonTypeInfo parent, JsonPropertyInfo property, int depth)
    {
        if (depth >= MaxDepth || property.PropertyType == typeof(string) || property.PropertyType.IsPrimitive)
        {
            return null;
        }

        var typeInfo = parent.Options.GetTypeInfo(property.PropertyType);
        return typeInfo.Kind == JsonTypeInfoKind.Object ? typeInfo : null;
    }

    private static bool IsUnique(JsonPropertyInfo property) =>
        property.AttributeProvider?.IsDefined(typeof(UniqueKeyAttribute), inherit: true) == true;
}

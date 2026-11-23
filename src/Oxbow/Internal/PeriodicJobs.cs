using System.Text.Json;
using System.Text.Json.Serialization;
using Npgsql;

namespace Oxbow.Internal;

internal static class PeriodicJobNames
{
    public const string MetadataKey = "oxbow:periodic_job";

    public static bool IsValid(string name) => !string.IsNullOrWhiteSpace(name) && name.Length < 256;

    public static string Derive(string kind, PeriodicSchedule schedule) => $"{kind} {schedule}";
}

/// <summary>The args of a stored periodic job whose kind has no worker on this node, kept as the stored JSON.</summary>
internal sealed class RawJobArgs(string kind, string json) : IJobArgs
{
    public string Kind => kind;

    public string Json => json;
}

/// <summary>Stored insert options. Snake case and string enums, so the column reads like the rest of the schema.</summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    UseStringEnumConverter = true)]
[JsonSerializable(typeof(InsertOptions))]
internal sealed partial class PeriodicJsonContext : JsonSerializerContext;

internal static class PeriodicJobRowReader
{
    public static PeriodicJobRow Read(NpgsqlDataReader reader) => new()
    {
        Name = reader.GetString(0),
        RegisteredInCode = reader.GetBoolean(1),
        Kind = reader.IsDBNull(2) ? null : reader.GetString(2),
        Args = reader.IsDBNull(3) ? null : ParseJson(reader.GetString(3)),
        Options = reader.IsDBNull(4) ? null : ReadOptions(reader.GetString(4)),
        CronExpression = reader.IsDBNull(5) ? null : reader.GetString(5),
        TimeZoneId = reader.IsDBNull(6) ? null : reader.GetString(6),
        Interval = reader.IsDBNull(7) ? null : TimeSpan.FromSeconds(reader.GetDouble(7)),
        PausedAt = reader.IsDBNull(8) ? null : reader.GetFieldValue<DateTimeOffset>(8),
        NextRunAt = reader.IsDBNull(9) ? null : reader.GetFieldValue<DateTimeOffset>(9),
        LastRunAt = reader.IsDBNull(10) ? null : reader.GetFieldValue<DateTimeOffset>(10),
        CreatedAt = reader.GetFieldValue<DateTimeOffset>(11),
        UpdatedAt = reader.GetFieldValue<DateTimeOffset>(12),
    };

    public static async Task<List<PeriodicJobRow>> ReadAllAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var rows = new List<PeriodicJobRow>();
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                rows.Add(Read(reader));
            }
        }

        return rows;
    }

    // One row this version can't read mustn't break listing the rest.
    private static InsertOptions? ReadOptions(string json)
    {
        try
        {
            return JsonSerializer.Deserialize(json, PeriodicJsonContext.Default.InsertOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static JsonElement ParseJson(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }
}

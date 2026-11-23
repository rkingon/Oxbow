using System.Text.Json;
using Npgsql;

namespace Oxbow.Internal;

internal static class JobRowReader
{
    public static JobRow Read(NpgsqlDataReader reader)
    {
        return new JobRow
        {
            Id = reader.GetInt64(0),
            State = JobStates.Parse(reader.GetString(1)),
            Attempt = reader.GetInt16(2),
            MaxAttempts = reader.GetInt16(3),
            AttemptedAt = reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTimeOffset>(4),
            CreatedAt = reader.GetFieldValue<DateTimeOffset>(5),
            FinalizedAt = reader.IsDBNull(6) ? null : reader.GetFieldValue<DateTimeOffset>(6),
            ScheduledAt = reader.GetFieldValue<DateTimeOffset>(7),
            Priority = reader.GetInt16(8),
            Args = ParseJson(reader.GetString(9)),
            AttemptedBy = reader.IsDBNull(10) ? [] : reader.GetFieldValue<string[]>(10),
            Errors = reader.IsDBNull(11) ? [] : ParseErrors(reader.GetFieldValue<string[]>(11)),
            Kind = reader.GetString(12),
            Metadata = ParseJson(reader.GetString(13)),
            Queue = reader.GetString(14),
            Tags = reader.GetFieldValue<string[]>(15),
            SequenceKey = reader.IsDBNull(16) ? null : reader.GetString(16),
        };
    }

    public static async Task<List<JobRow>> ReadAllAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var rows = new List<JobRow>();
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

    public static async Task<JobRow?> ReadSingleAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var rows = await ReadAllAsync(command, cancellationToken).ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    private static JsonElement ParseJson(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }

    private static AttemptError[] ParseErrors(string[] errors)
    {
        var parsed = new AttemptError[errors.Length];
        for (var i = 0; i < errors.Length; i++)
        {
            parsed[i] = JsonSerializer.Deserialize(errors[i], OxbowJsonContext.Default.AttemptError)
                ?? throw new InvalidOperationException("A job's error entry was null.");
        }

        return parsed;
    }
}

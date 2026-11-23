using Npgsql;

namespace Oxbow.Internal;

/// <summary>
/// Makes the next job of each given sequence live. Safe to run from any node at any time: the live index admits one
/// job per sequence, so a promoter that loses a race retries on a fresh snapshot and finds the sequence busy. A
/// deadlock is only possible between two such losers, and is retried the same way.
/// </summary>
internal static class SequencePromoter
{
    private const int MaxTries = 3;

    /// <summary>Returns the queues of jobs made available; their producers have been notified.</summary>
    public static async Task<IReadOnlyCollection<string>> PromoteAsync(
        OxbowRuntime runtime,
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken)
    {
        if (keys.Count == 0)
        {
            return [];
        }

        // Postgres truncates identifiers to 63 bytes, and so the name it reports; prefixes are ASCII.
        var liveIndex = runtime.Options.TablePrefix + "job_sequence_live_idx";
        liveIndex = liveIndex[..Math.Min(liveIndex.Length, 63)];
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var queues = new HashSet<string>(StringComparer.Ordinal);
                var command = runtime.DataSource.CreateCommand(runtime.Sql.SequencePromote);
                await using (command.ConfigureAwait(false))
                {
                    command.Parameters.Add(OxbowClient.TextArray("keys", keys.ToArray()));
                    command.Parameters.AddWithValue("channel", runtime.Sql.InsertChannel);
                    var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                    await using (reader.ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            if (reader.GetString(1) == "available")
                            {
                                queues.Add(reader.GetString(0));
                            }
                        }
                    }
                }

                return queues;
            }
            catch (PostgresException ex) when (
                (ex.SqlState == PostgresErrorCodes.UniqueViolation && ex.ConstraintName == liveIndex)
                || ex.SqlState == PostgresErrorCodes.DeadlockDetected)
            {
                // Losing every try means other promoters keep winning; the leader's sweep picks up the rest.
                if (attempt == MaxTries)
                {
                    return [];
                }
            }
        }
    }
}

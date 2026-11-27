using Npgsql;
using NpgsqlTypes;

namespace Oxbow.Internal;

/// <summary>
/// Renders a <see cref="JobFilter"/> as a WHERE clause holding only the filters that are set, so the planner sees
/// plain predicates it can match to indexes rather than <c>@x IS NULL OR …</c>.
/// </summary>
internal static class JobFilterSql
{
    public static bool MatchesEverything(JobFilter filter) =>
        filter.States is null
        && filter.Queues is null
        && filter.Kinds is null
        && filter.BatchId is null
        && filter.SequenceKey is null
        && filter.PartitionKey is null
        && (filter.Metadata is null || filter.Metadata.Count == 0)
        && (filter.Tags is null || filter.Tags.Count == 0);

    /// <summary>The clause, with its parameters added to <paramref name="parameters"/>.</summary>
    public static string Where(JobFilter filter, string stateTypeName, NpgsqlParameterCollection parameters)
    {
        var clauses = new List<string>(8);
        if (filter.States is { } states)
        {
            clauses.Add($"state = ANY(@filter_states::{stateTypeName}[])");
            parameters.Add(OxbowClient.TextArray("filter_states", states.Select(JobStates.ToDb).ToArray()));
        }

        if (filter.Queues is { } queues)
        {
            clauses.Add("queue = ANY(@filter_queues)");
            parameters.Add(OxbowClient.TextArray("filter_queues", queues.ToArray()));
        }

        if (filter.Kinds is { } kinds)
        {
            clauses.Add("kind = ANY(@filter_kinds)");
            parameters.Add(OxbowClient.TextArray("filter_kinds", kinds.ToArray()));
        }

        if (filter.BatchId is { } batchId)
        {
            clauses.Add("batch_id = @filter_batch_id");
            parameters.Add(new NpgsqlParameter("filter_batch_id", NpgsqlDbType.Bigint) { Value = batchId });
        }

        if (filter.SequenceKey is { } sequenceKey)
        {
            clauses.Add("sequence_key = @filter_sequence_key");
            parameters.Add(new NpgsqlParameter("filter_sequence_key", NpgsqlDbType.Text) { Value = sequenceKey });
        }

        if (filter.PartitionKey is { } partitionKey)
        {
            clauses.Add("partition_key = @filter_partition_key");
            parameters.Add(new NpgsqlParameter("filter_partition_key", NpgsqlDbType.Text) { Value = partitionKey });
        }

        if (filter.Metadata is { Count: > 0 } metadata)
        {
            clauses.Add("metadata @> @filter_metadata::jsonb");
            parameters.Add(new NpgsqlParameter("filter_metadata", NpgsqlDbType.Text) { Value = metadata.ToJsonString() });
        }

        if (filter.Tags is { Count: > 0 } tags)
        {
            // Unbounded varchar: a typmod cast would truncate a long filter tag into one that matches.
            clauses.Add("tags @> @filter_tags::varchar[]");
            parameters.Add(OxbowClient.TextArray("filter_tags", tags.ToArray()));
        }

        return clauses.Count == 0 ? "TRUE" : string.Join(" AND ", clauses);
    }
}

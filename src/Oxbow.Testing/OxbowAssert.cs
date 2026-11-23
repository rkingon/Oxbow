using System.Text.Json;
using Npgsql;

namespace Oxbow.Testing;

/// <summary>
/// Asserts that jobs were, or weren't, inserted. Works inside the test's own transaction, so a test can insert and
/// assert without committing. Throws <see cref="OxbowAssertionException"/> on failure, so it works with any test
/// framework.
/// </summary>
/// <param name="schema">The Postgres schema holding Oxbow's tables.</param>
/// <param name="tablePrefix">The prefix on Oxbow's objects.</param>
public sealed class OxbowAssert(string schema = "public", string tablePrefix = "oxbow_")
{
    private readonly string _table = Validate(schema, tablePrefix);

    /// <summary>
    /// Decrypts args of kinds that encrypt them: the app's <see cref="IJobArgsProtector"/>, e.g. resolved from its
    /// service provider. Without it, asserting on an encrypted job throws.
    /// </summary>
    public IJobArgsProtector? ArgsProtector { get; init; }

    /// <summary>Asserts exactly one job of <typeparamref name="TArgs"/>'s kind matches, and returns it.</summary>
    /// <typeparam name="TArgs">The args type.</typeparam>
    /// <param name="connection">An open connection.</param>
    /// <param name="transaction">The transaction to look in, if any.</param>
    /// <param name="filter">Further conditions the job must meet.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The matching job, with its args deserialized using <see cref="InsertedJobFilter{TArgs}.JsonSerializerOptions"/>.</returns>
    public async Task<Job<TArgs>> InsertedAsync<TArgs>(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction = null,
        InsertedJobFilter<TArgs>? filter = null,
        CancellationToken cancellationToken = default)
        where TArgs : IJobArgs
    {
        var matches = await FindAsync(connection, transaction, filter, cancellationToken).ConfigureAwait(false);
        return matches.Count switch
        {
            1 => matches[0],
            0 => throw new OxbowAssertionException($"Expected one inserted '{KindOf<TArgs>()}' job matching the filter, found none."),
            _ => throw new OxbowAssertionException($"Expected one inserted '{KindOf<TArgs>()}' job matching the filter, found {matches.Count}."),
        };
    }

    /// <summary>Asserts no job of <typeparamref name="TArgs"/>'s kind matches.</summary>
    /// <typeparam name="TArgs">The args type.</typeparam>
    /// <param name="connection">An open connection.</param>
    /// <param name="transaction">The transaction to look in, if any.</param>
    /// <param name="filter">Further conditions; only jobs meeting them count.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when the assertion passes.</returns>
    public async Task NotInsertedAsync<TArgs>(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction = null,
        InsertedJobFilter<TArgs>? filter = null,
        CancellationToken cancellationToken = default)
        where TArgs : IJobArgs
    {
        var matches = await FindAsync(connection, transaction, filter, cancellationToken).ConfigureAwait(false);
        if (matches.Count > 0)
        {
            throw new OxbowAssertionException(
                $"Expected no inserted '{KindOf<TArgs>()}' job matching the filter, found {matches.Count} (IDs {string.Join(", ", matches.Select(m => m.Id))}).");
        }
    }

    /// <summary>All jobs of <typeparamref name="TArgs"/>'s kind matching the filter, oldest first.</summary>
    /// <typeparam name="TArgs">The args type.</typeparam>
    /// <param name="connection">An open connection.</param>
    /// <param name="transaction">The transaction to look in, if any.</param>
    /// <param name="filter">Further conditions the jobs must meet.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The matching jobs.</returns>
    public async Task<IReadOnlyList<Job<TArgs>>> FindAsync<TArgs>(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction = null,
        InsertedJobFilter<TArgs>? filter = null,
        CancellationToken cancellationToken = default)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(connection);
        filter ??= new InsertedJobFilter<TArgs>();
        var json = new JsonSerializerOptions(filter.JsonSerializerOptions ?? new JsonSerializerOptions(JsonSerializerDefaults.Web));
        json.TypeInfoResolver ??= new System.Text.Json.Serialization.Metadata.DefaultJsonTypeInfoResolver();

        var command = new NpgsqlCommand(
            $"""
            SELECT id, state::text, attempt, max_attempts, attempted_at, created_at, finalized_at, scheduled_at, priority,
                   args::text, coalesce(attempted_by, '{"{}"}'), kind, metadata::text, queue, tags::text[], sequence_key
            FROM {_table}
            WHERE kind = @kind
            ORDER BY id
            """,
            connection,
            transaction);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("kind", KindOf<TArgs>());
            var jobs = new List<Job<TArgs>>();
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = new JobRow
                    {
                        Id = reader.GetInt64(0),
                        State = Enum.Parse<JobState>(reader.GetString(1), ignoreCase: true),
                        Attempt = reader.GetInt16(2),
                        MaxAttempts = reader.GetInt16(3),
                        AttemptedAt = reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTimeOffset>(4),
                        CreatedAt = reader.GetFieldValue<DateTimeOffset>(5),
                        FinalizedAt = reader.IsDBNull(6) ? null : reader.GetFieldValue<DateTimeOffset>(6),
                        ScheduledAt = reader.GetFieldValue<DateTimeOffset>(7),
                        Priority = reader.GetInt16(8),
                        Args = JsonDocument.Parse(reader.GetString(9)).RootElement.Clone(),
                        AttemptedBy = reader.GetFieldValue<string[]>(10),
                        Errors = [],
                        Kind = reader.GetString(11),
                        Metadata = JsonDocument.Parse(reader.GetString(12)).RootElement.Clone(),
                        Queue = reader.GetString(13),
                        Tags = reader.GetFieldValue<string[]>(14),
                        SequenceKey = reader.IsDBNull(15) ? null : reader.GetString(15),
                    };
                    var args = (TArgs)Args(row).Deserialize(json.GetTypeInfo(typeof(TArgs)))!;
                    var job = new Job<TArgs>(row, args);
                    if (filter.Matches(job))
                    {
                        jobs.Add(job);
                    }
                }
            }

            return jobs;
        }
    }

    private JsonElement Args(JobRow row)
    {
        if (ArgsProtector is not null)
        {
            return ArgsProtector.UnprotectArgs(row);
        }

        return row.Args.ValueKind == JsonValueKind.String && row.Args.GetString()!.StartsWith("oxbow:enc:", StringComparison.Ordinal)
            ? throw new OxbowAssertionException($"Args of job {row.Id} are encrypted. Set OxbowAssert.ArgsProtector to decrypt them.")
            : row.Args;
    }

    private static string KindOf<TArgs>() =>
        typeof(TArgs).GetCustomAttributes(typeof(JobKindAttribute), inherit: false) is [JobKindAttribute attribute, ..]
            ? attribute.Kind
            : throw new InvalidOperationException($"'{typeof(TArgs).FullName}' has no [JobKind] attribute.");

    private static string Validate(string schema, string prefix)
    {
        // Reuse the schema API's identifier validation.
        _ = OxbowSchema.GetGrants("validation", schema, prefix);
        return $"{schema}.{prefix}job";
    }
}

/// <summary>Conditions an inserted job must meet. Unset properties match anything.</summary>
/// <typeparam name="TArgs">The args type.</typeparam>
public sealed record InsertedJobFilter<TArgs>
    where TArgs : IJobArgs
{
    /// <summary>The job's args must satisfy this.</summary>
    public Func<TArgs, bool>? Args { get; init; }

    /// <summary>The job's queue.</summary>
    public string? Queue { get; init; }

    /// <summary>The job's priority.</summary>
    public int? Priority { get; init; }

    /// <summary>The job's max attempts.</summary>
    public int? MaxAttempts { get; init; }

    /// <summary>The job's state.</summary>
    public JobState? State { get; init; }

    /// <summary>Tags the job must have (it may have more).</summary>
    public IReadOnlyList<string>? Tags { get; init; }

    /// <summary>Serializer options matching the client's, for deserializing args. Default: web defaults.</summary>
    public JsonSerializerOptions? JsonSerializerOptions { get; init; }

    internal bool Matches(Job<TArgs> job) =>
        (Args is null || Args(job.Args))
        && (Queue is null || job.Queue == Queue)
        && (Priority is null || job.Row.Priority == Priority)
        && (MaxAttempts is null || job.MaxAttempts == MaxAttempts)
        && (State is null || job.Row.State == State)
        && (Tags is null || Tags.All(job.Row.Tags.Contains));
}

/// <summary>An Oxbow test assertion failed.</summary>
public sealed class OxbowAssertionException : Exception
{
    /// <summary>Creates the exception.</summary>
    public OxbowAssertionException()
    {
    }

    /// <summary>Creates the exception with a message.</summary>
    /// <param name="message">What failed.</param>
    public OxbowAssertionException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and a cause.</summary>
    /// <param name="message">What failed.</param>
    /// <param name="innerException">The cause.</param>
    public OxbowAssertionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

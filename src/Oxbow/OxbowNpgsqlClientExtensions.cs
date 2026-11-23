using System.Text.Json;
using Npgsql;

namespace Oxbow;

/// <summary>
/// <see cref="IOxbowNpgsqlClient"/>'s members on <see cref="IOxbowClient"/>, so code written against
/// <see cref="IOxbowClient"/> keeps calling them unchanged.
/// </summary>
public static class OxbowNpgsqlClientExtensions
{
    /// <summary>
    /// Inserts a job on the caller's connection, inside <paramref name="transaction"/> if given, so the job commits or
    /// rolls back with the caller's data.
    /// </summary>
    /// <typeparam name="TArgs">The args type.</typeparam>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="connection">An open connection.</param>
    /// <param name="transaction">The transaction to insert in, if any.</param>
    /// <param name="args">The job's args.</param>
    /// <param name="options">Insert options.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The insert result.</returns>
    public static Task<InsertResult> InsertAsync<TArgs>(
        this IOxbowClient client,
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        TArgs args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default)
        where TArgs : IJobArgs =>
        Npgsql(client).InsertAsync(connection, transaction, args, options, cancellationToken);

    /// <summary>Inserts many jobs in one statement on the caller's connection and transaction.</summary>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="connection">An open connection.</param>
    /// <param name="transaction">The transaction to insert in, if any.</param>
    /// <param name="items">The jobs.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The insert results, in order.</returns>
    public static Task<IReadOnlyList<InsertResult>> InsertManyAsync(
        this IOxbowClient client,
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IEnumerable<InsertManyItem> items,
        CancellationToken cancellationToken = default) =>
        Npgsql(client).InsertManyAsync(connection, transaction, items, cancellationToken);

    /// <summary>Inserts many jobs with binary COPY on the caller's connection and transaction.</summary>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="connection">An open connection.</param>
    /// <param name="transaction">The transaction to insert in, if any.</param>
    /// <param name="items">The jobs.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The number of jobs inserted.</returns>
    public static Task<int> InsertManyFastAsync(
        this IOxbowClient client,
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IEnumerable<InsertManyItem> items,
        CancellationToken cancellationToken = default) =>
        Npgsql(client).InsertManyFastAsync(connection, transaction, items, cancellationToken);

    /// <summary>
    /// Inserts a batch of jobs on the caller's connection and transaction, so the batch commits or rolls back with the
    /// caller's data.
    /// </summary>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="connection">An open connection.</param>
    /// <param name="transaction">The transaction to insert in, if any.</param>
    /// <param name="items">The member jobs.</param>
    /// <param name="options">The callbacks.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The batch ID and the insert results, in order.</returns>
    public static Task<BatchInsertResult> InsertBatchAsync(
        this IOxbowClient client,
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IEnumerable<InsertManyItem> items,
        BatchOptions? options = null,
        CancellationToken cancellationToken = default) =>
        Npgsql(client).InsertBatchAsync(connection, transaction, items, options, cancellationToken);

    /// <summary>
    /// Creates a periodic job stored in the database, which the leader enqueues without a restart. Fails if the name is
    /// taken, so an admin tool can't overwrite a job by accident.
    /// </summary>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="name">A unique name, 1 to 255 characters.</param>
    /// <param name="schedule">When it runs. A cron time zone must be one the system can find by ID.</param>
    /// <param name="args">The args each run is inserted with.</param>
    /// <param name="options">The insert options each run is inserted with. <see cref="InsertOptions.ScheduledAt"/> isn't allowed.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The stored periodic job.</returns>
    /// <exception cref="PeriodicJobExistsException">A periodic job with the name exists.</exception>
    public static Task<PeriodicJobRow> CreatePeriodicJobAsync(
        this IOxbowClient client,
        string name,
        PeriodicSchedule schedule,
        IJobArgs args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default) =>
        Npgsql(client).CreatePeriodicJobAsync(name, schedule, args, options, cancellationToken);

    /// <summary>
    /// Creates a periodic job from a kind and args JSON, for callers without the args type, such as admin tools.
    /// Otherwise the same as <see cref="IOxbowNpgsqlClient.CreatePeriodicJobAsync(string, PeriodicSchedule, IJobArgs, InsertOptions?, CancellationToken)"/>.
    /// </summary>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="name">A unique name, 1 to 255 characters.</param>
    /// <param name="schedule">When it runs. A cron time zone must be one the system can find by ID.</param>
    /// <param name="kind">The job kind.</param>
    /// <param name="args">The args each run is inserted with.</param>
    /// <param name="options">The insert options each run is inserted with. <see cref="InsertOptions.ScheduledAt"/> isn't allowed.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The stored periodic job.</returns>
    /// <exception cref="PeriodicJobExistsException">A periodic job with the name exists.</exception>
    public static Task<PeriodicJobRow> CreatePeriodicJobAsync(
        this IOxbowClient client,
        string name,
        PeriodicSchedule schedule,
        string kind,
        JsonElement args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default) =>
        Npgsql(client).CreatePeriodicJobAsync(name, schedule, kind, args, options, cancellationToken);

    /// <summary>
    /// Creates or replaces a periodic job stored in the database, which the leader enqueues without a restart. Replacing
    /// keeps its run history and pause; a changed schedule starts again from its next occurrence.
    /// </summary>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="name">A unique name, 1 to 255 characters. Can't be the name of a job registered in code.</param>
    /// <param name="schedule">When it runs. A cron time zone must be one the system can find by ID.</param>
    /// <param name="args">The args each run is inserted with.</param>
    /// <param name="options">The insert options each run is inserted with. <see cref="InsertOptions.ScheduledAt"/> isn't allowed.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The stored periodic job.</returns>
    public static Task<PeriodicJobRow> UpsertPeriodicJobAsync(
        this IOxbowClient client,
        string name,
        PeriodicSchedule schedule,
        IJobArgs args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default) =>
        Npgsql(client).UpsertPeriodicJobAsync(name, schedule, args, options, cancellationToken);

    /// <summary>
    /// Creates or replaces a periodic job from a kind and args JSON, for callers without the args type, such as admin
    /// tools. Otherwise the same as <see cref="IOxbowNpgsqlClient.UpsertPeriodicJobAsync(string, PeriodicSchedule, IJobArgs, InsertOptions?, CancellationToken)"/>.
    /// </summary>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="name">A unique name, 1 to 255 characters. Can't be the name of a job registered in code.</param>
    /// <param name="schedule">When it runs. A cron time zone must be one the system can find by ID.</param>
    /// <param name="kind">The job kind.</param>
    /// <param name="args">The args each run is inserted with.</param>
    /// <param name="options">The insert options each run is inserted with. <see cref="InsertOptions.ScheduledAt"/> isn't allowed.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The stored periodic job.</returns>
    public static Task<PeriodicJobRow> UpsertPeriodicJobAsync(
        this IOxbowClient client,
        string name,
        PeriodicSchedule schedule,
        string kind,
        JsonElement args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default) =>
        Npgsql(client).UpsertPeriodicJobAsync(name, schedule, kind, args, options, cancellationToken);

    /// <summary>Pauses a periodic job, stored or registered in code, until it's resumed. The pause persists across restarts.</summary>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="name">The job's name.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The job after the request, or <see langword="null"/> if it doesn't exist.</returns>
    public static Task<PeriodicJobRow?> PausePeriodicJobAsync(
        this IOxbowClient client,
        string name,
        CancellationToken cancellationToken = default) =>
        Npgsql(client).PausePeriodicJobAsync(name, cancellationToken);

    /// <summary>
    /// Resumes a paused periodic job. Runs missed while paused aren't enqueued; the next run is the schedule's next
    /// occurrence.
    /// </summary>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="name">The job's name.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The job after the request, or <see langword="null"/> if it doesn't exist.</returns>
    public static Task<PeriodicJobRow?> ResumePeriodicJobAsync(
        this IOxbowClient client,
        string name,
        CancellationToken cancellationToken = default) =>
        Npgsql(client).ResumePeriodicJobAsync(name, cancellationToken);

    /// <summary>
    /// Deletes a periodic job. For a job still registered in code, this only resets its stored state: the leader
    /// recreates it.
    /// </summary>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="name">The job's name.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The deleted job, or <see langword="null"/> if it doesn't exist.</returns>
    public static Task<PeriodicJobRow?> DeletePeriodicJobAsync(
        this IOxbowClient client,
        string name,
        CancellationToken cancellationToken = default) =>
        Npgsql(client).DeletePeriodicJobAsync(name, cancellationToken);

    /// <summary>Gets a periodic job.</summary>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="name">The job's name.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The job, or <see langword="null"/> if it doesn't exist.</returns>
    public static Task<PeriodicJobRow?> GetPeriodicJobAsync(
        this IOxbowClient client,
        string name,
        CancellationToken cancellationToken = default) =>
        Npgsql(client).GetPeriodicJobAsync(name, cancellationToken);

    /// <summary>Lists periodic jobs, stored or registered in code, ordered by name, one page at a time.</summary>
    /// <param name="client">The Oxbow client, which must implement <see cref="IOxbowNpgsqlClient"/>.</param>
    /// <param name="query">Paging.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A page of jobs and the cursor for the next one.</returns>
    public static Task<PeriodicJobListResult> ListPeriodicJobsAsync(
        this IOxbowClient client,
        PeriodicJobListQuery query,
        CancellationToken cancellationToken = default) =>
        Npgsql(client).ListPeriodicJobsAsync(query, cancellationToken);

    private static IOxbowNpgsqlClient Npgsql(IOxbowClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        return client as IOxbowNpgsqlClient ?? throw new NotSupportedException(
            $"'{client.GetType().FullName}' doesn't implement IOxbowNpgsqlClient, which this method needs.");
    }
}

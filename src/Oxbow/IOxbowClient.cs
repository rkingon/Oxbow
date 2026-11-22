using Npgsql;

namespace Oxbow;

/// <summary>Inserts and manages jobs. Registered as a singleton by <c>AddOxbow</c>.</summary>
public interface IOxbowClient
{
    /// <summary>Inserts a job on a connection of its own.</summary>
    /// <typeparam name="TArgs">The args type.</typeparam>
    /// <param name="args">The job's args.</param>
    /// <param name="options">Insert options.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The insert result.</returns>
    Task<InsertResult> InsertAsync<TArgs>(TArgs args, InsertOptions? options = null, CancellationToken cancellationToken = default)
        where TArgs : IJobArgs;

    /// <summary>
    /// Inserts a job on the caller's connection, inside <paramref name="transaction"/> if given, so the job commits or
    /// rolls back with the caller's data.
    /// </summary>
    /// <typeparam name="TArgs">The args type.</typeparam>
    /// <param name="connection">An open connection.</param>
    /// <param name="transaction">The transaction to insert in, if any.</param>
    /// <param name="args">The job's args.</param>
    /// <param name="options">Insert options.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The insert result.</returns>
    Task<InsertResult> InsertAsync<TArgs>(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        TArgs args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default)
        where TArgs : IJobArgs;

    /// <summary>Inserts many jobs in one statement on a connection of its own.</summary>
    /// <param name="items">The jobs.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The insert results, in order.</returns>
    Task<IReadOnlyList<InsertResult>> InsertManyAsync(IEnumerable<InsertManyItem> items, CancellationToken cancellationToken = default);

    /// <summary>Inserts many jobs in one statement on the caller's connection and transaction.</summary>
    /// <param name="connection">An open connection.</param>
    /// <param name="transaction">The transaction to insert in, if any.</param>
    /// <param name="items">The jobs.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The insert results, in order.</returns>
    Task<IReadOnlyList<InsertResult>> InsertManyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IEnumerable<InsertManyItem> items,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts many jobs with binary COPY on a connection of its own: the fastest path, returning only a count. Unique
    /// jobs aren't supported (COPY can't skip duplicates); use <see cref="InsertManyAsync(IEnumerable{InsertManyItem}, CancellationToken)"/>
    /// for those. Insert middleware runs, with an empty result list.
    /// </summary>
    /// <param name="items">The jobs.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The number of jobs inserted.</returns>
    Task<int> InsertManyFastAsync(IEnumerable<InsertManyItem> items, CancellationToken cancellationToken = default);

    /// <summary>Inserts many jobs with binary COPY on the caller's connection and transaction.</summary>
    /// <param name="connection">An open connection.</param>
    /// <param name="transaction">The transaction to insert in, if any.</param>
    /// <param name="items">The jobs.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The number of jobs inserted.</returns>
    Task<int> InsertManyFastAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IEnumerable<InsertManyItem> items,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Cancels a job. A job that hasn't started is cancelled at once and never runs. A running job's worker is
    /// signalled through its cancellation token, on whichever node runs it, and decides whether to stop.
    /// </summary>
    /// <param name="id">The job's ID.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The job after the request, or <see langword="null"/> if it doesn't exist.</returns>
    Task<JobRow?> CancelAsync(long id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Makes a job available to run now. Running jobs are left alone; a job out of attempts gets one more.
    /// </summary>
    /// <param name="id">The job's ID.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The job after the request, or <see langword="null"/> if it doesn't exist.</returns>
    Task<JobRow?> RetryAsync(long id, CancellationToken cancellationToken = default);

    /// <summary>Deletes a job unless it's running.</summary>
    /// <param name="id">The job's ID.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The job, deleted unless it was running, or <see langword="null"/> if it doesn't exist.</returns>
    Task<JobRow?> DeleteAsync(long id, CancellationToken cancellationToken = default);

    /// <summary>Gets a job.</summary>
    /// <param name="id">The job's ID.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The job, or <see langword="null"/> if it doesn't exist.</returns>
    Task<JobRow?> GetAsync(long id, CancellationToken cancellationToken = default);

    /// <summary>Lists jobs, ordered by ID, one page at a time.</summary>
    /// <param name="query">Filters and paging.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A page of jobs and the cursor for the next one.</returns>
    Task<JobListResult> ListAsync(JobListQuery query, CancellationToken cancellationToken = default);

    /// <summary>Pauses a queue on every node: running jobs finish, no new ones start.</summary>
    /// <param name="queue">The queue, or <c>*</c> for all queues.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when the pause is recorded.</returns>
    Task PauseQueueAsync(string queue, CancellationToken cancellationToken = default);

    /// <summary>Resumes a paused queue on every node.</summary>
    /// <param name="queue">The queue, or <c>*</c> for all queues.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when the resume is recorded.</returns>
    Task ResumeQueueAsync(string queue, CancellationToken cancellationToken = default);
}

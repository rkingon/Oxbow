namespace Oxbow;

/// <summary>
/// Inserts and manages jobs. Registered by <c>AddOxbow</c> as a singleton. Registering an
/// <c>IOxbowTransactionAccessor</c> makes it scoped, and its inserts then join the accessor's current transaction.
/// </summary>
public interface IOxbowClient
{
    /// <summary>
    /// Inserts a job: in the ambient transaction if the scoped client has one, otherwise in a transaction of its own.
    /// </summary>
    /// <typeparam name="TArgs">The args type.</typeparam>
    /// <param name="args">The job's args.</param>
    /// <param name="options">Insert options.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The insert result.</returns>
    Task<InsertResult> InsertAsync<TArgs>(TArgs args, InsertOptions? options = null, CancellationToken cancellationToken = default)
        where TArgs : IJobArgs;

    /// <summary>
    /// Inserts many jobs in one statement: in the ambient transaction if the scoped client has one, otherwise in a
    /// transaction of its own.
    /// </summary>
    /// <param name="items">The jobs.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The insert results, in order.</returns>
    Task<IReadOnlyList<InsertResult>> InsertManyAsync(IEnumerable<InsertManyItem> items, CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts many jobs with binary COPY, in a transaction like <see cref="InsertManyAsync"/>: the fastest path, returning
    /// only a count. Unique jobs aren't supported (COPY can't skip duplicates); use <see cref="InsertManyAsync"/> for
    /// those. Insert middleware runs, with an empty result list.
    /// </summary>
    /// <param name="items">The jobs.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The number of jobs inserted.</returns>
    Task<int> InsertManyFastAsync(IEnumerable<InsertManyItem> items, CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts a batch of jobs, in a transaction like <see cref="InsertManyAsync"/>. The batch and its jobs are written in
    /// one statement, so the batch is never seen without them. Once every job has finished, the leader enqueues the callbacks in
    /// <paramref name="options"/>. An empty batch finishes at once and enqueues both.
    /// </summary>
    /// <param name="items">The member jobs.</param>
    /// <param name="options">The callbacks.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The batch ID and the insert results, in order.</returns>
    Task<BatchInsertResult> InsertBatchAsync(
        IEnumerable<InsertManyItem> items,
        BatchOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>Gets a batch and its progress.</summary>
    /// <param name="id">The batch's ID.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The batch, or <see langword="null"/> if it doesn't exist or has been cleaned up.</returns>
    Task<Batch?> GetBatchAsync(long id, CancellationToken cancellationToken = default);

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

    /// <summary>
    /// Cancels every job matching <paramref name="filter"/>, as <see cref="CancelAsync"/> would one at a time. Jobs are
    /// processed in batches, each committed on its own, so a failure part-way leaves earlier batches cancelled; running
    /// it again is safe. Jobs inserted while it runs may or may not be included.
    /// </summary>
    /// <param name="filter">
    /// The jobs to cancel. A filter that matches everything is rejected unless <see cref="JobFilter.All"/> is set, and so
    /// is a <see cref="JobListQuery"/>, whose paging would suggest only one page is affected.
    /// </param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>How many jobs were cancelled, and how many running ones were asked to stop.</returns>
    Task<CancelManyResult> CancelManyAsync(JobFilter filter, CancellationToken cancellationToken = default);

    /// <summary>
    /// Makes every matching job available to run now, as <see cref="RetryAsync"/> would one at a time, in batches like
    /// <see cref="CancelManyAsync"/>. A job that would take a unique key another live job holds is skipped, where
    /// <see cref="RetryAsync"/> would throw.
    /// </summary>
    /// <param name="filter">The jobs to retry; see <see cref="CancelManyAsync"/>.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>How many jobs were made available, and how many were skipped for their unique key.</returns>
    Task<RetryManyResult> RetryManyAsync(JobFilter filter, CancellationToken cancellationToken = default);

    /// <summary>Deletes every matching job that isn't running, in batches like <see cref="CancelManyAsync"/>.</summary>
    /// <param name="filter">The jobs to delete; see <see cref="CancelManyAsync"/>.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>How many jobs were deleted.</returns>
    Task<long> DeleteManyAsync(JobFilter filter, CancellationToken cancellationToken = default);

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

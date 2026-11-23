using System.Text.Json;
using Npgsql;

namespace Oxbow.Internal;

/// <summary>
/// The <see cref="IOxbowClient"/> registered once a transaction accessor is: inserts join the scope's ambient
/// transaction and run insert middleware from the scope.
/// </summary>
internal sealed class ScopedOxbowClient(OxbowClient client, IServiceProvider services) : IOxbowNpgsqlClient
{
    public Task<InsertResult> InsertAsync<TArgs>(TArgs args, InsertOptions? options = null, CancellationToken cancellationToken = default)
        where TArgs : IJobArgs =>
        client.InsertAsync(args, options, services, cancellationToken);

    public Task<IReadOnlyList<InsertResult>> InsertManyAsync(IEnumerable<InsertManyItem> items, CancellationToken cancellationToken = default) =>
        client.InsertManyAsync(items, services, cancellationToken);

    public Task<int> InsertManyFastAsync(IEnumerable<InsertManyItem> items, CancellationToken cancellationToken = default) =>
        client.InsertManyFastAsync(items, services, cancellationToken);

    public Task<BatchInsertResult> InsertBatchAsync(
        IEnumerable<InsertManyItem> items,
        BatchOptions? options = null,
        CancellationToken cancellationToken = default) =>
        client.InsertBatchAsync(items, options, services, cancellationToken);

    public Task<InsertResult> InsertAsync<TArgs>(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        TArgs args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default)
        where TArgs : IJobArgs =>
        client.InsertAsync(connection, transaction, args, options, services, cancellationToken);

    public Task<IReadOnlyList<InsertResult>> InsertManyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IEnumerable<InsertManyItem> items,
        CancellationToken cancellationToken = default) =>
        client.InsertManyAsync(connection, transaction, items, services, cancellationToken);

    public Task<int> InsertManyFastAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IEnumerable<InsertManyItem> items,
        CancellationToken cancellationToken = default) =>
        client.InsertManyFastAsync(connection, transaction, items, services, cancellationToken);

    public Task<BatchInsertResult> InsertBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        IEnumerable<InsertManyItem> items,
        BatchOptions? options = null,
        CancellationToken cancellationToken = default) =>
        client.InsertBatchAsync(connection, transaction, items, options, services, cancellationToken);

    public Task<Batch?> GetBatchAsync(long id, CancellationToken cancellationToken = default) =>
        client.GetBatchAsync(id, cancellationToken);

    public Task<JobRow?> CancelAsync(long id, CancellationToken cancellationToken = default) =>
        client.CancelAsync(id, cancellationToken);

    public Task<JobRow?> RetryAsync(long id, CancellationToken cancellationToken = default) =>
        client.RetryAsync(id, cancellationToken);

    public Task<JobRow?> DeleteAsync(long id, CancellationToken cancellationToken = default) =>
        client.DeleteAsync(id, cancellationToken);

    public Task<CancelManyResult> CancelManyAsync(JobFilter filter, CancellationToken cancellationToken = default) =>
        client.CancelManyAsync(filter, cancellationToken);

    public Task<RetryManyResult> RetryManyAsync(JobFilter filter, CancellationToken cancellationToken = default) =>
        client.RetryManyAsync(filter, cancellationToken);

    public Task<long> DeleteManyAsync(JobFilter filter, CancellationToken cancellationToken = default) =>
        client.DeleteManyAsync(filter, cancellationToken);

    public Task<JobRow?> GetAsync(long id, CancellationToken cancellationToken = default) =>
        client.GetAsync(id, cancellationToken);

    public Task<JobListResult> ListAsync(JobListQuery query, CancellationToken cancellationToken = default) =>
        client.ListAsync(query, cancellationToken);

    public Task PauseQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        client.PauseQueueAsync(queue, cancellationToken);

    public Task ResumeQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        client.ResumeQueueAsync(queue, cancellationToken);

    public Task<PeriodicJobRow> CreatePeriodicJobAsync(
        string name,
        PeriodicSchedule schedule,
        IJobArgs args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default) =>
        client.CreatePeriodicJobAsync(name, schedule, args, options, cancellationToken);

    public Task<PeriodicJobRow> CreatePeriodicJobAsync(
        string name,
        PeriodicSchedule schedule,
        string kind,
        JsonElement args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default) =>
        client.CreatePeriodicJobAsync(name, schedule, kind, args, options, cancellationToken);

    public Task<PeriodicJobRow> UpsertPeriodicJobAsync(
        string name,
        PeriodicSchedule schedule,
        IJobArgs args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default) =>
        client.UpsertPeriodicJobAsync(name, schedule, args, options, cancellationToken);

    public Task<PeriodicJobRow> UpsertPeriodicJobAsync(
        string name,
        PeriodicSchedule schedule,
        string kind,
        JsonElement args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default) =>
        client.UpsertPeriodicJobAsync(name, schedule, kind, args, options, cancellationToken);

    public Task<PeriodicJobRow?> PausePeriodicJobAsync(string name, CancellationToken cancellationToken = default) =>
        client.PausePeriodicJobAsync(name, cancellationToken);

    public Task<PeriodicJobRow?> ResumePeriodicJobAsync(string name, CancellationToken cancellationToken = default) =>
        client.ResumePeriodicJobAsync(name, cancellationToken);

    public Task<PeriodicJobRow?> DeletePeriodicJobAsync(string name, CancellationToken cancellationToken = default) =>
        client.DeletePeriodicJobAsync(name, cancellationToken);

    public Task<PeriodicJobRow?> GetPeriodicJobAsync(string name, CancellationToken cancellationToken = default) =>
        client.GetPeriodicJobAsync(name, cancellationToken);

    public Task<PeriodicJobListResult> ListPeriodicJobsAsync(PeriodicJobListQuery query, CancellationToken cancellationToken = default) =>
        client.ListPeriodicJobsAsync(query, cancellationToken);
}

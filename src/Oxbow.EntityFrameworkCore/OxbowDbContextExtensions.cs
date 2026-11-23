using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;

namespace Oxbow;

/// <summary>Inserts jobs in a <see cref="DbContext"/>'s current transaction.</summary>
public static class OxbowDbContextExtensions
{
    /// <summary>
    /// Inserts a job in <paramref name="dbContext"/>'s current transaction, so it commits or rolls back with the
    /// context's changes.
    /// </summary>
    /// <typeparam name="TArgs">The args type.</typeparam>
    /// <param name="client">The Oxbow client.</param>
    /// <param name="dbContext">A context using Npgsql, with a transaction begun.</param>
    /// <param name="args">The job's args.</param>
    /// <param name="options">Insert options.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The insert result.</returns>
    /// <exception cref="InvalidOperationException">The context has no current transaction.</exception>
    public static Task<InsertResult> InsertAsync<TArgs>(
        this IOxbowClient client,
        DbContext dbContext,
        TArgs args,
        InsertOptions? options = null,
        CancellationToken cancellationToken = default)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(client);
        var (connection, transaction) = Resolve(dbContext);
        return client.InsertAsync(connection, transaction, args, options, cancellationToken);
    }

    /// <summary>Inserts many jobs in <paramref name="dbContext"/>'s current transaction.</summary>
    /// <param name="client">The Oxbow client.</param>
    /// <param name="dbContext">A context using Npgsql, with a transaction begun.</param>
    /// <param name="items">The jobs.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The insert results, in order.</returns>
    /// <exception cref="InvalidOperationException">The context has no current transaction.</exception>
    public static Task<IReadOnlyList<InsertResult>> InsertManyAsync(
        this IOxbowClient client,
        DbContext dbContext,
        IEnumerable<InsertManyItem> items,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        var (connection, transaction) = Resolve(dbContext);
        return client.InsertManyAsync(connection, transaction, items, cancellationToken);
    }

    /// <summary>Inserts a batch of jobs in <paramref name="dbContext"/>'s current transaction.</summary>
    /// <param name="client">The Oxbow client.</param>
    /// <param name="dbContext">A context using Npgsql, with a transaction begun.</param>
    /// <param name="items">The member jobs.</param>
    /// <param name="options">The callbacks.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The batch ID and the insert results, in order.</returns>
    /// <exception cref="InvalidOperationException">The context has no current transaction.</exception>
    public static Task<BatchInsertResult> InsertBatchAsync(
        this IOxbowClient client,
        DbContext dbContext,
        IEnumerable<InsertManyItem> items,
        BatchOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        var (connection, transaction) = Resolve(dbContext);
        return client.InsertBatchAsync(connection, transaction, items, options, cancellationToken);
    }

    private static (NpgsqlConnection Connection, NpgsqlTransaction Transaction) Resolve(DbContext dbContext)
    {
        ArgumentNullException.ThrowIfNull(dbContext);
        var current = dbContext.Database.CurrentTransaction
            ?? throw new InvalidOperationException(
                "The DbContext has no current transaction, so the job would commit on its own, ahead of SaveChanges. " +
                "Call dbContext.Database.BeginTransactionAsync() first, or use IOxbowClient.InsertAsync without a " +
                "DbContext if you meant to commit the job immediately.");

        if (dbContext.Database.GetDbConnection() is not NpgsqlConnection connection
            || current.GetDbTransaction() is not NpgsqlTransaction transaction)
        {
            throw new InvalidOperationException("Oxbow needs a DbContext configured with Npgsql (UseNpgsql).");
        }

        return (connection, transaction);
    }
}

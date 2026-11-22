# Transactional enqueue

Insert a job on the same connection and transaction as the data it belongs to. If the transaction rolls back, the job
never existed; if it commits, the job is worked. No outbox, no dual write.

```csharp
await using var connection = await dataSource.OpenConnectionAsync();
await using var transaction = await connection.BeginTransactionAsync();

await using (var command = new NpgsqlCommand("INSERT INTO orders (id, total) VALUES (@id, @total)", connection, transaction))
{
    command.Parameters.AddWithValue("id", orderId);
    command.Parameters.AddWithValue("total", total);
    await command.ExecuteNonQueryAsync();
}

await oxbow.InsertAsync(connection, transaction, new ChargeOrder(orderId));
await transaction.CommitAsync();
```

- Workers can't see the job until the transaction commits.
- The wake-up NOTIFY is part of the transaction too, so a rolled-back insert wakes nobody.
- Oxbow runs its SQL on your connection without changing its role, settings, `search_path` or row-level security
  context.
- `InsertManyAsync(connection, transaction, items)` inserts a batch in one statement, with the same guarantees.
- Pass `transaction: null` to insert on your connection in autocommit mode.

## Dapper

Dapper uses the same `NpgsqlConnection` and `NpgsqlTransaction`, so the overload above is all you need.

## EF Core

With `Oxbow.EntityFrameworkCore`:

```csharp
await using var transaction = await db.Database.BeginTransactionAsync();
db.Orders.Add(order);
await db.SaveChangesAsync();
await oxbow.InsertAsync(db, new ChargeOrder(order.Id));
await transaction.CommitAsync();
```

`InsertAsync(db, …)` throws `InvalidOperationException` when the context has no current transaction, because the job
would otherwise commit on its own, ahead of `SaveChanges`. To commit a job immediately, use `IOxbowClient.InsertAsync`
without a `DbContext`.

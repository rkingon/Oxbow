# Abstractions and layering

`Oxbow.Abstractions` holds Oxbow's contracts with no dependencies beyond the BCL: no Npgsql, no hosting, no runtime.
Domain and application projects reference it to own their jobs, and only the host references `Oxbow`.

| Project | References | Holds |
|---|---|---|
| Application | `Oxbow.Abstractions` | Job args, workers, use cases that enqueue through `IOxbowClient` |
| Infrastructure / host | `Oxbow`, `Oxbow.EntityFrameworkCore` | `AddOxbow`, worker registration, the transaction accessor, periodic jobs |

## What's in it

- Job definition: `IJobArgs`, `[JobKind]`, `[UniqueKey]`, `[PartitionKey]`, `[SequenceKey]`, `[EncryptArgs]`
- Workers: `IWorker<TArgs>`, `Job<TArgs>`, `IJobRuntime`, `JobRetryException`, `JobSnoozeException`,
  `JobCancelException`, `JobAttemptSupersededException`, `IJobWorkMiddleware`, `JobContext`, `IJobDiscardHandler`
- Inserting: `IOxbowClient`, `InsertOptions`, `UniqueOptions`, `InsertManyItem`, `InsertResult`, `BatchOptions`
- Reading: `JobRow`, `JobState`, `JobFilter`, `JobListQuery`, `Batch`

The types keep the `Oxbow` namespace, and `Oxbow` forwards them, so code that references `Oxbow` compiles unchanged.
Code that implements or mocks `IOxbowClient`, and assemblies compiled against 0.2.0-alpha.1 that call its connection or
periodic-job members, need updating or recompiling: those members moved to `IOxbowNpgsqlClient`.

`IOxbowClient` has every member that needs neither Npgsql nor the runtime: inserts, reads, cancel/retry/delete and their
bulk forms, and queue pause/resume. The rest is on `IOxbowNpgsqlClient : IOxbowClient` in `Oxbow`: inserts on your
own connection and transaction, and periodic jobs. Extension methods make them callable on `IOxbowClient` too.

## Enqueue in the unit of work

Application code calls `InsertAsync(args)` and doesn't see a connection. To keep Oxbow's guarantee, that the job commits
or rolls back with your data, the host registers an `IOxbowTransactionAccessor`:

```csharp
builder.Services.AddDbContext<AppDbContext>(o => o.UseNpgsql(connectionString));
builder.Services.AddOxbow(o => o.RequireAmbientTransaction = true)
    .AddWorker<ShipOrder, ShipOrderWorker>()
    .AddDbContextTransaction<AppDbContext>(); // Oxbow.EntityFrameworkCore
```

```csharp
// Application project: references Oxbow.Abstractions only.
public sealed class PlaceOrder(AppDbContext db, IOxbowClient oxbow)
{
    public async Task ExecuteAsync(Order order)
    {
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Orders.Add(order);
        await db.SaveChangesAsync();
        await oxbow.InsertAsync(new ShipOrder(order.Id)); // joins the transaction
        await transaction.CommitAsync();
    }
}
```

With an accessor registered:

- `IOxbowClient` is **scoped**. `InsertAsync`, `InsertManyAsync`, `InsertManyFastAsync` and `InsertBatchAsync` insert
  in the accessor's current transaction.
- With no current transaction they use one of their own, or throw if `RequireAmbientTransaction` is set. Setting it
  without an accessor fails on start.
- Singletons and hosted services can't take the scoped `IOxbowClient`. Give them `IOxbowNpgsqlClient`, which stays a
  singleton and never joins an ambient transaction, or create a scope.
- Inserts given a connection, and Oxbow's own periodic runs, never use the accessor.
- Reads such as `GetAsync` use a connection of their own, so they don't see a job until its transaction commits.

For something other than EF, implement `IOxbowTransactionAccessor` (scoped, returning your unit of work's open
`NpgsqlConnection` and `NpgsqlTransaction`, or `null`) and register it with `AddTransactionAccessor<T>()`.

## Scoped insert middleware

Insert middleware that depends on scoped services, such as a tenant context, registers with a lifetime:

```csharp
builder.Services.AddOxbow().AddInsertMiddleware<StampTenant>(ServiceLifetime.Scoped);
```

Through the scoped `IOxbowClient` it's resolved from the inserting scope. Elsewhere (`IOxbowNpgsqlClient`, the leader's
periodic runs) it's resolved from a scope of its own, where that scoped state is empty, so handle that case.

## Testing

- `RecordingOxbowClient` in `Oxbow.Testing` is an in-memory `IOxbowClient`: unit test a use case and assert the kind,
  args, options and metadata it enqueued. See [testing](testing.md#unit-test-code-that-enqueues).
- `WorkerHarness` runs workers written against the abstractions.
- `new Job<TArgs>(row, args, new RecordingJobRuntime())` records a worker's heartbeats and progress.

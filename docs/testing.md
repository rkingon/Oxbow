# Testing

`Oxbow.Testing` has helpers for asserting inserts, unit testing code that enqueues, and running workers.

## Assert inserts inside the test's transaction

```csharp
await using var connection = await dataSource.OpenConnectionAsync();
await using var transaction = await connection.BeginTransactionAsync();

await signup.RegisterAsync(connection, transaction, "ada@example.com");

var assert = new OxbowAssert();
var job = await assert.InsertedAsync<SendWelcomeEmail>(connection, transaction,
    new() { Args = a => a.Email == "ada@example.com", Queue = "email" });
await assert.NotInsertedAsync<ChargeCard>(connection, transaction);

await transaction.RollbackAsync(); // nothing persists
```

Failures throw `OxbowAssertionException`, so it works with any test framework. For kinds with
[encrypted args](encryption.md#testing), set `ArgsProtector`.

## Unit test code that enqueues

`RecordingOxbowClient` is an in-memory `IOxbowClient`. Pass it to code that enqueues, then assert what it stored:

```csharp
var oxbow = new RecordingOxbowClient(insertMiddleware: [new StampTenant(tenant)]);
await new PlaceOrder(oxbow).ExecuteAsync(orderId);

var job = Assert.Single(oxbow.Inserted<ShipOrder>());   // Job<ShipOrder>: typed args and the stored row
Assert.Equal(orderId, job.Args.OrderId);
Assert.Equal("acme", job.Metadata.GetProperty("tenant").GetString());
Assert.Equal("shipping", oxbow.Jobs[0].Options?.Queue);   // options in effect, args defaults included
```

Inserts run the given insert middleware and are stored as Oxbow would store them. Reads, cancel, retry, delete and
their bulk forms act on what's stored. Nothing is worked, and there are no transactions, unique keys or sequence
ordering. Keys derived from `[UniqueKey]`, `[PartitionKey]` and `[SequenceKey]` aren't computed. Cover those, and
rollback, with integration tests.

## Run a worker without a database

```csharp
var harness = new WorkerHarness<SendWelcomeEmail, SendWelcomeEmailWorker>(serviceProvider);
var result = await harness.WorkAsync(new SendWelcomeEmail("ada@example.com"));

Assert.Equal(WorkOutcome.Completed, result.Outcome);
Assert.Equal("sent", result.Output?.GetProperty("status").GetString());  // what job.SetOutput recorded
```

The harness opens a DI scope, runs your registered work middleware, applies the worker's timeout, and reports what
Oxbow would record: `Completed`, `Errored`, `Snoozed`, `Cancelled` or `TimedOut`. A worker that throws
`JobRetryException` is `Errored`, with its delay in `result.RetryDelay`. `result.Progress` is the progress the worker
last reported; heartbeats do nothing.

To call a worker directly instead, create the job with a `RecordingJobRuntime` to see its heartbeats and every progress
report:

```csharp
var runtime = new RecordingJobRuntime();
await worker.WorkAsync(new Job<Import>(row, new Import(10), runtime), CancellationToken.None);
Assert.Equal([50.0, 100.0], runtime.Progress.Select(p => p.Percent));
```

For full integration tests, run Postgres in a container (Testcontainers), apply `OxbowSchema.GetMigrations(0)`, and use a
unique `TablePrefix` per test to isolate them, as Oxbow's own suite does.

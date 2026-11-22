# Testing

`Oxbow.Testing` has two helpers.

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

Failures throw `OxbowAssertionException`, so it works with any test framework.

## Run a worker without a database

```csharp
var harness = new WorkerHarness<SendWelcomeEmail, SendWelcomeEmailWorker>(serviceProvider);
var result = await harness.WorkAsync(new SendWelcomeEmail("ada@example.com"));

Assert.Equal(WorkOutcome.Completed, result.Outcome);
```

The harness opens a DI scope, runs your registered work middleware, applies the worker's timeout, and reports what
Oxbow would record: `Completed`, `Errored`, `Snoozed`, `Cancelled` or `TimedOut`.

For full integration tests, run Postgres in a container (Testcontainers), apply `OxbowSchema.GetMigrations(0)`, and use a
unique `TablePrefix` per test to isolate them, as Oxbow's own suite does.

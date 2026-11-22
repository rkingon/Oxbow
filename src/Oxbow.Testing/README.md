# Oxbow.Testing

Test helpers for [Oxbow](https://github.com/rkingon/Oxbow).

## Assert a job was inserted, inside the test's transaction

```csharp
var assert = new OxbowAssert();
var job = await assert.InsertedAsync<SendEmail>(connection, transaction, new() { Args = a => a.To == "ada@example.com" });
await assert.NotInsertedAsync<ChargeCard>(connection, transaction);
```

## Run a worker inline, without a database

```csharp
var harness = new WorkerHarness<SendEmail, SendEmailWorker>(serviceProvider);
var result = await harness.WorkAsync(new SendEmail("ada@example.com", "Hi"));
Assert.Equal(WorkOutcome.Completed, result.Outcome);
```

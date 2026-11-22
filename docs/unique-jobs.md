# Unique jobs

```csharp
await oxbow.InsertAsync(new SyncAccount(accountId), new InsertOptions
{
    Unique = new UniqueOptions { ByArgs = true, ByPeriod = TimeSpan.FromMinutes(15) },
});
```

If a job with the same unique key exists in one of the unique states, the insert returns that job with
`UniqueSkippedAsDuplicate = true` instead of inserting. Uniqueness is enforced by a partial unique index, so it holds
across processes and transactions: a concurrent insert of the same key waits for the first transaction to commit or
roll back.

The key is a SHA-256 of:

| Option | Adds |
|---|---|
| (always, unless `ExcludeKind`) | the kind |
| `ByArgs` | the args: the properties marked `[UniqueKey]`, or the whole args object if none are, keys sorted |
| `ByPeriod` | the start of the period containing the job's `ScheduledAt` (or now) |
| `ByQueue` | the queue |

`ByState` sets which states block a duplicate. The default is every state except `cancelled` and `discarded`, so a
cancelled or discarded job doesn't stop a new one. A custom set must include `available`, `pending`, `running` and
`scheduled`.

```csharp
[JobKind("charge")]
public sealed record Charge([property: UniqueKey] string InvoiceId, string TraceId) : IJobArgs
{
    public InsertOptions GetInsertOptions() => new() { Unique = new() { ByArgs = true } };
}
```

On positional records, write `[property: UniqueKey]` so the attribute lands on the property. Nested objects work too:
mark the nested properties that matter.

When the scheduler promotes several retryable or scheduled jobs that share a key, only one becomes available; the rest
are discarded with `metadata.unique_key_conflict`.

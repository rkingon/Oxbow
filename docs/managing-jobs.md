# Managing jobs

`IOxbowClient` acts on one job by ID or on every job matching a `JobFilter`.

| One job | Many jobs |
|---|---|
| `GetAsync(id)` | `ListAsync(JobListQuery)` |
| `CancelAsync(id)` | `CancelManyAsync(JobFilter)` |
| `RetryAsync(id)` | `RetryManyAsync(JobFilter)` |
| `DeleteAsync(id)` | `DeleteManyAsync(JobFilter)` |

## Filters

```csharp
var acme = new JobFilter
{
    Metadata = new JsonObject { ["tenant"] = "acme" },
    States = [JobState.Available, JobState.Scheduled, JobState.Retryable, JobState.Running],
};
```

| Filter | Matches |
|---|---|
| `States`, `Queues`, `Kinds` | Any of the values. |
| `BatchId` | Members of a [batch](batches.md). |
| `Metadata` | Jobs whose metadata contains the object (`@>`): `{"tenant": "acme"}` matches any job with that key and value, and nested objects match the same way. Served by the metadata GIN index. |
| `Tags` | Jobs carrying all of the tags. |

Filters combine with AND, and unset ones match everything. `JobListQuery` is a `JobFilter` with paging (`Limit`,
`Cursor`, `Descending`). Bulk operations reject a `JobListQuery`: they act on every match, not a page, so "delete what I
see" must not quietly delete everything. Pass a `JobFilter`.

## Bulk operations

```csharp
var cancel = await oxbow.CancelManyAsync(acme);   // (Cancelled, CancelRequested)
var retry = await oxbow.RetryManyAsync(new JobFilter { States = [JobState.Discarded], Kinds = ["send_invoice"] });
var deleted = await oxbow.DeleteManyAsync(new JobFilter { Tags = ["import-2026-11"] });
```

Each job is treated as the single-job method would treat it: see [cancellation](cancellation.md); retry never touches a
running job and gives a job out of attempts one more; delete never touches a running job. One difference: a retry that
would take a unique key another live job holds skips that job, where `RetryAsync` throws, and counts it in
`RetryManyResult.SkippedUniqueConflicts`.

- **Guard.** A filter that matches every job is rejected with `ArgumentException`. Set `All = true` to mean it.
- **Batches.** Jobs are processed 1,000 at a time in ID order, one statement per batch, each committed on its own. No
  transaction holds locks on the whole set, and producers' fetches skip a batch's locked rows rather than wait. If the
  operation fails or is cancelled part-way, earlier batches stay done; running it again is safe.
- **Concurrent inserts.** An operation only considers jobs up to the highest ID when it started, so a stream of
  matching inserts can't keep it running. Jobs inserted while it runs may or may not be included.
- **Results.** Counts, not rows, since a set can be millions of jobs: `CancelManyResult`, `RetryManyResult` and a
  `long` for deletes. List with the same filters to see the jobs.

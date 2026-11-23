# Batches

A batch is a group of jobs with callback jobs that run when the group finishes: fan out an import, then fan in to
report on it.

```csharp
var batch = await oxbow.InsertBatchAsync(connection, transaction,
    rows.Select(r => new InsertManyItem(new ImportRow(importId, r.Id))),
    new BatchOptions
    {
        OnComplete = new BatchCallback(new ImportFinished(importId)),   // every job finished, any outcome
        OnSuccess = new BatchCallback(new PublishImport(importId)),     // every job completed successfully
    });

await transaction.CommitAsync();
```

`InsertBatchAsync` writes the batch row and all its jobs in **one statement**, on your connection and transaction like
`InsertManyAsync` (or on its own connection, or a `DbContext`'s transaction with `Oxbow.EntityFrameworkCore`). A
rolled-back transaction leaves neither. There's no "create a batch, then add jobs to it" API: a batch that exists
before its jobs do looks finished to the leader, and sealing it explicitly would be one more thing to forget. Insert
everything at once; a batch of thousands of jobs is still one statement.

## Callbacks

Once no member is `available`, `scheduled`, `retryable`, `running` or `pending`, the leader's batch finisher (every
second) inserts the callbacks and marks the batch finished in the same statement, under a row lock on the batch. A
leader failover, or two leaders briefly overlapping, can't enqueue a callback twice.

- `OnComplete` is enqueued whatever the outcome.
- `OnSuccess` is enqueued only if every member `completed`: none `discarded`, `cancelled` or deleted.
- A callback is an ordinary job. It's prepared when the batch is inserted: defaults are resolved, it goes through insert
  middleware there (with an empty result list, as with `InsertManyFastAsync`), so tenant and trace metadata reach it.
  Middleware therefore sees a callback that may never fire, such as `OnSuccess` of a batch that fails. Each callback
  runs through the pipeline on its own; middleware that doesn't call `next` drops it. Middleware that doesn't call
  `next` for the members makes `InsertBatchAsync` throw, since a batch without its jobs isn't inserted. Its unique key
  is computed then too.
  The leader inserts it as prepared, so the leader needn't have its args type. `ScheduledAt` is an absolute time.
- `OnComplete` and `OnSuccess` with the same unique key are rejected, since only one could ever be inserted. A callback
  that's skipped as a unique duplicate links the existing job, whose metadata (and so `BatchId`) still names the
  batch that inserted it.
- The callback's metadata carries `oxbow:batch_id` and `oxbow:batch_callback` (`complete` or `success`):

```csharp
public sealed class ImportFinishedWorker(IOxbowClient oxbow) : IWorker<ImportFinished>
{
    public async Task WorkAsync(Job<ImportFinished> job, CancellationToken cancellationToken)
    {
        var batch = await oxbow.GetBatchAsync(job.BatchId!.Value, cancellationToken);
        // batch.Completed, batch.Discarded, batch.Cancelled ...
        var failed = await oxbow.ListAsync(new JobListQuery { BatchId = batch!.Id, States = [JobState.Discarded] }, cancellationToken);
    }
}
```

## Progress

`GetBatchAsync(id)` returns `Total`, `Completed`, `Discarded`, `Cancelled` and `Remaining`, counted from the members
with one aggregate query until the batch finishes, then frozen on the batch row. Members carry `oxbow:batch_id` in
their metadata too (`JobRow.BatchId`), and `JobListQuery.BatchId` lists them.

Counters aren't kept on the batch row as jobs finish: the completer writes thousands of outcomes per statement, and
one row updated by every write would serialize them.

## Membership

| Case | Behavior |
|---|---|
| Empty batch | Finishes on the finisher's next pass; `OnComplete` and `OnSuccess` both fire. |
| Unique job skipped as a duplicate | Not a member and not counted. The existing job keeps its own batch, if any. |
| Member cancelled | Finished, counted in `Cancelled`; `OnSuccess` won't fire. |
| Member deleted before the batch finishes | Counted in `Cancelled`. |
| Member retried after the batch finished | Runs again, but the batch stays finished and its counts stay frozen. |
| `InsertManyFastAsync` | Not supported for batches: COPY can't share a statement with the batch row. |

## Retention

The job cleaner leaves a batch's members alone while the batch row exists, so a member that completed on day one isn't
deleted while a sibling spends three weeks retrying, and progress and `OnSuccess` stay right. A finished batch is
deleted after `OxbowOptions.BatchRetention` (24 hours; `Timeout.InfiniteTimeSpan` keeps it), but not while one of its
callbacks is still unfinished, so a retrying callback can always read its batch. Its members are then cleaned by their
own retention. In effect, a member is kept for the batch's lifetime plus `BatchRetention`, rather than its own
retention from when it finished.

## Upgrading

Batches need schema version 2. A node on an older release ignores batches: as leader it finishes none (they wait for a
newer leader) and its cleaner deletes members by their own retention, which can make progress undercount and keep
`OnSuccess` from firing. Upgrade every node before using batches.

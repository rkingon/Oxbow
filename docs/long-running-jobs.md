# Long-running jobs

A job that runs for minutes or hours (an agent's turn, a backfill, a vendor export) needs three things a short job
doesn't: a timeout that fits, a way to show it's alive, and a way to pick up where it left off when an attempt ends
early. Oxbow has one for each, and a fourth for writes that must not outlive the attempt that made them.

```csharp
public sealed class ExportWorker(IVendor vendor, AppDb db) : IWorker<Export>
{
    public async Task WorkAsync(Job<Export> job, CancellationToken cancellationToken)
    {
        var exportId = await job.StepAsync("start", ct => vendor.StartExportAsync(job.Args.AccountId, ct), cancellationToken);
        var file = await job.StepAsync("download", ct => vendor.DownloadAsync(exportId, ct), cancellationToken);

        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        await job.LockAttemptAsync(transaction.GetDbTransaction(), cancellationToken);
        // Keyed by the export, so a repeat after a crash between this commit and the job's completion is a no-op.
        if (!await db.Imports.AnyAsync(i => i.ExportId == exportId, cancellationToken))
        {
            db.Imports.Add(Import.From(exportId, file));
            await db.SaveChangesAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public TimeSpan? GetTimeout(Job<Export> job) => Timeout.InfiniteTimeSpan;
}
```

## Timeout

`OxbowOptions.JobTimeout` is 1 minute. A longer job overrides `GetTimeout`, with a bound or with
`Timeout.InfiniteTimeSpan`; otherwise its token fires at the timeout, the attempt fails, and it starts again from the
top. See [workers](workers.md#timeouts).

## Heartbeat

A job with no timeout is judged by the rescuer on `attempted_at` alone, so after `RescueStuckJobsAfter` (1 h) it looks
dead. Heartbeat with `job.HeartbeatAsync()`, or `ReportProgressAsync`, and the rescuer judges the attempt by its last
heartbeat instead. Every step heartbeats as it starts, but a single step that runs longer than `RescueStuckJobsAfter`
must heartbeat inside its delegate too. See [heartbeat and progress](workers.md#heartbeat-and-progress).

## Steps

`job.StepAsync(name, run)` records a step's result under its name. A later execution of the job (after a failed
attempt, a snooze, a rescue, a shutdown or `RetryAsync`) gets the recorded result back without running the step again,
so a worker written from the top resumes where it was.

```csharp
var reply = await job.StepAsync("ask", ct => model.CompleteAsync(prompt, ct), cancellationToken);
await job.StepAsync("notify", ct => chat.PostAsync(reply, ct), cancellationToken);   // no result: records null
```

- **At least once.** A step's result is recorded after it runs, so an execution that dies in between runs it again.
  Make the step idempotent, or give the vendor an idempotency key, if a repeat matters.
- **Names identify results.** A name is at most 255 characters and runs once per execution; using it again in the
  same execution throws, unless its step failed. Renaming a step, or changing what a name returns, makes the next
  execution run it again or read a result of the old shape. Use names that say what the step does, not a counter.
- **Results** are serialized as the declared type with `OxbowOptions.JsonSerializerOptions` (register the type in your
  `JsonSerializerContext` if you use one), at most `JobRow.MaxOutputSize` (1 MiB) of JSON; a larger one fails the step.
  Store large results elsewhere and step on a reference.
- **A failed step records nothing.** Its exception fails the attempt like any other, and the retry runs the step again.
- **Encrypted kinds** have their step results encrypted too, decided by whether the job's args are, under a purpose apart
  from args. See [encryption](encryption.md#step-results).
- **Kept with the job.** Results are stored in `{prefix}job_step` and deleted with their job, by the cleaner,
  `DeleteAsync` or `DeleteManyAsync`. `RetryAsync` keeps them: a retried job resumes.
- **The first result recorded wins.** A step that finds a result recorded while it ran returns that one instead of its
  own, so every execution agrees with what was recorded.

`Snooze` and steps compose into a durable wait: record what you're waiting for, snooze, and the next execution
replays to the same point.

```csharp
var exportId = await job.StepAsync("start", ct => vendor.StartExportAsync(job.Args.AccountId, ct), cancellationToken);
if (!await vendor.IsReadyAsync(exportId, cancellationToken))
{
    throw new JobSnoozeException(TimeSpan.FromMinutes(1));   // not an attempt; replays "start" next time
}
```

## Superseded attempts

An attempt that runs past its timeout or heartbeat horizon can be rescued while its worker is still going: the job is
retried, and two executions overlap. Oxbow keeps the stale one from changing the job: its heartbeats, progress and
outcome are ignored, and its steps throw `JobAttemptSupersededException`. Let it propagate.

- A step called once the attempt is known to be superseded throws before running, and doesn't replay a recorded result.
- A step already running when the job was rescued records its result if none is recorded yet, since its effect
  happened, and then throws. If the current attempt recorded one first, the stale result is dropped.

Writes of your own need the same guard. `job.LockAttemptAsync(transaction)` locks the job's row in your transaction
while this attempt is the one running:

- While the lock is held, the rescuer can't take the job, so what the transaction writes commits only as the current
  attempt's. It's a `FOR SHARE` lock: several of the attempt's own transactions can hold it at once.
- **If the attempt is superseded, the lock fails the transaction** and throws `JobAttemptSupersededException`. Postgres
  won't commit a failed transaction, so nothing written in it is kept, even if the exception is caught. Roll it back and
  let the exception propagate.
- **Lock first.** Under `REPEATABLE READ` or `SERIALIZABLE`, a write to the job's row after the transaction's snapshot
  (a heartbeat, progress) makes the lock fail with a serialization failure (`40001`), even though the attempt is
  current. Lock before anything else in the transaction, and retry the whole transaction on `40001`. `READ COMMITTED`
  has neither problem.
- **Commit before the worker returns.** A worker that returns with the row still locked holds its own outcome, and its
  worker slot, until the transaction ends, with a warning in the log.
- While the lock is held, the rescuer leaves the job for a later pass, `CancelAsync` on it waits for the transaction,
  and the attempt's heartbeats and progress are skipped rather than waiting on a transaction the worker is inside.
- The transaction must be an `NpgsqlTransaction` on the database Oxbow uses. With EF Core,
  `transaction.GetDbTransaction()`.

## Testing

Create the job with a `RecordingJobRuntime`, and seed it to test a replay:

```csharp
var runtime = new RecordingJobRuntime();
runtime.RecordStep("start", "export-1");
await worker.WorkAsync(new Job<Export>(row, new Export(42), runtime), CancellationToken.None);
Assert.Equal("export-1", runtime.Steps["start"].GetString());
Assert.Equal(1, runtime.AttemptLocks);
```

Without a runtime (`WorkerHarness`, or a `Job` created without one), steps run every time and `LockAttemptAsync`
does nothing. A custom `IJobRuntime` written before steps throws `NotSupportedException` from both, rather than
appearing to record or lock.

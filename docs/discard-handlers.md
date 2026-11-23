# Discard handlers

A discard handler is called after Oxbow gives up on a job: it's `discarded` or `cancelled`. Use it to alert, compensate,
or move the job somewhere a human will look.

```csharp
public sealed class AlertOnDiscard(IAlerts alerts) : IJobDiscardHandler
{
    public Task HandleAsync(JobDiscardContext context, CancellationToken cancellationToken) =>
        alerts.SendAsync($"Job {context.Job.Id} ({context.Job.Kind}) {context.State}: {context.Reason}", cancellationToken);
}

services.AddOxbow(...).AddDiscardHandler<AlertOnDiscard>();
```

| `Reason` | `State` | When |
|---|---|---|
| `AttemptsExhausted` | `discarded` | The last attempt failed, or got stuck and was rescued. |
| `WorkerCancelled` | `cancelled` | The worker threw `JobCancelException`. |
| `CancelRequested` | `cancelled` | `CancelAsync` was called while the job was running: the worker stopped, or its node died and the rescuer cancelled it. |
| `UniqueKeyConflict` | `discarded` | The job became due while another job held its unique key (`metadata.unique_key_conflict`). |

- **After the write.** The call comes after the outcome is in the database, with the state actually written. A retry
  that a cancel request turned into a cancellation is reported as `CancelRequested`, which middleware, seeing only the
  worker's exception, can't know.
- **Discards without a worker.** The rescuer's and the scheduler's discards and cancellations run on the leader, so
  their handlers do too.
- `context.Job` is read after the write, with the error history, metadata and `finalized_at` as stored. `Exception`
  is the worker's exception when the outcome came from this node's worker, and `null` otherwise.
- `CancelAsync` on a job that isn't running cancels it on the spot and returns the row; no handler is called, because
  the caller already knows.
- Each job gets a new DI scope. Handlers run in registration order; an exception is logged and the next handler still
  runs.
- Handlers run on one background loop, never on a worker slot or a maintenance loop, so a slow handler delays other
  handlers but not jobs. Hand long work off (insert a job) rather than doing it inline.

## Best-effort

Calls are queued in memory on the node that wrote the outcome. They're lost if:

- the node dies, or shutdown's grace runs out: after the completer drains, Oxbow waits up to 5 seconds for queued
  calls, then cancels the handler's token and drops the rest, with a warning;
- the node can't read the jobs back (the database is down), or a job was deleted first;
- more than 10,000 calls are waiting.

In rare races a call can also be made twice (a worker reporting a snooze after the rescuer already cancelled its job),
so make handlers idempotent. For a guarantee, query: `ListAsync` with `States = [Discarded, Cancelled]`.

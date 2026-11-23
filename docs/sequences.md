# Sequences

Jobs with the same sequence key run one at a time, in insert order: a key's next job starts only after the previous
one finishes. Different keys run in parallel. Use them for work that must not overlap or reorder per entity, such as
webhook delivery per endpoint or event processing per aggregate.

```csharp
[JobKind("deliver_webhook")]
public sealed record DeliverWebhook([property: SequenceKey] string EndpointId, string EventId) : IJobArgs;

await oxbow.InsertAsync(connection, transaction, new DeliverWebhook(endpointId, eventId));
```

The key is the kind plus the values of the properties marked `[SequenceKey]` (top-level only), e.g.
`deliver_webhook:{"endpointId":"ep_1"}`. Set `InsertOptions.SequenceKey` instead to choose the key yourself: it's used
verbatim and wins over the attribute, so jobs of different kinds can share a sequence. Keys are 1 to 255 characters.
`GetInsertOptions()` on the args can compute it too.

## How it works

- Every sequence job is inserted as `pending`. The insert takes no lock, so it never blocks other inserts for the key
  while your transaction is open.
- A **promoter** makes the oldest pending job of a sequence (lowest `id`) `available`, or `scheduled` if its
  `ScheduledAt` is in the future, once nothing in the sequence is live (`available`, `scheduled`, `running`,
  `retryable`) and nothing halts it. A partial unique index admits at most one live job per sequence, so the guarantee
  holds even when promoters race.
- Promotion happens:
  - on the node whose completer just finished a sequence job (completed, cancelled or discarded), right after the
    outcome is written;
  - on the leader, when an insert commits: the insert NOTIFYs the sequence key and the leader promotes it;
  - on the node calling `CancelAsync`, `RetryAsync` or `DeleteAsync` on a sequence job;
  - on the leader every `SchedulerInterval` (5 s), sweeping every sequence with pending jobs. This catches anything the
    other paths missed: notifications lost to a leader change or `PollOnly`, jobs the rescuer or scheduler finalized.

  So a finished job's successor normally starts within one completer flush (50 ms) plus a fetch. With `PollOnly`, a
  sequence's first job waits for the leader's sweep.
- Retries, snoozes and graceful-shutdown interruptions keep the job live, so the sequence waits for it.
- Order is by `id`, not priority. A job scheduled in the future holds up the jobs behind it until it has run.

## Failures halt the sequence

A sequence job that **ran** (attempt ≥ 1) and ended `discarded` or `cancelled` halts its sequence: the jobs behind it
stay `pending`. To move on:

- `RetryAsync(id)` on the halted job. It goes back to `pending` and, being the oldest, runs next.
- `DeleteAsync(id)` on it. The next job is promoted at once.

A halting job is exempt from the cleaner's retention, so a halted sequence stays halted until you act.

To move past failures instead, set `SequenceContinueOn` on the jobs (`Cancelled`, `Discarded` or both). The flag on the
failed job is the one that counts, so set it consistently across a sequence, e.g. in `GetInsertOptions()`.

Cancelling a job that never started (`pending`, or promoted but not yet fetched) only takes it out of the line; it
never halts the sequence.

## Interactions

- **Retry.** `RetryAsync` on a finished or pending sequence job puts it back to `pending`, never straight to
  `available`, so it can't run beside the live job. Retrying a completed job re-runs it after whatever is live now.
- **Unique jobs.** Work as usual. A pending job blocks a duplicate, since the unique states always include `pending`.
- **`InsertManyFastAsync`.** Supported: COPY writes sequence jobs as `pending` and the insert NOTIFYs their keys.
- **Concurrent transactions.** Order is by `id`, which is assigned at insert, but a job is only promoted once it's
  committed. If two transactions insert into the same sequence and the one with the higher ids commits first, its jobs
  can run before the other's. Each transaction's own jobs keep their order, and jobs never overlap. Callers that
  serialize on the entity (e.g. `SELECT … FOR UPDATE` on its row) get commit order and id order alike.

## Mixed-version fleets

Sequences need this release (schema version 2) on the leader: it runs the sweep and promotes on insert
notifications. Older nodes are safe alongside it. Their scheduler only moves `retryable` and `scheduled` jobs and their
fetch only takes `available` ones, so they never touch `pending` rows by themselves. Their completers don't promote, so
until the leader's next sweep a sequence finished on an old node waits. Admin calls made through an older client
don't know about sequences: its `RetryAsync` makes a pending or halted job `available` directly (rejected by the live
index if the sequence already has a live job). An older leader neither promotes nor spares halting jobs from the
cleaner.

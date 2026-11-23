# Roadmap

What's planned, and what isn't. Precedent is noted where another queue ships the feature.

## Schema version 2

These features need new columns or tables, so they're designed together and ship as one migration rather than a
string of versions that consumers apply by hand.

### Partitioned concurrency limits

Caps in-flight jobs per partition key, on each node and across all of them.

- `ConcurrencyKey` on `InsertOptions`, or `[ConcurrencyKey]` on args properties.
- Limits per queue or kind: `{ Global = 5, Local = 2 }`.
- Enforced at fetch: a key at its limit isn't fetched, so its jobs stay `available` and spend no attempt.
- Local limits are filtered by the producer. Global limits need the fetch serialized per queue (a lock on the queue
  row), because `SKIP LOCKED` alone lets two nodes both see room. Only queues with limits pay for that; others keep
  the plain fetch.
- Precedent: River Pro concurrency limits.

### Rate limiting

Caps job starts over time, across every node running the queue.

- `QueueOptions.RateLimit = new(1, TimeSpan.FromSeconds(1))`, with an optional burst.
- GCRA: one `timestamptz` on the `{p}queue` row (the next time a start is allowed) is the whole limiter, with no
  refill job. The fetch locks the queue row, computes how many starts `now()` allows (capped by burst and free slots),
  uses that as its `LIMIT`, and advances the timestamp.
- Over-limit jobs are never fetched: no attempt spent, no snooze churn. The producer wakes when the next start is
  due instead of waiting for the poll.
- Global by default, so it holds through rolling deploys and scale-out, which `MaxWorkers` and in-process limiters
  don't.
- Per-key limits (one rate per customer's API key) reuse the concurrency key column and follow queue-level limits.
- Precedent: Sidekiq Enterprise rate limiters, BullMQ rate limiter.

### Sequences

Strict FIFO per key: a key's next job starts only after the previous one finishes.

- `SequenceKey` on `InsertOptions`, or `[SequenceKey]` on args.
- Jobs behind the head of their sequence are inserted as `pending`, the state already reserved in the enum. They're
  promoted when the head finishes, by the completer's set-state query or by the leader.
- The insert path takes no lock on the sequence. It runs in the caller's transaction, so a lock would block every
  other insert for that key until the caller commits.
- When a job in a sequence is discarded, the sequence either blocks or continues, set per sequence.
- Concurrency limits with `Global = 1` give mutual exclusion but not order, because retries are fetched by
  `priority, scheduled_at, id`.
- Use cases: webhook delivery ordered per endpoint, event processing per entity.
- Precedent: River Pro sequences.

### Batches

Groups jobs and enqueues a callback job when the group finishes.

- `CreateBatchAsync(...)` returns a batch ID, and jobs are inserted with it.
- `OnComplete` (all finished) and `OnSuccess` (all succeeded) callback jobs.
- Progress: total, completed, failed, discarded, from one aggregate query.
- The leader finds finished batches and inserts the callback as a unique job keyed by the batch ID, so a failover
  can't enqueue it twice. Batch counters stay out of the completer's batched writes, where one batch row would become
  a hot spot.
- Use cases: imports, fan-out/fan-in, report generation.
- Precedent: Sidekiq batches.

### Durable periodic jobs

Schedules stored in the database, created, edited, paused and deleted at runtime.

- Stored as kind plus args JSON, because a `Func<IJobArgs>` can't be stored. Code-registered periodic jobs keep
  working alongside them.
- Last-run times are persisted for every periodic job, code-registered ones included. That fixes the skipped or
  repeated run on leader failover, which today's in-memory schedule allows.
- Precedent: River Pro durable periodic jobs.

## No schema change

### Heartbeat and progress

- `HeartbeatAsync()` on the job records `oxbow:heartbeat_at` in metadata. The rescuer takes the later of
  `attempted_at` and the heartbeat, so a healthy long job isn't retried. That matters most for jobs with an infinite
  timeout, and for kinds the leader has no worker for, which today are judged by `attempted_at` alone.
- `ReportProgressAsync(percent, message)` writes progress to metadata, readable through `GetAsync`.
- `attempted_at` itself doesn't move; queue wait and history depend on it. Heartbeats are throttled on the client,
  since each one updates a hot row.

### Bulk cancel and retry by query

- `CancelManyAsync(filter)` and `RetryManyAsync(filter)`, generalizing the single-job queries.
- Bulk cancel sends a control NOTIFY for running jobs so their nodes stop them.
- `JobListQuery` gains `Metadata` (containment, served by the existing GIN index) and `Tags` filters, used by both
  `ListAsync` and the bulk operations. Example: cancel every job for one customer.

### Job output

- A worker records a result, stored under `output` in metadata and returned by `GetAsync` and `ListAsync`.
- Written with the completion through the completer's existing metadata merge, so it costs no extra round trip.
- Capped in size.
- Precedent: River job output.

### Discard hook

- `IJobDiscardHandler`, called when a job is discarded or cancelled.
- Called after the completer writes the outcome, so it sees the state actually written, including a retry that
  became a cancellation.
- Covers discards that middleware never sees: the rescuer's and the scheduler's unique-key conflicts.
- Best-effort: if the node dies first, the call is lost.

### Args encryption

- Built on ASP.NET Core Data Protection (`IDataProtector`), which brings key IDs and rotation.
- Opt in per kind (`[EncryptArgs]`) or globally. Encrypted args are stored as `{"enc": …, "kid": …}`, so the column
  stays jsonb.
- Encrypted after the unique key is computed, decrypted when the worker unit is created.
- With encryption on, the unique key becomes an HMAC. A plain SHA-256 of low-entropy args (an email, an invoice ID)
  would let anyone with database access confirm a guess.
- `OxbowAssert` takes the protector, and the args GIN index is no use for encrypted kinds.
- Precedent: Sidekiq Enterprise encryption.

## Packages

### Oxbow.HealthChecks

`IHealthCheck`s for queue lag, stuck jobs, an elected leader, and a matching schema version. They query `{p}leader`
and `{p}job` directly: the `oxbow.queue.depth` gauge is published only by the leader, and `IsLeader` only describes
the local node.

### Oxbow.AspNetCore

Minimal-API admin endpoints: list, inspect, retry, cancel and delete jobs, in bulk too; pause and resume queues; batch,
sequence and periodic-job views as those ship. Authorization is left to the host through endpoint metadata.

### Oxbow.Dashboard

A web UI over `Oxbow.AspNetCore`, with no data access of its own: queues, job states, errors, retries, batches,
sequences and periodic jobs. Precedent: River UI, Hangfire Dashboard.

### Oxbow.Testing additions

- `DrainAsync()` works every available job inline until the queues are empty. It uses the real executor, so
  middleware and outcomes match production.
- `MakeDueAsync()` sets `scheduled_at = now()` on scheduled and retryable jobs, then runs the scheduler. A fake
  `TimeProvider` can't do this, because fetching and scheduling compare against the database's `now()`.

## Not planned

| Feature | Why not |
|---|---|
| Fair scheduling across partitions | A round-robin fetch needs a window function over the queue, which leaves `{p}job_fetch_idx` and costs more the deeper the queue gets. Per-key concurrency limits stop one partition from taking every slot, and separate queues cover tiers. |
| Custom metadata | Already shipped: `InsertOptions.Metadata`, insert middleware, and `Job.Metadata` in workers. The missing piece, list filters, is part of bulk operations. |

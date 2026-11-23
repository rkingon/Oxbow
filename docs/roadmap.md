# Roadmap

What's planned, and what isn't. Precedent is noted where another queue ships the feature.

## Shipped in 0.2.0-alpha.1

The core items, with schema version 2:
[concurrency limits](concurrency-limits.md), [rate limiting](rate-limiting.md), [sequences](sequences.md),
[batches](batches.md), [durable periodic jobs](scheduled-and-periodic-jobs.md),
[heartbeat and progress](workers.md#heartbeat-and-progress), [bulk operations and list filters](managing-jobs.md),
[job output](workers.md#output), [discard handlers](discard-handlers.md) and [args encryption](encryption.md).

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

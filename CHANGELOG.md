# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [0.2.0-alpha.2] - 2026-11-23

`Oxbow.Abstractions` and ambient transaction enlistment. No schema change.

### Added

- `Oxbow.Abstractions`: job args, workers, `IOxbowClient` and the read models in a package with no dependencies beyond
  the BCL, so domain and application projects can own their jobs without Npgsql. The types keep the `Oxbow`
  namespace, and `Oxbow` depends on the package and forwards them. See [docs/abstractions.md](docs/abstractions.md).
- Ambient transaction enlistment: register an `IOxbowTransactionAccessor` with `AddTransactionAccessor<T>()`, or
  `AddDbContextTransaction<TContext>()` from `Oxbow.EntityFrameworkCore`. `IOxbowClient` then becomes scoped, and its
  `InsertAsync`, `InsertManyAsync`, `InsertManyFastAsync` and `InsertBatchAsync` insert in the accessor's current
  transaction. `OxbowOptions.RequireAmbientTransaction` makes those inserts throw when there's no current transaction.
- `AddInsertMiddleware<T>(ServiceLifetime)`: scoped insert middleware, resolved from the inserting scope through the
  scoped client, or from a scope of its own otherwise.
- `IJobRuntime`, behind `Job<TArgs>.HeartbeatAsync` and `ReportProgressAsync`, with a `Job<TArgs>` constructor that
  takes one. `Oxbow.Testing` adds `RecordingJobRuntime`.
- `RecordingOxbowClient` in `Oxbow.Testing`: an in-memory `IOxbowClient` for unit testing code that enqueues. It runs
  insert middleware and records each job's kind, args, options and metadata.
- `JobInsertContext` and `JobInsertParams` have public constructors, for unit testing insert middleware.

### Changed

- **Breaking:** `IOxbowNpgsqlClient : IOxbowClient` in `Oxbow` now holds the members that need Npgsql or the runtime:
  the overloads taking an `NpgsqlConnection` and `NpgsqlTransaction`, and periodic-job management. Extension methods
  on `IOxbowClient` keep existing calls compiling. Code that implements or mocks `IOxbowClient` and set up those
  members must target `IOxbowNpgsqlClient` instead, and assemblies compiled against them need recompiling.
  `AddOxbow` registers both interfaces.

## [0.2.0-alpha.1] - 2026-11-23

Schema version 2 and the core roadmap. **Upgrade the schema before deploying**: see the first item under Changed.

### Added

- Args encryption at rest: mark a kind `[EncryptArgs]` or set `OxbowOptions.EncryptAllArgs`, and register a protector.
  The new `Oxbow.DataProtection` package's `AddArgsEncryption()` implements `IJobArgsProtector` on ASP.NET Core Data
  Protection. Encrypted args are stored as the JSON string `"oxbow:enc:v1:…"`; unique-by-args keys of encrypted kinds
  are an HMAC. `IJobArgsProtector.UnprotectArgs(row)` reads stored args, and `OxbowAssert.ArgsProtector` decrypts in
  assertions.
  See [docs/encryption.md](docs/encryption.md).
- Heartbeat and progress: `Job<TArgs>.HeartbeatAsync` and `ReportProgressAsync`, stored as `oxbow:heartbeat_at` and
  `oxbow:progress` in metadata and read through `JobRow.HeartbeatAt` and `JobRow.Progress`. The rescuer uses the later
  of `attempted_at` and the heartbeat, so a healthy long job isn't rescued, and an attempt that heartbeats is rescued
  once its heartbeats stop, even with no timeout. `WorkResult.Progress` in `Oxbow.Testing`.
- Batches: `IOxbowClient.InsertBatchAsync` inserts a group of jobs and its batch row in one statement (on your
  transaction, its own, or a `DbContext`'s), with `OnComplete` and `OnSuccess` callback jobs that the leader enqueues
  exactly once when the group finishes. `GetBatchAsync` reports progress, `JobRow.BatchId` and
  `JobFilter.BatchId` link jobs to their batch, and `OxbowOptions.BatchRetention` (24 hours) sets how long a finished
  batch is kept. The job cleaner keeps a batch's members while the batch exists.
- `CancelManyAsync`, `RetryManyAsync` and `DeleteManyAsync` on `IOxbowClient`: the single-job semantics applied to every
  job matching a `JobFilter`, in batches. Bulk retry skips jobs whose unique key another live job holds and reports
  them in `RetryManyResult`. Bulk cancel stops running jobs on every node with batched control NOTIFYs; nodes on
  0.1.0-alpha.3 pick those up at their cancel poll instead.
- `JobFilter`, now the base of `JobListQuery`, with `BatchId`, `Metadata` (JSON containment) and `Tags` (all of)
  filters for `ListAsync` and the bulk operations.
- Job output: `job.SetOutput(value)` records a result in `metadata.output` with the completion, read back with
  `JobRow.Output` and `JobRow.GetOutput<T>()`; capped at `JobRow.MaxOutputSize` (1 MiB). `WorkResult.Output` in
  `Oxbow.Testing`.
- Discard handlers: `AddDiscardHandler<T>()` registers an `IJobDiscardHandler`, called best-effort after a job is
  discarded or cancelled by its worker, a cancel request, the rescuer or the scheduler's unique-key check.
- Durable periodic jobs. Run times of every periodic job are persisted in `{prefix}periodic_job` (schema version 2), so
  a leader failover or restart neither skips nor repeats a run, and missed runs catch up once. Periodic jobs can be
  stored and managed at runtime: `IOxbowClient.CreatePeriodicJobAsync`, `UpsertPeriodicJobAsync`,
  `PausePeriodicJobAsync`, `ResumePeriodicJobAsync`, `DeletePeriodicJobAsync`, `GetPeriodicJobAsync` and
  `ListPeriodicJobsAsync` (paged).
  `AddPeriodicJob(name, …)` names a code-registered job; unnamed ones get a name derived from kind and schedule.
- Sequences: `InsertOptions.SequenceKey` or `[SequenceKey]` on args runs jobs with the same key one at a time, in
  insert order. A job that ran and ended discarded or cancelled halts its sequence until it's retried or deleted, unless
  `InsertOptions.SequenceContinueOn` says to move past it. `JobRow.SequenceKey` and `JobFilter.SequenceKey` read and
  target a sequence. Needs schema version 2 and this release on the leader.
- Concurrency limits: `QueueOptions.ConcurrencyLimit = new() { Global = 5, Local = 2 }` caps running jobs per
  partition key, across nodes and per node. The key comes from `InsertOptions.PartitionKey`, the args' defaults,
  `[PartitionKey]` properties, or insert middleware (`JobInsertParams.PartitionKey`). Over-limit jobs stay
  `available` without spending an attempt. The key is on `JobRow.PartitionKey`, and `JobFilter.PartitionKey` lists and
  bulk-manages one partition. Schema version 2 adds `{prefix}job.partition_key`.
- Rate limiting: `QueueOptions.RateLimit = new(1, TimeSpan.FromSeconds(1))` caps job starts per period across every
  node working the queue, with an optional `Burst`; `QueueOptions.PartitionRateLimit` does the same per partition key.
  Enforced at fetch with GCRA under the queue lock, on database time: jobs over the limit stay `available` without
  spending an attempt, and the producer wakes when the next start is due. Schema version 2 adds
  `{prefix}queue.rate_limit_tat` and `{prefix}rate_limit`. See [docs/rate-limiting.md](docs/rate-limiting.md).

### Changed

- Schema version 2. Apply `OxbowSchema.GetMigrations(1, 2)` (or `AddOxbowSchema(version: 2)`, or `oxbow migrate`) and
  grant `GetGrants(role, version: 2)` before deploying. The previous release keeps working against version 2.
- **Breaking (EF):** `AddOxbowSchema`, `RemoveOxbowSchema` and `GrantOxbowAccess` take a required `version`. A default
  of the latest version made existing migrations replay different SQL after a package upgrade. Pass `version: 1` in
  your existing Oxbow migration.
- Grants are versioned: `OxbowSchema.GetGrants(role, version)` returns the grants for the objects one version adds.
  `GetGrants(role)` still returns the full set.
- `runOnStart` on periodic jobs enqueues the first run only, when the job has no persisted state yet. It no longer runs
  on every start or change of leader. Periodic runs carry `oxbow:periodic_job` in their metadata.
- `JobListQuery` derives from `JobFilter`, which now declares `States`, `Queues` and `Kinds`. Source compatible;
  recompile code built against an earlier release.

## [0.1.0-alpha.3] - 2026-11-23

### Fixed

- `StopAsync` on Oxbow's hosted service no longer throws `ObjectDisposedException` when called after the host is
  disposed; it's idempotent, and disposing a never-stopped host cancels Oxbow's loops.

## [0.1.0-alpha.2] - 2026-11-22

### Added

- `Oxbow.EntityFrameworkCore`: `InsertAsync(DbContext, …)` and `InsertManyAsync(DbContext, …)` in the context's
  current transaction (throws without one), and `AddOxbowSchema`, `RemoveOxbowSchema` and `GrantOxbowAccess` for EF
  migrations.
- `Oxbow.Testing`: `OxbowAssert` (inserted / not inserted, inside the test's transaction) and `WorkerHarness`, which runs
  a worker inline with its middleware and timeout, without a database.
- `Oxbow.Cli`: the `oxbow` dotnet tool, with `sql`, `grants` and `migrate`.
- `IOxbowClient.InsertManyFastAsync`: bulk insert with binary COPY, returning a count (no unique jobs).
- `Job<TArgs>` and `JobContext` have public constructors, for unit testing workers and middleware.

### Changed

- Attempt outcomes are written in batches (River's batch completer) instead of one round trip per job.
- Inserts send at most one wake-up NOTIFY per queue per fetch cooldown.

## [0.1.0-alpha.1] - 2026-11-22

First preview of the core `Oxbow` package.

### Added

- Schema version 1 as embedded SQL (`OxbowSchema.GetMigrations`, `OxbowSchema.GetGrants`), with configurable schema
  and object prefix.
- Transactional insert on the caller's `NpgsqlConnection` and `NpgsqlTransaction`; `InsertManyAsync` in one statement.
- Typed jobs (`IJobArgs`, `[JobKind]`), DI-scoped workers (`IWorker<TArgs>`), System.Text.Json args.
- Retries with River's backoff and an errors history; `JobSnoozeException`, `JobCancelException`; per-job timeouts.
- Cancellation of queued jobs and of running jobs across nodes (LISTEN/NOTIFY, with polling fallback).
- Unique jobs by args, period, queue and state, enforced by a unique index.
- Scheduled jobs, periodic jobs (interval or cron), named queues with per-queue concurrency, pause and resume.
- Leader election with scheduler, rescuer, job cleaner and queue cleaner.
- Insert and work middleware; `ActivitySource` and `Meter` named `Oxbow`.
- Graceful shutdown that returns interrupted jobs to `available` without spending an attempt.

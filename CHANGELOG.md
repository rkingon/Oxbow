# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Durable periodic jobs. Run times of every periodic job are persisted in `{prefix}periodic_job` (schema version 2), so
  a leader failover or restart neither skips nor repeats a run, and missed runs catch up once. Periodic jobs can be
  stored and managed at runtime: `IOxbowClient.CreatePeriodicJobAsync`, `UpsertPeriodicJobAsync`,
  `PausePeriodicJobAsync`, `ResumePeriodicJobAsync`, `DeletePeriodicJobAsync`, `GetPeriodicJobAsync` and
  `ListPeriodicJobsAsync` (paged).
  `AddPeriodicJob(name, …)` names a code-registered job; unnamed ones get a name derived from kind and schedule.

### Changed

- `runOnStart` on periodic jobs enqueues the first run only, when the job has no persisted state yet. It no longer runs
  on every start or change of leader. Periodic runs carry `oxbow:periodic_job` in their metadata.

- Schema version 2. Apply `OxbowSchema.GetMigrations(1, 2)` (or `AddOxbowSchema(version: 2)`, or `oxbow migrate`) and
  grant `GetGrants(role, version: 2)` before deploying. The previous release keeps working against version 2.
- Grants are versioned: `OxbowSchema.GetGrants(role, version)` returns the grants for the objects one version adds.
  `GetGrants(role)` still returns the full set.
- **Breaking (EF):** `AddOxbowSchema`, `RemoveOxbowSchema` and `GrantOxbowAccess` take a required `version`. A default
  of the latest version made existing migrations replay different SQL after a package upgrade. Pass `version: 1` in
  your existing Oxbow migration.

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

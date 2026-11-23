# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

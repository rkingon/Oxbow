# Differences from River

Reference: River `main` at `1f19003`. Each entry gives what differs and why.

| Area | River | Oxbow | Why |
|---|---|---|---|
| Driver | Pluggable drivers (pgx, `database/sql`, SQLite) | Npgsql only, no driver abstraction | .NET has one Postgres driver. An abstraction would be surface with no second implementation behind it. |
| Migrations | `rivermigrate` runs DDL and tracks it in `river_migration` | Consumer applies the SQL from `OxbowSchema`, EF, or the CLI. `{p}migration` is only a version marker checked at startup | The consumer owns migrations and the runtime never runs DDL. |
| Schema history | Migrations 001–008 | Version 1 = River's state at 008, without `river_migration`, `river_notification` (SQLite outbox) or `river_client*` | Greenfield, so there's no history to replay. |
| Naming | Configurable schema; fixed `river_` prefix | Configurable schema; configurable prefix on every created object: tables, enum, functions, indexes, NOTIFY channels (default `public`, `oxbow_`) | Lets several apps, or test runs, share a database. |
| Reindexer | Leader runs `REINDEX` on job indexes | Not included | `REINDEX` needs ownership, and the runtime role has none. Consumers schedule it as the owner if needed. |
| Rescuer, unknown kind | Discards a stuck job when the leader has no worker for its kind | Retries it with the global retry policy, discarded only by attempts | The leader may legitimately not run that kind's queue (queues are the routing boundary), so the leader's worker set says nothing about whether the kind exists. |
| Job kind | `Kind()` method on the args struct | `[JobKind("…")]` attribute | A static abstract interface member would make `IJobArgs` unusable as a type argument (CS8920). |
| Unique fields | `river:"unique"` struct tag | `[UniqueKey]` property attribute, read via `JsonPropertyInfo.AttributeProvider` | Works with System.Text.Json source generation. |
| Worker control flow | `JobSnooze`/`JobCancel` sentinel errors | `JobSnoozeException`/`JobCancelException` | .NET reports failure with exceptions. |
| Cancellation context | `context.Context` cancel cause | One `CancellationToken`. The executor tracks which source (remote, timeout, shutdown) fired | Workers only need to honor the token. |
| Hooks | Separate hook interfaces (`InsertBegin`, `WorkBegin`, `WorkEnd`, …) alongside middleware | Insert and work middleware only | Middleware covers every hook point and is the ASP.NET Core shape .NET developers already know. |
| Job output | `river.RecordOutput`, stored in `metadata.output`, up to 32 MB | `job.SetOutput`, stored in `metadata.output`, up to 1 MiB (`JobRow.MaxOutputSize`) | The output rides in the job row and through the batched completion; `metadata` is GIN-indexed. Large results belong elsewhere, with a reference in the output. |
| Discard hook | None; hooks (`WorkEnd`) and `client.Subscribe` see failures as they happen | `IJobDiscardHandler`, called after a discard or cancellation is written, including the rescuer's and the scheduler's | Middleware only sees what the worker did, not the state written (a retry that became a cancellation) or discards that happen without a worker. |
| Event subscription | `client.Subscribe(kinds…)` channel | Not included; covered by `Meter`/`ActivitySource` and middleware | Conventional .NET observability instead of a bespoke event stream. |
| Durable periodic jobs, workflows, sequences, resumable jobs, plugins | River Pro, or recent OSS additions | Not included | Out of scope for parity v1. `pending` stays in the enum for future use. |
| Bulk insert | `InsertMany` (unnest, returning rows) and `InsertManyFast` (COPY, count only) | Same split: `InsertManyAsync` (unnest, rows returned, unique jobs supported) and `InsertManyFastAsync` (binary COPY, count only, unique jobs rejected) | — (parity) |
| EF insert | — | Throws without `Database.CurrentTransaction` | Otherwise the job commits ahead of `SaveChanges`. |
| Tracing | None built in | `oxbow.insert`/`oxbow.work` spans; `traceparent` stored in metadata and linked | OpenTelemetry is the .NET convention. A link, not a parent, because work can run hours later. |
| Heartbeat and progress | None; the rescuer judges a job by `attempted_at` and its timeout | `HeartbeatAsync` and `ReportProgressAsync` on the job, stored in metadata; the rescuer uses the later of `attempted_at` and the heartbeat, and an attempt that has heartbeated is rescued once `RescueStuckJobsAfter` passes without a heartbeat, regardless of its timeout (even none) | Long jobs, jobs with no timeout, and kinds the leader doesn't run can't otherwise be told apart from dead ones. |
| Rescue metadata key | `river:rescue_count` | `oxbow:rescue_count` | Namespace. |
| Pausing an unknown queue | `QueuePause` returns not-found | Creates the queue row already paused | Lets a deploy pause a queue before any node has run it. |

# Oxbow design

Oxbow is a durable, Postgres-backed job queue for .NET, modeled on [River](https://github.com/riverqueue/river)
(reference: River `main` at `1f19003`, schema line `main` through migration 008). Where Oxbow departs from River, the
departure and its reason are recorded in [differences-from-river.md](differences-from-river.md). This document doesn't
repeat them.

## Packages and targets

| Package                     | Contents                                                                    | Depends on                                                   |
|-----------------------------|-----------------------------------------------------------------------------|--------------------------------------------------------------|
| `Oxbow`                     | Abstractions, client, processor, maintenance, embedded SQL, `OxbowSchema`    | `Npgsql` ≥ 10, `Microsoft.Extensions.*.Abstractions`, `Cronos` |
| `Oxbow.EntityFrameworkCore` | `InsertAsync(DbContext, …)`, `migrationBuilder.AddOxbowSchema(…)`            | `Oxbow`, `Npgsql.EntityFrameworkCore.PostgreSQL` 10.x        |
| `Oxbow.Testing`             | Insert assertions, an inline worker harness                                 | `Oxbow`                                                      |
| `Oxbow.Cli` (`dotnet tool`) | `oxbow sql`, `oxbow grants`, `oxbow migrate`                                 | `Oxbow`, `System.CommandLine`                                |

- **Target:** `net10.0` (LTS). Dependency floors are the lowest versions that work (Npgsql 10), so consumers on Npgsql 11
  float up. EF Core 11 needs `net11.0`, so `Oxbow.EntityFrameworkCore` gains a `net11.0` target once someone needs it.
- **Cron parser:** [Cronos](https://github.com/HangfireIO/Cronos). It's MIT, maintained by the Hangfire team
  (0.13.0, Apr 2026, 80M+ downloads), handles seconds and time zones, and does the right thing across DST transitions,
  which is where most cron parsers get it wrong. NCrontab, the alternative, has no time-zone or DST handling.

## Repository layout

```
Oxbow.slnx  global.json  Directory.Build.props  Directory.Packages.props  .editorconfig
src/Oxbow/  src/Oxbow.EntityFrameworkCore/  src/Oxbow.Testing/  src/Oxbow.Cli/
tests/Oxbow.Tests/               unit tests, no database
tests/Oxbow.IntegrationTests/    Testcontainers, Postgres 16 and 17, everything under the least-privilege role
benchmarks/Oxbow.Benchmarks/     BenchmarkDotNet
samples/  docs/
```

Tests use xunit.v3. CI runs on GitHub Actions, with actions pinned to SHAs. A `v*` tag runs
`.github/workflows/publish.yml` in the `nuget` environment, which packs and publishes to nuget.org through **Trusted
Publishing**: the job requests `id-token: write`, `NuGet/login` (user from the `NUGET_USER` repo variable) exchanges the
GitHub OIDC token for a short-lived key, and no API key is ever stored. The nuget.org policy is bound to repo
`rkingon/Oxbow`, workflow `publish.yml` and environment `nuget`, so renaming any of the three breaks publishing.

## Schema

Version 1 collapses River's migrations 001–008 into one script, minus River's own migration table, the SQLite
notification outbox and the dropped `river_client*` tables. Every identifier is qualified as `{schema}.{prefix}name`,
and the prefix goes on everything Oxbow creates: tables, the enum, functions, indexes, constraints and NOTIFY channels.
The defaults are schema `public` and prefix `oxbow_`. Both are validated against `^[a-z_][a-z0-9_]*$`, and the combined
length is capped so that the longest NOTIFY channel, `{schema}.{prefix}leadership`, stays within Postgres's 63-byte limit.

```sql
CREATE TYPE {s}.{p}job_state AS ENUM ('available','cancelled','completed','discarded','pending','retryable','running','scheduled');

CREATE TABLE {s}.{p}job (
  id            bigserial PRIMARY KEY,
  state         {s}.{p}job_state NOT NULL DEFAULT 'available',
  attempt       smallint NOT NULL DEFAULT 0,
  max_attempts  smallint NOT NULL DEFAULT 25,
  attempted_at  timestamptz,
  created_at    timestamptz NOT NULL DEFAULT now(),
  finalized_at  timestamptz,
  scheduled_at  timestamptz NOT NULL DEFAULT now(),
  priority      smallint NOT NULL DEFAULT 1,
  args          jsonb NOT NULL,
  attempted_by  text[],
  errors        jsonb[],
  kind          text NOT NULL,
  metadata      jsonb NOT NULL DEFAULT '{}',
  queue         text NOT NULL DEFAULT 'default',
  tags          varchar(255)[] NOT NULL DEFAULT '{}',
  unique_key    bytea,
  unique_states bit(8),
  -- River's CHECKs: finalized_at set iff state is terminal; priority 1..4; queue/kind length 1..127
);
-- {p}job_kind_idx              (kind)
-- {p}job_finalized_idx         (state, finalized_at) WHERE finalized_at IS NOT NULL
-- {p}job_fetch_idx             (state, queue, priority, scheduled_at, id)
-- {p}job_args_idx              GIN(args)
-- {p}job_metadata_idx          GIN(metadata)
-- {p}job_unique_idx            UNIQUE (unique_key) WHERE unique_key IS NOT NULL AND unique_states IS NOT NULL
--                                AND {p}job_state_in_bitmask(unique_states, state)
CREATE FUNCTION {s}.{p}job_state_in_bitmask(bit(8), {s}.{p}job_state) RETURNS boolean IMMUTABLE ...;

CREATE UNLOGGED TABLE {s}.{p}leader (elected_at, expires_at, leader_id, name text PRIMARY KEY DEFAULT 'default' CHECK (name = 'default'));
CREATE TABLE {s}.{p}queue (name text PRIMARY KEY, created_at, metadata jsonb, paused_at, updated_at);
CREATE TABLE {s}.{p}migration (version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());
```

Version 2 adds, for [batches](batches.md), `{p}batch` (counts, prepared callbacks and their job IDs; partial indexes on
unfinished and finished rows) and a nullable `{p}job.batch_id` with `{p}job_batch_idx (batch_id, state) WHERE batch_id
IS NOT NULL`, which serves both the finisher's "any live member?" probe and the progress aggregate.

The `{p}migration` table is a version marker that each script writes to. Nothing reads it as a migration runner. At
startup the client checks `max(version)` against the version it requires and fails fast with a message naming the
missing versions and how to get their SQL. It never applies them.

### Shipping the SQL

- `src/Oxbow/Schema/NNN_name.up.sql` and `.down.sql` are embedded resources with `{{schema}}` and `{{prefix}}` tokens.
- `OxbowSchema.GetMigrations(int fromVersion, int toVersion, OxbowSchemaOptions? options)` returns the rendered scripts
  as `IReadOnlyList<OxbowMigration>` (version, direction, SQL). A downgrade is just `from > to`.
- `src/Oxbow/Schema/NNN_name.grants.sql` holds the grants on the objects version NNN adds (possibly none).
  `OxbowSchema.GetGrants(role, version)` renders one version's; `GetGrants(role)` renders all of them, for the latest
  schema.
- EF: `migrationBuilder.AddOxbowSchema(version: N)` and `GrantOxbowAccess(role, version: N)` in `Up`,
  `RemoveOxbowSchema(version: N)` in `Down`, so each Oxbow schema version gets its own EF migration in the consumer's
  history. Versions are required, never defaulted to the latest: a migration must replay the same SQL after a package
  upgrade, and a version's grants must not name objects a later version adds.
- A schema version's scripts may change until a release ships it. A database that ran an unreleased version's script
  has to be recreated or reconciled by hand.
- CLI: `oxbow sql --from 0 --to 1 [--schema s --prefix p]` prints the SQL, `oxbow grants --role r` prints the grants, and
  `oxbow migrate --connection … [--to N]` applies the scripts and their marker rows in one transaction.

### Least privilege

The runtime role needs exactly the following. It ships as `GetGrants`, and the integration suite runs everything under a
role with these grants and nothing else: no superuser, no ownership, no `CREATE` on any schema, `TEMP` revoked on the
database.

```sql
GRANT USAGE ON SCHEMA {s} TO {role};
GRANT SELECT, INSERT, UPDATE, DELETE ON {s}.{p}job, {s}.{p}queue, {s}.{p}leader TO {role};
GRANT SELECT ON {s}.{p}migration TO {role};
GRANT USAGE ON SEQUENCE {s}.{p}job_id_seq TO {role};
GRANT EXECUTE ON FUNCTION {s}.{p}job_state_in_bitmask(bit(8), {s}.{p}job_state) TO {role};
```

LISTEN/NOTIFY needs no grant. The runtime never runs DDL, never creates temp tables, never calls `SET` (or
`SET LOCAL`), and never relies on `search_path`, because every identifier is qualified. That last part matters most for
transactional enqueue: the SQL runs on the caller's connection, inside whatever role, tenant settings and RLS context the
caller has established, and leaves all of it untouched. The suite includes a consumer table with `FORCE ROW LEVEL
SECURITY` written in the same transaction as an insert. Oxbow's own tables don't enable RLS. If a consumer turns it on
(they own the tables), they need a policy that lets the runtime role see every row; the docs carry an example.

### Talking to Postgres without owning the data source

Transactional enqueue runs on a connection Oxbow didn't configure, so the SQL can't rely on any type mapping registered
on the consumer's `NpgsqlDataSource`. Every query therefore:

- sends the state enum as `text` and casts it (`$n::{s}.{p}job_state`), and reads it back as `state::text`;
- sends JSON as `text` typed `NpgsqlDbType.Jsonb`, and reads `errors` back as `errors::text[]`;
- sends `unique_states` as `int` and casts it with `::bit(8)`.

Rendered SQL is built once per options instance and cached. No query concatenates user input.

## Public model

```csharp
[JobKind("send_email")]                                 // required; River's Kind(). Aliases = [...] for renames
public sealed record SendEmail([property: UniqueKey] string To, string Body) : IJobArgs
{
    // Optional per-kind insert defaults (River's InsertOpts() on args), via a default interface method.
    public InsertOptions GetInsertOptions() => new() { Queue = "email", Unique = new() { ByArgs = true } };
}

public sealed class SendEmailWorker(IEmailSender sender) : IWorker<SendEmail>
{
    public Task WorkAsync(Job<SendEmail> job, CancellationToken cancellationToken) => ...;

    // Optional overrides (default interface methods): GetTimeout(job), GetNextRetry(job).
}
```

- **Kind comes from `[JobKind]`, not a static abstract member.** An interface with a static abstract member can't be used
  as a type argument (CS8920), which would break `IEnumerable<IJobArgs>` in `InsertManyAsync` and anywhere else args
  are handled as a group. It reads like `[JsonDerivedType]`. Kinds are explicit strings with no default taken from the
  type name, so renaming a class never orphans queued jobs. Kinds and aliases are validated at startup: every registered
  args type has `[JobKind]`, and no kind or alias appears twice. Lookups are cached per type and stay trim-safe, which
  leaves room for a source generator later. An insert whose args type has no `[JobKind]` throws with the type's name.
- **Inserting a kind with no registered worker throws**, as River does. The check is skipped automatically when no
  workers are registered (insert-only clients). `OxbowOptions.SkipUnknownKindCheck` covers clients that have workers
  but insert kinds worked by another service. This is an options-level setting, like River's
  `Config.SkipUnknownJobCheck`, rather than a per-call flag every insert would have to repeat.
- **`Job<TArgs>`** carries `Args` plus the row: `Id`, `Kind`, `Queue`, `Priority`, `Attempt`, `MaxAttempts`, `AttemptedAt`,
  `AttemptedBy`, `CreatedAt`, `ScheduledAt`, `Errors`, `Metadata`, `Tags`. The untyped `JobRow` is the same without `Args`
  (raw `JsonElement`), and it's what the client's get/list/cancel methods return.
- **Serialization** uses `OxbowOptions.JsonSerializerOptions`. Its `TypeInfoResolver` can be the consumer's
  `JsonSerializerContext`, so trimming and AOT keep working. Args go through `GetTypeInfo(type)`, never through reflection.
- **Unique by selected properties:** a `[UniqueKey]` attribute, found through `JsonPropertyInfo.AttributeProvider`, which
  source generation also populates. The key string follows River (`&kind=…&args={sorted}&period=…&queue=…`), hashed with
  SHA-256. Keys are ordinally sorted, and the walk recurses into nested objects the way River's struct-tag walk does.
  When an encrypted kind is unique by args, the protector's HMAC replaces SHA-256 ([encryption](encryption.md)).
- **Control flow from a worker** uses exceptions, which is the .NET equivalent of River's sentinel errors:
  `throw new JobSnoozeException(TimeSpan)` and `throw new JobCancelException(reason)`.

### Registration

```csharp
builder.Services.AddNpgsqlDataSource(connectionString);           // Npgsql's own DI package; Oxbow resolves it
builder.Services.AddOxbow(o =>
    {
        o.Queues["default"] = new() { MaxWorkers = 100 };
        o.Queues["email"]   = new() { MaxWorkers = 10 };
        o.AddPeriodicJob(PeriodicSchedule.Cron("0 * * * *"), () => new Cleanup(), runOnStart: true);
    })
    .AddWorker<SendEmail, SendEmailWorker>()                      // scoped; resolved in a fresh scope per job
    .AddWorkMiddleware<TenantMiddleware>()
    .AddInsertMiddleware<StampCorrelationId>();
```

- `OxbowOptions` is validated on start (`ValidateOnStart`): queue names, schema and prefix identifiers, positive
  intervals, and retention periods.
- A different data source can be chosen with `o.DataSourceServiceKey` (keyed services).
- With no queues configured, Oxbow runs insert-only: it registers the client, and the hosted service starts nothing.

## Client API (`IOxbowClient`, singleton)

| Method | Notes |
|---|---|
| `InsertAsync<TArgs>(args, options?, ct)` | Own connection from the data source. |
| `InsertAsync<TArgs>(NpgsqlConnection, NpgsqlTransaction?, args, options?, ct)` | Caller's connection and transaction. The insert NOTIFY is part of the transaction, so a rollback sends nothing. |
| `InsertManyAsync(IEnumerable<InsertManyItem>, ct)` and the connection/transaction overload | Multi-row `INSERT … SELECT unnest(…)` with unique support; returns rows (see bulk insert). |
| `CancelAsync(id, ct)` | Not started: becomes cancelled immediately. Running: `cancel_attempted_at` is set and a control NOTIFY goes out. Finalized: returned as is. |
| `RetryAsync(id, ct)`, `DeleteAsync(id, ct)`, `GetAsync(id, ct)` | River semantics. Delete and retry never touch a running job. |
| `ListAsync(JobListQuery, ct)` | Filters (kinds, queues, states), ascending or descending by ID, keyset cursor. |
| `InsertBatchAsync(items, BatchOptions?, ct)` and the connection/transaction overload | The batch row and its jobs in one statement (`WITH jobs AS (INSERT …) INSERT INTO {p}batch …`), so it's atomic even in autocommit. Callbacks are prepared and stored on the batch row. |
| `GetBatchAsync(id, ct)` | Progress: aggregated from members until finished, frozen on the batch row after. |
| `PauseQueueAsync(name, ct)`, `ResumeQueueAsync(name, ct)` | Updates the `{p}queue` row and sends a control NOTIFY in one statement; `"*"` means all queues. |

`InsertResult` is `(JobRow Job, bool UniqueSkippedAsDuplicate)`. EF: `client.InsertAsync(dbContext, args, …)` uses
`dbContext.Database.GetDbConnection()` and `CurrentTransaction.GetDbTransaction()`. With no current transaction it throws
`InvalidOperationException`, whose message names both fixes: call `Database.BeginTransactionAsync` first, or use the
plain `IOxbowClient.InsertAsync` if an immediate commit was intended. The EF path has no opt-out flag; an immediate
commit always goes through the plain client.

## State machine

```
insert ──► available ──fetch──► running ──ok──────────────► completed
   │  (scheduled_at > now)        │  ├─error, attempts left ► retryable ──scheduler──► available
   └──► scheduled ──scheduler──►  │  ├─error, attempts gone ► discarded
                                  │  ├─JobCancelException ──► cancelled
                                  │  ├─JobSnoozeException ──► scheduled (attempt - 1, metadata.snoozes + 1)
                                  │  ├─soft shutdown ───────► available (attempt - 1)
                                  │  └─stuck > RescueAfter ─► retryable / discarded / cancelled  (rescuer)
CancelAsync on available/scheduled/retryable ─► cancelled
```

Every completion goes through River's `JobSetStateIfRunningMany`, which only moves a job that is still `running`. If
`cancel_attempted_at` is set, a requested retry, snooze or soft stop becomes `cancelled`, while a successful completion
stays `completed` (the worker chose to finish). When the next run is due within the scheduler interval, the job goes
straight to `available` rather than through `retryable`/`scheduled`, as River does. `errors` gets one entry per failed
attempt: `{ at, attempt, error: "{ExceptionType}: {Message}", trace }`.

**Retry policy:** `attempt^4` seconds with ±10% jitter, where `attempt = errors.Count + 1`. That's River's default, and
it sums to roughly three weeks over 25 attempts. A worker's `GetNextRetry` returns `DateTimeOffset?` (null falls through),
and the global `IRetryPolicy` can be replaced in DI. A result in the past falls back to the default policy, with a
warning, as in River.

**Timeouts:** the client default is 1 minute (`OxbowOptions.JobTimeout`). A worker's `GetTimeout` can override it;
`Timeout.InfiniteTimeSpan` disables it. The timeout is enforced with `CancelAfter` on the job's token and recorded as an
ordinary error, so it gets retried.

**One token, three causes.** `WorkAsync` gets a single `CancellationToken`, linked from remote cancel, the timeout and
hard shutdown. The executor knows which source fired and records the outcome to match: cancelled, errored, or put back
to available. The worker only has to honor the token (or not).

## Processor

All of these are internal and started by one `OxbowHostedService : IHostedService`.

**Notifier.** One dedicated connection from the data source runs `LISTEN` on `{s}.{p}insert`, `{s}.{p}control` and
`{s}.{p}leadership`, loops on `WaitAsync`, and reconnects with backoff. Subscribers are in-process callbacks. With
`o.PollOnly = true` (for PgBouncer in transaction mode), there's no notifier and everything falls back to the poll loops.

**Producer, one per queue** (River's `producer.go`). A single-reader `Channel` event loop owns all mutable state, so the
producer needs no locks:

- a fetch trigger, debounced to `FetchCooldown` (100 ms), fed by insert NOTIFYs for this queue, a jittered poll every
  `FetchPollInterval` (1 s), freed slots, and resume;
- a fetch locks `min(MaxWorkers - active, …)` rows with River's `JobGetAvailable` (`FOR UPDATE SKIP LOCKED`, ordered by
  `priority, scheduled_at, id`, by queue only, so the scan stays on `{p}job_fetch_idx`), sets them to `running`,
  increments `attempt` and appends the node ID to `attempted_by` (capped at 100). Queues are the routing boundary: a
  node that shouldn't take some work doesn't run that queue;
- each job runs on its own task with its own linked `CancellationTokenSource`;
- cancel requests come in by NOTIFY (`{action:"cancel", job_id, queue}`), with a poll of `cancel_attempted_at` for
  running IDs as backup (every 1 min, or 2 s in poll-only mode). A cancel that arrives mid-fetch is held and applied
  once the job starts, as in River;
- pause and resume come from control NOTIFYs plus a 2 s poll of the `{p}queue` row. While paused, the producer doesn't
  fetch, and running jobs finish;
- on start, the producer upserts its `{p}queue` row (River's `QueueCreateOrSetUpdatedAt`).

**Executor.** It creates a scope, resolves `IWorker<TArgs>`, deserializes the args, builds the middleware pipeline, runs
the worker, maps the outcome to a state update and hands it to the completer. Exceptions are caught, so a worker can
never take the host down. A row whose args fail to deserialize, or to decrypt, is recorded as a failed attempt and
retried, as in River. Encrypted args (the JSON string `"oxbow:enc:v1:…"`) are decrypted by `IJobArgsProtector` just before deserializing.
A kind with no registered worker is handled the River way: the attempt fails with `UnknownJobKindException` in `errors`
and retries with backoff. A rolling deploy heals itself, and a kind that's really gone ends up visibly `discarded` after
max attempts.

**Completer.** Outcomes are queued and written in batches through `JobSetStateIfRunningMany`: the completer flushes
every 50 ms or at 5,000 rows, makes workers wait when the backlog reaches 20,000, and retries a failed batch three
times. A job's slot is freed once its outcome is queued; the row stays `running` until the batch is written, so it
can't be fetched twice. Shutdown drains the queue. A node that dies with queued outcomes leaves those jobs to the
rescuer.

**Discard handlers.** When an `IJobDiscardHandler` is registered, the executor attaches a continuation to the
completer's write and, if the state written is `discarded` or `cancelled` (including a retry that `cancel_attempted_at`
turned into a cancellation), queues the job for its handlers. The rescuer and the scheduler queue their own discards
and cancellations on the leader. A background loop reads the final rows, then calls the handlers in a fresh scope per
job; the worker slot is long gone by then. Shutdown waits 5 s for queued calls after the completer drains, then
cancels and drops the rest. See [discard-handlers.md](discard-handlers.md).

**Elector** (River's `leadership/elector.go`). Every 5 s ±1 s, a follower runs `DELETE expired; INSERT … ON CONFLICT
(name) DO NOTHING RETURNING *` in one transaction. The TTL is the interval plus 10 s, and a leader trusts its term only
until TTL - 1 s, counted from when the attempt started. The leader re-elects with `UPDATE … WHERE leader_id = me AND
elected_at = term AND expires_at >= now()`. On stop it resigns with `DELETE` plus a NOTIFY on the leadership channel,
which wakes the followers at once.

**Maintenance** runs on the leader only. Each service starts when leadership is gained and stops when it's lost:

| Service | Default | Behavior (River parity) |
|---|---|---|
| Scheduler | every 5 s | Moves due `scheduled` and `retryable` jobs to `available`, discarding unique-key conflicts as River's `JobSchedule` does, then NOTIFYs the affected queues. |
| Rescuer | every 30 s, after 1 h | Takes `running` jobs whose later of `attempted_at` and `metadata.oxbow:heartbeat_at` is older than the horizon. If cancel was attempted, they become cancelled; if the attempt heartbeated, rescued; still inside their own timeout (when the leader knows the kind), left alone; otherwise retried with backoff or discarded by attempts. A kind the leader has no worker for is retried with the global policy, not discarded (see differences). Increments `metadata.oxbow:rescue_count`. |
| Job cleaner | every 30 s | Deletes `completed` older than 24 h, `cancelled` older than 24 h and `discarded` older than 7 d, in batches. A retention of `Timeout.InfiniteTimeSpan` keeps them forever. Skips members of a `{p}batch` row that still exists. |
| Queue cleaner | every 1 h | Deletes `{p}queue` rows not updated for 24 h. |
| Batch finisher | every 1 s | For each unfinished batch with no live member, one statement locks the batch row (`FOR UPDATE SKIP LOCKED`, rechecking `finalized_at IS NULL`), counts members, inserts the prepared callbacks and freezes the counts. See [batches](batches.md). |
| Batch cleaner | every 30 s | Deletes batches finished more than `BatchRetention` (24 h) ago whose callback jobs are finalized or gone. |
| Periodic enqueuer | timer | See below. |
| Queue depth | every 10 s | `count(*)` by queue and state, published as an observable gauge. |

Maintenance runs as the runtime role, so River's reindexer is left out (`REINDEX` needs ownership).

**Periodic jobs.** These are registered in options: `PeriodicSchedule.Every(TimeSpan)` or `PeriodicSchedule.Cron(expr,
TimeZoneInfo?)` (Cronos), a constructor returning `IJobArgs` plus optional `InsertOptions`, and `runOnStart`. Only the
leader enqueues them. The next-run times live in memory and are recomputed when a node gains leadership, so a failover can
skip or repeat a run. As River's docs advise, pair periodic jobs with `Unique.ByPeriod` when repeats matter. Run-on-start
jobs are enqueued when the enqueuer starts, which on a failover means the new leader enqueues them too.

## Inserting

1. Resolve the defaults: options passed in, then the args' `GetInsertOptions()`, then the client's
   (`MaxAttempts` 25, `Priority` 1, `Queue` "default"). Validate priority (1..4), queue name, and tags (≤ 255 chars,
   River's regex).
2. `ScheduledAt` in the future means state `scheduled`, otherwise `available`.
3. Unique options: build the key and bitmask. Then, for an encrypted kind, encrypt the args into the envelope. The default states are available, completed, pending, retryable, running
   and scheduled. A custom set must include available, pending, running and scheduled, and `ByPeriod` must be at least 1 s.
4. The insert middleware pipeline runs over the batch.
5. Execute: `INSERT … ON CONFLICT (unique_key) WHERE … DO UPDATE SET kind = {p}job.kind RETURNING *, (xmax <> 0)`. A
   conflicting row is returned with `UniqueSkippedAsDuplicate = true`. Because the unique index enforces this, a race
   between two transactions makes the second wait for the first to commit or roll back. Uniqueness is never checked in
   application code.
6. NOTIFY `{s}.{p}insert` for each distinct queue that got an `available` row, on the same connection and inside the
   same transaction. As in River, a per-queue limiter sends at most one NOTIFY per fetch cooldown, since one wake-up
   makes the producer fetch everything available; a wake-up lost to a rolled-back transaction is covered by polling.

**Bulk insert** (`InsertManyAsync`) is one statement, River's `JobInsertFastMany` shape:
`INSERT … SELECT … FROM unnest(@args::jsonb[], @kinds::text[], @states::text[], …)` with the state cast in SQL
(`::{s}.{p}job_state`) and `unique_states` sent as `int[]` and cast to `bit(8)`. It supports `ON CONFLICT … RETURNING`, so
unique jobs work and rows come back, and none of it touches the consumer's Npgsql type mappings. Tags travel as one
comma-joined string per row and are split with `string_to_array` in SQL, as River does, which avoids a two-dimensional
array parameter. It runs on the caller's transaction when one is given.

`InsertManyFastAsync` is the COPY path: binary COPY straight into the job table, returning only a count. The state
is written by its type name (`{s}.{p}job_state`), which Npgsql supports for unmapped enums, so it needs neither the
consumer's type mappings nor a temp staging table, and runs under the least-privilege role. COPY can't skip
duplicates, so unique jobs are rejected with a pointer to `InsertManyAsync`. Insert middleware runs with an empty
result list.

## Middleware

```csharp
public interface IJobWorkMiddleware   { Task InvokeAsync(JobContext context, JobWorkDelegate next); }
public interface IJobInsertMiddleware { Task<IReadOnlyList<InsertResult>> InvokeAsync(JobInsertContext context, JobInsertDelegate next); }
```

These follow the ASP.NET Core shape. Work middleware is resolved from the job's scope, so tenancy or a `DbContext`
middleware gets the same scoped instances as the worker. Insert middleware can change metadata, tags, queue and priority
before the insert runs. They run outermost-first, in registration order. Logging, tenancy and tracing propagation are
left to users, with samples.

## Shutdown

`IHostedService.StopAsync(ct)` stops all producers fetching, sends no new work, and resigns leadership. Running jobs get
until `ct` fires (the host's `ShutdownTimeout`, 30 s by default). Then their tokens are cancelled as a **soft stop**,
which returns those jobs to `available` without spending an attempt. Oxbow waits briefly for their completions to flush,
then stops the completer and the notifier. Calling `IHostApplicationLifetime.StopApplication` from inside a worker is
safe. Any job that doesn't report back is left `running`, and the rescuer picks it up later.

## Observability

- `ILogger<T>` with `[LoggerMessage]` source generation.
- An `ActivitySource("Oxbow")` with spans `oxbow.insert` (per batch) and `oxbow.work` (per job). The insert stores
  `traceparent` in the job's metadata, and the work span links to it, a link rather than a parent, because the job can
  run hours later.
- A `Meter("Oxbow")` with:
  - counters `oxbow.jobs.inserted` and `oxbow.jobs.attempts{queue,kind,outcome}`;
  - histograms `oxbow.job.duration` and `oxbow.job.queue_wait`;
  - the observable gauge `oxbow.queue.depth{queue,state}`.
- No OpenTelemetry SDK dependency.
- In-process time comes from `TimeProvider`. SQL uses the database's `now()` for fetching, scheduling and
  finalizing; app time is used for computed future times (retries, snoozes), as in River.

## Testing package

- `new OxbowAssert(schema, prefix)` with `InsertedAsync<TArgs>`, `NotInsertedAsync<TArgs>` and `FindAsync<TArgs>`,
  each taking a connection, an optional transaction and an `InsertedJobFilter<TArgs>`. They don't depend on a test
  framework and throw `OxbowAssertionException`.
- `WorkerHarness<TArgs, TWorker>`: runs a worker inline with a real DI scope, the registered work middleware and the
  worker's timeout, using an in-memory `Job<TArgs>`, and returns a `WorkResult` (`Completed`, `Errored`, `Snoozed`,
  `Cancelled`, `TimedOut`). No database is involved.

## Not yet built

See [roadmap.md](roadmap.md).

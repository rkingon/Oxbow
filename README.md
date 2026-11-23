# Oxbow

A durable, Postgres-backed job queue for .NET, modeled on [River](https://riverqueue.com). Its reason to exist:
**insert jobs in your own transaction**, so a job commits or rolls back with the data it belongs to.

> **Alpha.** The API may change before 1.0. See [CHANGELOG.md](CHANGELOG.md).

- Transactional enqueue on your `NpgsqlConnection`/`NpgsqlTransaction`
- Typed jobs and DI-resolved workers, args serialized with System.Text.Json
- Retries with exponential backoff and an errors history; snooze and self-cancel
- Job output stored with the completion; discard handlers for jobs that are discarded or cancelled
- Cancel a queued job, or signal a running one on any node through its `CancellationToken`
- Unique jobs enforced by a unique index: by args, period, queue and state
- Scheduled and periodic (interval or cron) jobs
- Batches: insert a group of jobs atomically, with callback jobs when it completes or succeeds
- Named queues with their own concurrency; pause and resume at runtime
- LISTEN/NOTIFY wake-ups with polling fallback; `FOR UPDATE SKIP LOCKED` fetching
- Heartbeat and progress for long-running jobs
- Leader-elected maintenance: scheduler, stuck-job rescuer, cleaner
- Insert and work middleware, OpenTelemetry-ready `ActivitySource` and `Meter`
- Args encryption at rest, per kind or for all, on ASP.NET Core Data Protection
- **You own the migrations**, and the runtime works as a least-privilege role

## Quickstart

```shell
dotnet add package Oxbow --prerelease
dotnet add package Npgsql.DependencyInjection
```

**1. Add the schema to your migrations.** Oxbow never runs DDL itself.

```csharp
foreach (var migration in OxbowSchema.GetMigrations(fromVersion: 0))
    Console.WriteLine(migration.Sql);                 // paste into your migration tool
Console.WriteLine(OxbowSchema.GetGrants("app_role")); // what the runtime role needs
```

**2. Define a job and a worker.**

```csharp
[JobKind("send_email")]
public sealed record SendEmail(string To, string Subject) : IJobArgs;

public sealed class SendEmailWorker(IEmailSender email) : IWorker<SendEmail>
{
    public Task WorkAsync(Job<SendEmail> job, CancellationToken cancellationToken) =>
        email.SendAsync(job.Args.To, job.Args.Subject, cancellationToken);
}
```

**3. Register.**

```csharp
builder.Services.AddNpgsqlDataSource(connectionString);
builder.Services
    .AddOxbow(o => o.Queues["default"] = new QueueOptions { MaxWorkers = 50 })
    .AddWorker<SendEmail, SendEmailWorker>();
```

**4. Insert in your transaction.**

```csharp
await using var connection = await dataSource.OpenConnectionAsync();
await using var transaction = await connection.BeginTransactionAsync();

// ... write your own rows on the same connection ...
await oxbow.InsertAsync(connection, transaction, new SendEmail("ada@example.com", "Welcome"));

await transaction.CommitAsync(); // the email is sent only if this commits
```

## Workers

```csharp
public sealed class SyncWorker : IWorker<SyncAccount>
{
    public async Task WorkAsync(Job<SyncAccount> job, CancellationToken cancellationToken)
    {
        if (!await RemoteIsReadyAsync(cancellationToken))
            throw new JobSnoozeException(TimeSpan.FromMinutes(1));  // retry later, attempt not spent

        if (job.Args.AccountClosed)
            throw new JobCancelException("Account closed.");        // stop for good

        await SyncAsync(job.Args, cancellationToken);               // throw anything else to retry
    }

    public TimeSpan? GetTimeout(Job<SyncAccount> job) => TimeSpan.FromMinutes(10);
    public DateTimeOffset? GetNextRetry(Job<SyncAccount> job) => DateTimeOffset.UtcNow.AddSeconds(30);
}
```

The `CancellationToken` fires on timeout, on `CancelAsync` (from any node), and at shutdown. A job interrupted by
shutdown goes back to `available` without spending its attempt.

## Inserting

```csharp
await oxbow.InsertAsync(new SendEmail("ada@example.com", "Hi"), new InsertOptions
{
    Queue = "email",
    Priority = 2,                                    // 1 (highest) to 4
    MaxAttempts = 5,
    ScheduledAt = DateTimeOffset.UtcNow.AddHours(1),
    Tags = ["onboarding"],
    Unique = new UniqueOptions { ByArgs = true, ByPeriod = TimeSpan.FromHours(24) },
});

await oxbow.InsertManyAsync(users.Select(u => new InsertManyItem(new SendEmail(u.Email, "News"))));
await oxbow.InsertManyFastAsync(items);              // binary COPY: fastest, returns a count, no unique jobs
```

Per-kind defaults live on the args:

```csharp
[JobKind("charge")]
public sealed record Charge([property: UniqueKey] string InvoiceId, string TraceId) : IJobArgs
{
    public InsertOptions GetInsertOptions() => new() { Queue = "billing", Unique = new() { ByArgs = true } };
}
```

## Managing jobs and queues

`CancelAsync`, `RetryAsync`, `DeleteAsync`, `GetAsync`, `ListAsync` (filters and a cursor), `PauseQueueAsync`,
`ResumeQueueAsync` on `IOxbowClient`.

## Periodic jobs

```csharp
builder.Services.AddOxbow(o =>
{
    o.Queues["default"] = new QueueOptions();
    o.AddPeriodicJob(PeriodicSchedule.Cron("0 3 * * *"), () => new NightlyReport(), runOnStart: false);
    o.AddPeriodicJob(PeriodicSchedule.Every(TimeSpan.FromMinutes(5)), () => new Heartbeat());
});
```

Only the elected leader enqueues them. Pair them with `UniqueOptions.ByPeriod` if a leader failover must never
produce a duplicate run.

## Least privilege

Workers and clients run as a role that owns nothing and can create nothing. `OxbowSchema.GetGrants(role)` returns
exactly what it needs:

```sql
GRANT USAGE ON SCHEMA public TO app_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.oxbow_job, public.oxbow_queue, public.oxbow_leader TO app_role;
GRANT SELECT ON public.oxbow_migration TO app_role;
GRANT USAGE ON SEQUENCE public.oxbow_job_id_seq TO app_role;
GRANT EXECUTE ON FUNCTION public.oxbow_job_state_in_bitmask(bit(8), public.oxbow_job_state) TO app_role;
```

The test suite runs everything as such a role, next to a table with forced row-level security. Oxbow never calls
`SET`, never relies on `search_path`, and never touches your session's settings.

## Observability

Spans come from the `ActivitySource` named `Oxbow` (`oxbow.insert`, `oxbow.work`) and metrics from the `Meter` named
`Oxbow` (`oxbow.jobs.inserted`, `oxbow.jobs.attempts`, `oxbow.job.duration`, `oxbow.job.queue_wait`,
`oxbow.queue.depth`):

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource("Oxbow"))
    .WithMetrics(m => m.AddMeter("Oxbow"));
```

## Packages

| Package | |
|---|---|
| `Oxbow` | The client, processor and schema |
| `Oxbow.EntityFrameworkCore` | Insert in a `DbContext`'s transaction; schema helpers for EF migrations |
| `Oxbow.Testing` | Insert assertions and an inline worker harness |
| `Oxbow.DataProtection` | Encrypt args at rest with ASP.NET Core Data Protection |
| `Oxbow.Cli` | The `oxbow` dotnet tool: print or apply the schema |

## Docs

[docs/](docs/README.md) has a page per feature. [docs/design.md](docs/design.md) covers the schema, the state machine,
fetching, leader election and maintenance.
[docs/differences-from-river.md](docs/differences-from-river.md) lists every deliberate deviation from River.

## License

MIT

# Scheduled and periodic jobs

## Scheduled

```csharp
await oxbow.InsertAsync(new SendReminder(userId), new InsertOptions { ScheduledAt = DateTimeOffset.UtcNow.AddDays(1) });
```

A future `ScheduledAt` inserts the job as `scheduled`; the leader's scheduler makes it `available` once it's due
(checked every `SchedulerInterval`, 5 seconds by default).

## Periodic

Periodic jobs are enqueued by the elected leader. They're registered in code, or stored in the database and managed at
runtime. Either way, their run times are persisted in `{prefix}periodic_job`, so a restart or a change of leader
neither skips nor repeats a run.

### Registered in code

```csharp
services.AddOxbow(o =>
{
    o.Queues["default"] = new QueueOptions();
    o.AddPeriodicJob("refresh-rates", PeriodicSchedule.Every(TimeSpan.FromMinutes(15)), () => new RefreshRates());
    o.AddPeriodicJob("nightly-report",
        PeriodicSchedule.Cron("0 3 * * *", TimeZoneInfo.FindSystemTimeZoneById("Europe/London")),
        () => new NightlyReport(), runOnStart: true);
});
```

- The name keys the job's persisted state. Without one, the name is derived from the kind and the schedule
  (`refresh_rates every 00:15:00`), so changing the schedule starts a fresh history, and `runOnStart` fires again. Name
  jobs whose history matters. Several unnamed jobs with the same kind and schedule get `#2`, `#3`, … in registration
  order. To learn the kind, the leader calls `createArgs` once when it starts the enqueuer.
- The definition stays in code: args, options and schedule come from the leader's registration. When the leader's
  schedule differs from the stored one, the stored one is replaced and the next run is recomputed from now.
- They can be paused, resumed and listed at runtime like stored jobs. They can't be replaced at runtime. Deleting one
  that's still registered only resets its state, because the leader recreates the row.
- A row whose job is no longer registered on any leader is deleted after 24 hours. Leaders refresh the rows of their
  own jobs hourly.

### Stored in the database

```csharp
await oxbow.UpsertPeriodicJobAsync("invoice-acme", PeriodicSchedule.Cron("0 9 1 * *"), new SendInvoices("acme"),
    new InsertOptions { Queue = "billing" });

await oxbow.PausePeriodicJobAsync("invoice-acme");
await oxbow.ResumePeriodicJobAsync("invoice-acme");
await oxbow.DeletePeriodicJobAsync("invoice-acme");
var page = await oxbow.ListPeriodicJobsAsync(new PeriodicJobListQuery { Limit = 100, Cursor = previous?.NextCursor });
```

- Listing is paged by name, like `ListAsync`. Rows expose the schedule as stored (`CronExpression`, `TimeZoneId`,
  `Interval`) without parsing it, so reading never depends on the reader's time zone database.
- Stored as kind, args JSON and insert options. An overload takes a kind and a `JsonElement`, for admin tools that
  don't have the args type.
- `CreatePeriodicJobAsync` fails with `PeriodicJobExistsException` if the name is taken, so an admin tool can't
  overwrite a job by accident. `UpsertPeriodicJobAsync` creates or replaces. Replacing keeps the run history and any pause. A changed schedule
  restarts from its next occurrence, and an unchanged one keeps its next run.
- Validated when stored: the schedule, a time zone that can be found by ID on the leader (`FindSystemTimeZoneById`), the
  options as an insert would check them, and the args against their type when this node has a worker for the kind.
  The unknown-kind check applies as it does to `InsertAsync`. `ScheduledAt` isn't allowed.
- The leader sees changes at once through a NOTIFY on the control channel. Without it (`PollOnly`, or a lost
  notification), it re-reads the table every `SchedulerInterval`.
- Each run is inserted through the normal insert path, so insert middleware runs. When the leader has a worker for the
  kind, the args are deserialized to their type first, so the type's `GetInsertOptions()` defaults and `[UniqueKey]`
  properties apply. Otherwise the stored JSON is inserted as is: no type defaults, and `ByArgs` uniqueness uses the
  whole args object. Insert middleware then sees an args object that is not the job's args type; use
  `JobInsertParams.Kind`.

### Runs

- **Exactly once per run.** A run's job is inserted in the same transaction that advances the job's `next_run_at`.
  The update is guarded on the row as the leader read it, so when two nodes both believe they lead, only one inserts
  the run.
- **Catch-up.** After downtime, a job that missed several runs runs once, and its schedule then continues from now.
- **`runOnStart`** enqueues the first run at once, instead of at the schedule's first occurrence. It applies when the
  job has no persisted state yet, not on every start or change of leader.
- **Pausing.** Runs missed while a job is paused are skipped. On resume, the next run is the schedule's next occurrence.
- **Failures.** If a run's insert fails for a reason that won't fix itself (args that no longer deserialize, a
  middleware throwing, invalid options), the run is skipped and logged. A database outage delays the run until the
  leader can reach the database again. A stored schedule the leader can't parse gets no next run until it's replaced.
- Runs carry `oxbow:periodic_job` (the job's name) in their metadata. They're enqueued without the unknown-kind check,
  so the leader doesn't need their workers.
- Cron expressions have five fields, or six with seconds first. They're parsed by
  [Cronos](https://github.com/HangfireIO/Cronos), which handles time zones and daylight-saving transitions correctly.

### Mixed versions

The leader is elected from the nodes that run queues or periodic jobs. Insert-only clients can manage stored periodic
jobs, but they never enqueue them.

0.1.0-alpha.3 and earlier don't read `{prefix}periodic_job`. While one of them leads, it enqueues the jobs registered in
its own code from in-memory schedules and runs no stored jobs. It also records no run times. When a newer node takes
over, a code-registered job whose stored next run has passed gets one catch-up run, which can repeat a run the old
leader just made. Finish the rollout before relying on stored periodic jobs. During a rolling deploy that changes a
schedule in code, each leader syncs its own version of the schedule.

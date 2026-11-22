# Scheduled and periodic jobs

## Scheduled

```csharp
await oxbow.InsertAsync(new SendReminder(userId), new InsertOptions { ScheduledAt = DateTimeOffset.UtcNow.AddDays(1) });
```

A future `ScheduledAt` inserts the job as `scheduled`; the leader's scheduler makes it `available` once it's due
(checked every `SchedulerInterval`, 5 seconds by default).

## Periodic

```csharp
services.AddOxbow(o =>
{
    o.Queues["default"] = new QueueOptions();
    o.AddPeriodicJob(PeriodicSchedule.Every(TimeSpan.FromMinutes(15)), () => new RefreshRates());
    o.AddPeriodicJob(PeriodicSchedule.Cron("0 3 * * *", TimeZoneInfo.FindSystemTimeZoneById("Europe/London")),
        () => new NightlyReport(), runOnStart: true);
});
```

- Cron expressions have five fields, or six with seconds first. They're parsed by
  [Cronos](https://github.com/HangfireIO/Cronos), which handles time zones and daylight-saving transitions correctly.
- Only the elected leader enqueues periodic jobs. Next-run times are kept in memory and recomputed when a node becomes
  leader, so a failover can skip or repeat a run. If a repeat would be harmful, make the job unique by period:
  `new InsertOptions { Unique = new() { ByPeriod = TimeSpan.FromMinutes(15) } }`.
- `runOnStart` enqueues the job when the enqueuer starts, which happens again on each change of leader.
- Periodic jobs are enqueued without the unknown-kind check, so the leader doesn't need their workers.

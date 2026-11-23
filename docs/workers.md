# Workers

```csharp
[JobKind("resize_image")]
public sealed record ResizeImage(Guid ImageId, int Width) : IJobArgs;

public sealed class ResizeImageWorker(IImageStore images) : IWorker<ResizeImage>
{
    public async Task WorkAsync(Job<ResizeImage> job, CancellationToken cancellationToken)
    {
        var image = await images.GetAsync(job.Args.ImageId, cancellationToken);
        await images.SaveAsync(image.Resize(job.Args.Width), cancellationToken);
    }
}

services.AddOxbow(...).AddWorker<ResizeImage, ResizeImageWorker>();
```

- Each job gets a new DI scope, and the worker is resolved from it, so scoped services such as a `DbContext` work.
- `job.Args` is the deserialized args; `job.Id`, `job.Attempt`, `job.Errors`, `job.Metadata` and `job.Row` describe
  the stored job.
- **Kinds** come from `[JobKind("…")]` and are stored with the job. Never derive them from class names: renaming a
  class must not orphan queued jobs. To rename a kind, keep the old one in `Aliases`.
- Every args type needs `[JobKind]`, and every kind may be registered once; both are checked at startup.
- Inserting a kind that has no worker on this client throws, unless no workers are registered (an insert-only client)
  or `OxbowOptions.SkipUnknownKindCheck` is set.
- If a node fetches a job whose kind it has no worker for, the attempt fails with `UnknownJobKindException` and is
  retried with backoff. Mid deploy, this heals itself once a node with the worker picks it up; a kind that's really
  gone ends up `discarded`, with the error in its history.

## Outcomes

| The worker… | The job becomes |
|---|---|
| returns | `completed` |
| throws | `retryable` (or `available` if the retry is due soon), or `discarded` after `MaxAttempts` |
| throws `JobSnoozeException(delay)` | `scheduled` for `now + delay`; the attempt isn't spent; `metadata.snoozes` is incremented |
| throws `JobCancelException` | `cancelled`, with the error recorded; never retried |

## Timeouts

The default is `OxbowOptions.JobTimeout` (1 minute). Override it per worker:

```csharp
public TimeSpan? GetTimeout(Job<ResizeImage> job) => TimeSpan.FromMinutes(5);  // Timeout.InfiniteTimeSpan for none
```

A timeout cancels the job's `CancellationToken`; the attempt is recorded as a `TimeoutException` and retried.

## Heartbeat and progress

```csharp
public async Task WorkAsync(Job<ImportCsv> job, CancellationToken cancellationToken)
{
    for (var i = 0; i < batches.Count; i++)
    {
        await ImportAsync(batches[i], cancellationToken);
        await job.ReportProgressAsync(100.0 * (i + 1) / batches.Count, $"batch {i + 1} of {batches.Count}", cancellationToken);
    }
}
```

- `job.HeartbeatAsync()` records that the attempt is alive, as `oxbow:heartbeat_at` in metadata. It's throttled:
  calls within a tenth of `RescueStuckJobsAfter` (30 s at most) of the last write do nothing, so call it as often as
  is convenient.
- `job.ReportProgressAsync(percent, message)` writes `oxbow:progress` (`percent` 0–100, `message` of at most 1,000
  characters, `attempt`, `at`) and counts as a heartbeat; out-of-range values throw. It isn't throttled; every call is a write to the job's row, so report at a sensible
  cadence.
- Read them with `GetAsync` or `ListAsync`: `JobRow.Progress` and `JobRow.HeartbeatAt`. `job.Progress` is what this
  attempt last reported.
- Neither is reset when a new attempt starts, and both stay after the job finishes. They belong to the current execution
  when `Progress.At` and `HeartbeatAt` are at or after `AttemptedAt`. The attempt number alone isn't enough: a snooze
  or a shutdown hands the same number to the next execution.
- Writes only land while this attempt is the one running. After the job finishes, or is rescued and picked up again,
  they do nothing.
- **An attempt that heartbeats is judged by its heartbeat.** The [rescuer](maintenance.md) leaves it alone, however
  long it runs, until `RescueStuckJobsAfter` (1 h) passes without one. Then it's rescued even if it has no timeout or
  its timeout hasn't elapsed, so a job that heartbeats must keep doing so: no single step may run longer than
  `RescueStuckJobsAfter` without a heartbeat.
- Outside Oxbow's executor (a `Job<TArgs>` created in a unit test, or `WorkerHarness`) heartbeats do nothing and
  progress only updates `job.Progress`; `WorkResult.Progress` reports it.

## The cancellation token

It fires on timeout, when the job is cancelled with `CancelAsync` from any node, and at shutdown. Oxbow records what
fired: a timeout fails the attempt, a cancel request cancels the job, a shutdown returns it to `available` without
spending the attempt. A worker that ignores the token simply finishes, and its result stands.

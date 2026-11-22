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

## The cancellation token

It fires on timeout, when the job is cancelled with `CancelAsync` from any node, and at shutdown. Oxbow records what
fired: a timeout fails the attempt, a cancel request cancels the job, a shutdown returns it to `available` without
spending the attempt. A worker that ignores the token simply finishes, and its result stands.

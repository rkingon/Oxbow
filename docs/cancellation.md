# Cancellation

```csharp
var job = await oxbow.CancelAsync(jobId);
```

- A job that hasn't started (`available`, `scheduled`, `retryable`) becomes `cancelled` at once and never runs.
- A **running** job stays `running`, gets `metadata.cancel_attempted_at`, and its node is told through a NOTIFY on the
  control channel. That node cancels the job's `CancellationToken`. If the worker stops by throwing, the job ends
  `cancelled`; if it finishes anyway, it ends `completed`. The worker decides.
- Without LISTEN/NOTIFY (`PollOnly`), or if the notification is missed, each node also polls for cancel requests on
  its running jobs (every minute, or every 2 seconds in poll-only mode).
- If the running job's node dies, the rescuer later cancels it instead of retrying it, because of
  `cancel_attempted_at`.
- A finished job is returned unchanged.

A worker can cancel its own job permanently with `throw new JobCancelException("reason")`.

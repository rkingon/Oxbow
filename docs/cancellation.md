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

## Cancelling many jobs

```csharp
var result = await oxbow.CancelManyAsync(new JobFilter { Metadata = new JsonObject { ["tenant"] = "acme" } });
```

Every matching job is handled as above, in batches (see [managing jobs](managing-jobs.md)). `result.Cancelled` counts
the jobs cancelled at once, `result.CancelRequested` the running ones whose nodes were told to stop them.

Running jobs are signalled with one NOTIFY per queue per batch, carrying up to 300 job IDs
(`{action:"cancel", queue, job_ids}`), to stay under Postgres's 8,000-byte payload limit. Nodes on 0.1.0-alpha.3 only
understand the single-job payload, so in a mixed fleet they stop bulk-cancelled jobs at their next cancel poll, within a
minute.

A worker can cancel its own job permanently with `throw new JobCancelException("reason")`.

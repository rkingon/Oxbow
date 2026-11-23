# Maintenance and leader election

Nodes that run queues take part in leader election through the `{prefix}leader` table, as in River. Every 5 seconds a
follower tries to insert the single leader row (deleting it first if expired); the leader extends it every 5 seconds
with a 15-second TTL, and stops trusting its leadership a second before it could have expired. A leader that stops
cleanly deletes the row and NOTIFYs, so another node takes over within milliseconds rather than waiting for the TTL.

The leader runs:

| Service | Every | Does |
|---|---|---|
| Scheduler | `SchedulerInterval` (5 s) | Makes due `scheduled` and `retryable` jobs `available`, and wakes their queues |
| Rescuer | 30 s | Jobs `running` with no sign of life (start or heartbeat) for `RescueStuckJobsAfter` (1 h) and past their timeout: retried, discarded if out of attempts, or cancelled if a cancel was requested |
| Cleaner | 30 s | Deletes `completed` and `cancelled` jobs after 24 h and `discarded` after 7 days (configurable; `Timeout.InfiniteTimeSpan` keeps them) |
| Queue cleaner | 1 h | Deletes `{prefix}queue` rows unused for 24 h |
| Periodic enqueuer | on schedule | Enqueues periodic jobs |
| Queue depth | 10 s | Publishes `oxbow.queue.depth` |

## The rescuer

A job is a candidate once the later of its `attempted_at` and its last [heartbeat](workers.md#heartbeat-and-progress)
is older than `RescueStuckJobsAfter`. Then:

- If a cancel was requested, it's cancelled.
- If the attempt has heartbeated, it's rescued, regardless of its timeout (even `Timeout.InfiniteTimeSpan`): it
  promised heartbeats and stopped sending them for `RescueStuckJobsAfter`.
- Otherwise, if the leader has a worker for the kind, it's left alone while it may legitimately still be running: with
  no timeout (`Timeout.InfiniteTimeSpan`), or within its timeout counted from `attempted_at`.
- Anything else is retried with backoff, or discarded when out of attempts.

So a long job that heartbeats is never rescued while it's healthy, even when the leader can't know its timeout, and a
job with no timeout that dies after heartbeating is recovered instead of staying `running` forever. A job that never
heartbeats is judged as before.

In a fleet mixing versions, a leader on a release before heartbeats ignores them and judges every job by
`attempted_at` and its timeout.

A heartbeat that isn't a valid timestamp (`oxbow:` metadata keys are Oxbow's; don't write them at insert) is ignored.

The rescuer retries a job whose kind the leader has no worker for, rather than discarding it as River does: the leader
may simply not run that job's queue.

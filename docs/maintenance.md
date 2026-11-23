# Maintenance and leader election

Nodes that run queues take part in leader election through the `{prefix}leader` table, as in River. Every 5 seconds a
follower tries to insert the single leader row (deleting it first if expired); the leader extends it every 5 seconds
with a 15-second TTL, and stops trusting its leadership a second before it could have expired. A leader that stops
cleanly deletes the row and NOTIFYs, so another node takes over within milliseconds rather than waiting for the TTL.

The leader runs:

| Service | Every | Does |
|---|---|---|
| Scheduler | `SchedulerInterval` (5 s) | Makes due `scheduled` and `retryable` jobs `available`, and wakes their queues |
| Rescuer | 30 s | Jobs `running` longer than `RescueStuckJobsAfter` (1 h) and past their timeout: retried, discarded if out of attempts, or cancelled if a cancel was requested |
| Cleaner | 30 s | Deletes `completed` and `cancelled` jobs after 24 h and `discarded` after 7 days (configurable; `Timeout.InfiniteTimeSpan` keeps them), except members of a batch that still exists |
| Queue cleaner | 1 h | Deletes `{prefix}queue` rows unused for 24 h |
| Batch finisher | 1 s | Enqueues the callbacks of batches whose jobs have all finished, and marks them finished, in one statement ([batches](batches.md)) |
| Batch cleaner | 30 s | Deletes batches finished more than `BatchRetention` (24 h) ago whose callbacks have finished |
| Periodic enqueuer | on schedule | Enqueues periodic jobs |
| Queue depth | 10 s | Publishes `oxbow.queue.depth` |

A job with no timeout (`Timeout.InfiniteTimeSpan`) is never rescued, since it may legitimately still be running.

The rescuer retries a job whose kind the leader has no worker for, rather than discarding it as River does: the leader
may simply not run that job's queue.

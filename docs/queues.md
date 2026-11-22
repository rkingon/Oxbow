# Queues

```csharp
services.AddOxbow(o =>
{
    o.Queues["default"] = new QueueOptions { MaxWorkers = 100 };
    o.Queues["email"] = new QueueOptions { MaxWorkers = 10 };
});

await oxbow.InsertAsync(new SendEmail(...), new InsertOptions { Queue = "email" });
```

- Each node works only the queues it configures, up to `MaxWorkers` jobs at a time per queue. Queues are how you route
  work: a node that shouldn't run some jobs doesn't run their queue.
- A node with no queues is insert-only: it registers the client and runs nothing else.
- Within a queue, jobs run by priority (1 first), then `ScheduledAt`, then ID. Fetching uses
  `FOR UPDATE SKIP LOCKED`, so nodes never block each other or double-process a job.
- Inserts wake idle queues immediately through LISTEN/NOTIFY; every queue also polls every `FetchPollInterval`
  (1 second). Behind PgBouncer in transaction mode, set `PollOnly = true`.

## Pause and resume

```csharp
await oxbow.PauseQueueAsync("email");   // "*" pauses every queue
await oxbow.ResumeQueueAsync("email");
```

Pausing applies to every node: running jobs finish, no new ones start. It's stored in `{prefix}queue`, so a paused
queue stays paused across restarts and on nodes that start later.

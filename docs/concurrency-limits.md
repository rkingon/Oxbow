# Concurrency limits

Caps how many jobs sharing a key run at once in a queue: on each node (`Local`), across every node (`Global`), or both.

```csharp
services.AddOxbow(o => o.Queues["sync"] = new QueueOptions
{
    MaxWorkers = 50,
    ConcurrencyLimit = new() { Global = 5, Local = 2 },   // per key: 5 across the fleet, 2 per node
});

[JobKind("sync_account")]
public sealed record SyncAccount([property: PartitionKey] string CustomerId, string AccountId) : IJobArgs
{
    public InsertOptions GetInsertOptions() => new() { Queue = "sync" };
}
```

Here at most five `SyncAccount` jobs for one customer run at once, and other customers' jobs fill the remaining
workers.

## The key

A job's partition key comes from, in order:

1. `InsertOptions.PartitionKey` passed to the insert;
2. `PartitionKey` in the args' `GetInsertOptions()`;
3. the args' properties marked `[PartitionKey]`, as a JSON object sorted by property name, e.g.
   `{"customerId":"acme"}`. Top-level properties only; a marked property the serializer omits is left out.

Insert middleware can then read or replace `JobInsertParams.PartitionKey`. Keys are 1 to 255 characters. A job with
no key is never limited, even in a limited queue.

For a kind with [encrypted args](encryption.md), a key derived from `[PartitionKey]` properties is stored as the
protector's keyed hash (hex), so the column doesn't reveal the values; jobs with equal values still share it. A key you
set yourself is stored as given. Stored periodic jobs have no args type, so they only get a key from their options.

The key is on `JobRow.PartitionKey`, and `JobFilter.PartitionKey` lists, cancels, retries or deletes one partition's
jobs (see [managing jobs](managing-jobs.md)).

Keys are per queue, not per kind: two kinds in one queue that share a key share its limit, so "at most two jobs per
customer, whatever they are" is one key. To give a kind a limit of its own, prefix its key with the kind, or give it its
own queue.

## How it's enforced

Limits are enforced when jobs are fetched. A key at its limit isn't fetched, so its jobs stay `available` and spend no
attempt; the free workers go to other keys. Keys at their limit are filtered out before the fetch's scan limit, so a
large backlog for one key can't hide other keys' jobs behind it. The scan still reads past that backlog on each fetch,
so its cost grows with it: on Postgres 17, 200,000 jobs of a full key queued ahead add about 120 ms to every fetch of
the queue.

- **Local:** the producer counts its running jobs per key and sends them with the fetch. A job counts until its outcome
  is written, not just until its worker returns, so the node never has more `running` rows for a key than the limit.
- **Global:** the fetch counts `running` jobs per key in the database. Fetches for the queue are serialized by locking
  its `{prefix}queue` row in the same transaction, because two nodes fetching at once could otherwise both see room.
  Queues without a global limit don't take the lock.
- Queues with no `ConcurrencyLimit` keep the plain `FOR UPDATE SKIP LOCKED` fetch.

Because outcomes are written in batches, a finished job holds its key's slot until the completer writes it, about
50 ms. A slot freed on another node is picked up at the next fetch, at worst one `FetchPollInterval` later.

## Caveats

- A job left `running` by a crashed node holds its key's slot until the rescuer moves it, after
  `RescueStuckJobsAfter` (1 hour by default). For tight limits such as `Global = 1`, have workers call
  [`HeartbeatAsync`](workers.md#heartbeat-and-progress) and lower `RescueStuckJobsAfter` to a few minutes: a job that
  heartbeats is rescued once its heartbeats stop for that long, however long it runs, so a dead node's slot comes back
  quickly without long-running jobs being rescued.
- Configure the same `Global` value on every node working the queue; each node enforces the value it's configured with.
- Nodes on 0.1.0-alpha.3 (schema version 1 code) fetch without limits and don't take the queue lock, so during a rolling
  upgrade they can exceed a key's limit. They also insert jobs without a key. Limits hold once every node working the
  queue runs a release that supports them.
- Concurrency limits with `Global = 1` give mutual exclusion per key, not ordering: retries are fetched by priority,
  `ScheduledAt` and ID.

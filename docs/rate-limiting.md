# Rate limiting

Caps how many jobs start per period in a queue, across every node working it: for the whole queue (`RateLimit`), per
partition key (`PartitionRateLimit`), or both.

```csharp
services.AddOxbow(o => o.Queues["geocode"] = new QueueOptions
{
    RateLimit = new(1, TimeSpan.FromSeconds(1)),   // the provider allows one request a second
});
```

However many nodes work `geocode`, and through rolling deploys and scale-out, its jobs start at most once a second.
Jobs waiting their turn stay `available`: they spend no attempt and aren't snoozed.

## Burst

By default starts are evenly spaced at `Period / Limit`, so no window of `Period` sees more than `Limit` of them.
`Burst` lets up to that many start at once after the queue has been idle, then the rate takes over:

```csharp
RateLimit = new(100, TimeSpan.FromMinutes(1)) { Burst = 10 },   // 10 at once, then one every 600 ms
```

A window of `Period` that begins with a burst can see up to `Limit + Burst - 1` starts, so leave `Burst` at 1 when an
external limit is strict.

## Per key

`PartitionRateLimit` gives each [partition key](concurrency-limits.md#the-key) a rate of its own, for example per
customer API key:

```csharp
o.Queues["sync"] = new QueueOptions
{
    PartitionRateLimit = new(5, TimeSpan.FromSeconds(1)),    // each customer: 5 starts a second
    RateLimit = new(200, TimeSpan.FromSeconds(1)),           // and the queue as a whole: 200
    ConcurrencyLimit = new() { Global = 2 },                 // and at most 2 running per customer
};

[JobKind("sync_account")]
public sealed record SyncAccount([property: PartitionKey] string CustomerId, string AccountId) : IJobArgs
{
    public InsertOptions GetInsertOptions() => new() { Queue = "sync" };
}
```

A key at its rate is skipped and other keys' jobs fill the workers, as with concurrency limits. Jobs without a key
aren't limited by `PartitionRateLimit`, but do count against the queue's `RateLimit`. A job starts only when every limit
on its queue allows it.

## How it's enforced

Each limiter is [GCRA](https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm): a single timestamp, the
theoretical arrival time, is the whole state. The queue's is `rate_limit_tat` on its `{prefix}queue` row; per-key ones
are rows of `{prefix}rate_limit`. There's no refill job.

- A rate-limited queue fetches like a queue with a global [concurrency limit](concurrency-limits.md#how-its-enforced):
  the fetch locks the queue's `{prefix}queue` row, works out how many starts the limiters allow at that moment (capped
  by `Burst` and the free workers), fetches at most that many, and advances each limiter by the jobs it actually
  started, all in one transaction. Fetches for the queue are serialized, so two nodes can't both spend the same
  allowance.
- The clock is the database's `now()`, never the node's.
- A start is a job moving to `running`. Retries and snoozed jobs coming back count again; a job that fails or is
  cancelled has still spent its start. The limit is on starts, not on how many run at once or how long they take:
  combine it with a `ConcurrencyLimit` for that.
- When the queue's limiter has no allowance left, the fetch reports when the next start is due and the producer wakes
  then, rather than at the next poll, so rates above one per `FetchPollInterval` or `FetchCooldown` work. Until then
  it doesn't fetch the queue at all: inserts and finished jobs don't make it poll the database for nothing. A throttled
  key likewise schedules a wake-up for when it's next due, subject to `FetchCooldown`.
- The leader deletes per-key rows once they've caught up (a caught-up limiter and no row behave the same), every
  cleaner interval, so the table holds only recently active keys.
- Queues without a rate limit keep their fetch as it was.

## Throughput

Each fetch starts at most `Burst` jobs per limiter. With `Burst = 1`, every start is a fetch of its own, serialized on
the queue row, so at high rates raise `Burst` to let one fetch start several.

With `Burst = 1` a late start isn't made up: the next one is due an interval after it, not after when it should have
been. Each start is late by the time a wake-up and fetch take (a few milliseconds), so a busy queue runs slightly under
its limit, which matters only at high rates. `Burst = 2` lets the limiter catch up, at the cost of allowing two starts
close together.

Per-key wake-ups respect `FetchCooldown` (100 ms), since with many keys they'd otherwise fetch constantly. A key starts
at most `Burst` jobs per fetch, so a per-key rate above one per `FetchCooldown` needs `Burst` of at least
`rate × FetchCooldown` to be reached.

## Configuration across nodes

Limits are configured in code on each node, and the limiter's state is just a timestamp, so:

- Configure the same limits on every node working the queue. Nodes that disagree each enforce their own limit against
  the shared timestamp when they fetch, so the queue runs at a rate between the two.
- A changed limit applies to the next start, but starts made under the old one still count: the first start under the
  new limit can wait up to the old limit's `Burst × Period / Limit`.
- Removing a limit leaves nothing to clean up.
- Nodes on 0.1.0-alpha.3 (schema version 1 code), and nodes that don't configure the limit, fetch the queue without it
  and don't advance its limiter, so during a rolling upgrade the rate can be exceeded. Limits hold once every node
  working the queue runs a release that supports them with the same configuration. A leader on 0.1.0-alpha.3 doesn't
  delete caught-up per-key rows; they wait for a newer leader.

# Retries

A failed attempt is recorded in the job's `errors` (one entry per attempt: time, attempt number, `Type: message`,
stack trace), and the job is retried until it has used `MaxAttempts` (default 25), then `discarded`.

The default delay is River's: `attempt⁴` seconds with ±10% jitter, where `attempt` counts the job's errors including
this one. That's 1 s, 16 s, 81 s, 4 min, 10 min, … and about three weeks in total over 25 attempts.

Override it per worker:

```csharp
public DateTimeOffset? GetNextRetry(Job<SyncAccount> job) => DateTimeOffset.UtcNow.AddSeconds(30 * job.Attempt);
```

or for every job, by registering your own `IRetryPolicy`:

```csharp
services.AddSingleton<IRetryPolicy, LinearRetryPolicy>();
```

A retry time in the past falls back to the default policy, with a warning. A retry due within the scheduler interval
goes straight to `available`; later ones wait as `retryable` until the leader's scheduler promotes them.

`IOxbowClient.RetryAsync(id)` makes a finished or waiting job available now. If it had used all its attempts, it gets
one more.

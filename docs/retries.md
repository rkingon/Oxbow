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

Oxbow calls `GetNextRetry` while the worker’s scope is alive. If the callback throws, Oxbow logs the callback failure
and uses the configured retry policy. The job’s error history retains the original work failure.

A retry time in the past falls back to the default policy, with a warning.

## A delay chosen at run time

When the worker learns the delay while working (a vendor's 429 or 503 with `Retry-After`, a lock that frees at a known
time), it fails the attempt with that delay:

```csharp
catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.TooManyRequests)
{
    throw new JobRetryException(retryAfter, "payments.rate_limited", ex);
}
```

- It's a failed attempt: it counts toward `MaxAttempts`, and past the limit the job is `discarded` and
  [discard handlers](discard-handlers.md) run with `AttemptsExhausted`.
- The error recorded is `JobRetryException: {message}` with its stack trace. With an inner exception it's
  `{InnerType}: {message} ({inner message})` with the inner exception's stack trace, e.g.
  `HttpRequestException: payments.rate_limited (Response status code does not indicate success: 429 (Too Many Requests).)`.
- The next attempt is at exactly `now + delay`, with no jitter. `GetNextRetry` and `IRetryPolicy` aren't consulted for
  this attempt; later failures go back to them.
- A zero delay retries now; a negative one throws `ArgumentOutOfRangeException` when the exception is constructed. A
  delay past the year 9999 waits until 9999-01-01, as a snooze does.

Snoozing (`JobSnoozeException`) is the alternative when waiting isn't a failure: it doesn't spend an attempt or record
an error.

## Scheduling and manual retries

A retry due within the scheduler interval goes straight to `available`; later ones wait as `retryable` until the
leader's scheduler promotes them, whichever of the above chose the time.

`IOxbowClient.RetryAsync(id)` makes a finished or waiting job available now. If it had used all its attempts, it gets
one more.

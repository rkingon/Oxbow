# Graceful shutdown

Oxbow's hosted service follows the generic host's shutdown:

1. Every queue stops fetching, and the leader resigns.
2. Running jobs keep going until the host's shutdown deadline (`HostOptions.ShutdownTimeout`, 30 seconds by default).
3. At the deadline, their `CancellationToken`s are cancelled. A job that stops by throwing goes back to `available`
   **without spending its attempt**, so it runs again on the next node.
4. A job that ignores its token past a 10-second grace period stays `running`; the rescuer recovers it later.

```csharp
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromMinutes(2));
```

# Observability

## Tracing

`ActivitySource` named `Oxbow`:

| Span | Kind | Tags |
|---|---|---|
| `oxbow.insert` | Producer | `oxbow.jobs.count` |
| `oxbow.work` | Consumer | `oxbow.job.id`, `oxbow.job.kind`, `oxbow.job.queue`, `oxbow.job.attempt`, `oxbow.job.outcome` |

When an insert is traced, its `traceparent` is stored in the job's metadata, and the work span **links** to it. It's
a link rather than a parent because the work can happen hours later, on another node.

## Metrics

`Meter` named `Oxbow`:

| Instrument | Type | Tags |
|---|---|---|
| `oxbow.jobs.inserted` | counter | |
| `oxbow.jobs.attempts` | counter | `queue`, `kind`, `outcome` (`completed`, `errored`, `discarded`, `cancelled`, `snoozed`, `interrupted`) |
| `oxbow.job.duration` | histogram (s) | `queue`, `kind`, `outcome` |
| `oxbow.job.queue_wait` | histogram (s) | `queue`, `kind` |
| `oxbow.queue.depth` | gauge | `queue`, `state`; published by the leader only |

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource("Oxbow"))
    .WithMetrics(m => m.AddMeter("Oxbow"));
```

## Logging

Logs go through `ILogger` under the `Oxbow` category: warnings for failed attempts, information for leadership changes
and rescues, errors for database failures.

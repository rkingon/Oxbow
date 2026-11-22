# Benchmarks

Run them with:

```shell
dotnet run -c Release --project benchmarks/Oxbow.Benchmarks -- --filter '*'
```

They start Postgres 17 in a container (with `fsync=off` and `synchronous_commit=off`, so the numbers measure Oxbow
rather than the disk), install the schema, and use a no-op worker.

## Results

Apple M5 (10 cores), macOS 26.5, .NET 10.0.15, Postgres 17 in Docker Desktop, Oxbow 0.1.0-alpha.2-dev.

| Benchmark | Mean |
|---|---:|
| `InsertAsync`, own transaction | 457 µs |
| `InsertAsync`, caller's transaction (open, begin, insert, commit) | 441 µs |
| `InsertManyAsync`, 1,000 jobs | 7.6 ms (≈ 131,000 jobs/s; 1.3 MB allocated) |
| `InsertManyFastAsync` (binary COPY), 1,000 jobs | 5.3 ms (≈ 190,000 jobs/s; 0.37 MB allocated) |
| One node drains 10,000 jobs, `MaxWorkers = 100` | 9.95 s (≈ 1,000 jobs/s) |
| One node drains 10,000 jobs, `MaxWorkers = 1000` | 0.92 s (≈ 10,900 jobs/s) |

The drain time includes starting the host.

## Reading them

- **Per-queue throughput is bounded by `MaxWorkers / FetchCooldown`.** A queue fetches at most once per cooldown
  (100 ms by default, as in River), and each fetch takes up to the free slots. With 100 workers that's 1,000 jobs per
  second per node, whatever the jobs cost. For many short jobs, raise `MaxWorkers`; for long jobs the cooldown
  doesn't matter.
- Completions are batched (up to 5,000 per write, every 50 ms), so finishing a job costs no round trip of its own.
- `InsertManyAsync` is one `INSERT … SELECT FROM unnest(…)` statement, with unique-job support and rows returned.
  `InsertManyFastAsync` uses binary COPY: about 30% faster and a quarter of the allocations, but it returns only a
  count and rejects unique jobs.

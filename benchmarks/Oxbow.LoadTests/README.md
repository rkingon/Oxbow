# Oxbow load tests

From the repository root, publish the portable Release binary, then run a profile with Python 3 and Docker.

```sh
dotnet publish benchmarks/Oxbow.LoadTests -c Release -o benchmarks/Oxbow.LoadTests/bin/Release/publish
python3 benchmarks/Oxbow.LoadTests/run.py recovery
python3 benchmarks/Oxbow.LoadTests/run.py recovery --postgres postgres:16-alpine
python3 benchmarks/Oxbow.LoadTests/run.py capacity
python3 benchmarks/Oxbow.LoadTests/run.py burst
python3 benchmarks/Oxbow.LoadTests/run.py hot
python3 benchmarks/Oxbow.LoadTests/run.py soak --rate 1000 --minutes 480
```

To repeat the longer v1 qualification on a quiet Docker host, use these settings after the recovery and hot-key checks:

```sh
python3 benchmarks/Oxbow.LoadTests/run.py burst --burst-seconds 120
for run in 1 2 3; do
    python3 benchmarks/Oxbow.LoadTests/run.py soak --rate 1200 --minutes 30 || break
done
python3 benchmarks/Oxbow.LoadTests/run.py soak --rate 1200 --minutes 480
python3 benchmarks/Oxbow.LoadTests/run.py capacity --seconds 300
```

To test a merged candidate as a NuGet consumer, pack a clean product checkout into a local feed. From the harness checkout, publish with the same version and the absolute feed path, then run the profiles above.

```sh
# In the product checkout
version="1.0.0-dev.g$(git rev-parse --short HEAD)"
feed="$(pwd)/artifacts/dev-pack"
dotnet pack Oxbow.slnx -c Release -p:Version="$version" -o "$feed"

# In the harness checkout, with the same version and feed variables
dotnet publish benchmarks/Oxbow.LoadTests -c Release \
  -p:OxbowPackageVersion="$version" -p:RestoreAdditionalProjectSources="$feed" \
  -o benchmarks/Oxbow.LoadTests/bin/Release/publish
```

Each invocation creates a private Docker network, a Postgres container with its own volume, one container per worker, and separate producer containers. The Python controller runs on the host. It prints the artifact directory on startup and removes its containers, volume, and network on exit. The artifact directory keeps the producer receipts, sampled metrics, worker log tails, and JSONL summary. A failed check exits nonzero.

| Profile | Workload and checks |
|---|---|
| Recovery | At 250 jobs/s, kill the elected worker after it records a durable effect. Verify the same job records a second effect on its successor, its batch callback runs once, a scheduled job retries, and a rolled-back insert stays absent. Then stop a worker gracefully and stop/start Postgres during separate traffic phases. Reconcile failed producer calls by checking every sequence range. |
| Capacity | Run 1, 2, and 4 nodes with 100 default-queue workers each. Offer 600 and 1,200 jobs/s per node with single inserts, 50-item bulk inserts, and 50-item COPY. Bulk workers simulate 50 ms of I/O. Single and COPY workers return immediately. The default 10-second steps locate saturation quickly. Use `--seconds 300` for longer steps. Overloaded steps are expected and still have to drain correctly. The producer records offers it cannot dispatch at its 512-call limit as skipped. A capacity step with skipped offers cannot establish a sustainable rate. |
| Burst | One producer continuously changes its target from 1,000 jobs/s to 4,000 and back to 1,000, with two worker nodes. Sample the queue every two seconds and time its return to baseline while producers are active. |
| Hot key | Put 100,000 jobs ahead of an independent partition on two nodes. Measure both partitions separately. Check peak overlapping worker executions against a global cap of 64. Repeat on a queue with a per-key rate of 500/s and burst of 64, checking the rate envelope against database claim timestamps. The independent partition must stay below one second p99 in both runs. |
| Soak | On two nodes, rotate bulk, COPY, and single inserts in one-minute cohorts. Reconcile every cohort before its five-minute completed-job retention expires. Record memory, CPU, queue size, table size, dead tuples, and autovacuums. Each cohort must have p99 claim wait below one second and zero unexpected retries. |

The controller verifies that Postgres has `fsync` and `synchronous_commit` enabled. Each process has a 64-connection pool. Fetch cooldown is the default 100 ms. For repeatable fault injection, job timeout is 10 s and rescue eligibility is 12 s. Election and maintenance use their normal cadences.

Positive sequence numbers form the offered manifest for the fresh database. Producer receipts identify acknowledged and uncertain ranges. For every committed range the controller checks the distinct sequence count, final state, and worker output. This also works with COPY, which returns a count. The crash probe writes an independent side effect so a repeated execution remains visible after recovery.

Queue-wait p99 covers first attempts and measures the time from `scheduled_at` to the database claim, `attempted_at`. Insert p99 is per acknowledged API call. Offered-to-ack latency includes producer scheduling delay. The reported completion rate divides reconciled jobs by the interval from the first database creation timestamp through the last finalization timestamp, including backlog drain. Use the outstanding-work samples alongside that average to judge whether a rate is sustainable.

## v1 candidate measurements

Initial Docker runs on Oxbow `6f77a34`, Apple M5, Docker Desktop Linux arm64 with 10 CPUs and 7.65 GiB, .NET 10.0.15. Performance runs used Postgres 17.12. Recovery also ran on 16.16.

| Test | Observed result |
|---|---|
| Hard leader kill, graceful shutdown, database stop/start | Across Postgres 16 and 17, all 24,400 acknowledged background jobs completed. During the database outages 1,100 unacknowledged offers were absent after restart. On each version the crash probe recorded its side effect on both workers, its batch callback ran once, and the rolled-back job stayed absent. A new leader was observed within 17.3 s and the crash probe completed within 54 s with the shortened rescue settings. |
| 10-second bulk insert steps, 50 ms workers | With 1, 2, and 4 nodes at 600, 1,200, and 2,400 jobs/s respectively, p99 claim waits were 102, 102, and 116 ms. Doubling each offer pushed p99 to 2.1–2.2 s. At 4 nodes the overloaded run finalized about 3,965 jobs/s including drain. The 4-node single-insert producer skipped offers at both traffic levels. |
| 12-second burst on two nodes | A surge to 4,000 jobs/s grew outstanding work to a sampled 24,096 jobs. Within 25.1 s of returning to 1,000 admitted jobs/s the sample returned to 200 or fewer. All 96,000 completed on their first attempt. |
| 100,000 older jobs for one tenant | With a global concurrency limit of 64, observed overlap peaked at 64 and the other tenant's p99 was 101 ms. With a per-key rate of 500/s and burst of 64, the other tenant's p99 was **195.6 s**. All 202,000 jobs completed once and the rate envelope held. The rate-limited queue failed the one-second isolation target. |
| Independent rate-limit reproduction on Postgres 16.16 | At 50 jobs/s with no leading backlog the other tenant's p99 was 89.9 ms. Adding 10,000 older jobs for another partition raised it to **14.9 s**. All 11,250 jobs completed once. |
| 120-second burst on two nodes | The queue continued growing after arrivals dropped from 4,000 to 1,000 jobs/s and missed the subsequent 180-second drain deadline. A pre-timeout snapshot contained 601,681 completed rows, 248,519 available, and 300 running. The total matched the 850,500 acknowledged offers in aggregate. Another 1,500 offers were unacknowledged. Several unrelated workloads were consuming Docker CPU. Cause and per-job reconciliation remain open. |
| Sustained and eight-hour profiles at 1,200 jobs/s | Both stopped on the first minute's latency gate on the shared host. Each cohort completed all 72,000 jobs on its first attempt. P99 claim wait was 4.69 s and 3.16 s respectively. These runs provide no overnight retention or memory-growth measurement. |
| Five-minute capacity sweep | The first step offered 180,000 single inserts at 600/s. The producer skipped 85,255 offers at its in-flight limit. Another 5,704 received no acknowledgement and reconciliation found them absent. All 89,041 acknowledged jobs completed once, with p99 claim wait of 2.58 s. The sweep stopped at that step. |

## Targeted validation before merge

On Postgres 17, the fix author repeated the same two-node workload on `6f77a34` and `52b565d`: 10,000 older jobs for tenant A, then 200 for tenant B at 50/s. B's p99 claim wait fell from 19.457 s to 0.421 s. On the patched revision, all 200 B jobs started while A still had queued work. The original 100,000-job case remains part of qualification on the merged dev pack. [PR #15](https://github.com/rkingon/Oxbow/pull/15)

After independent review, PR #15 merged into `main` at `2c79ae9`. The Postgres 16 and 17 CI jobs passed on the merged revision. [Main CI](https://github.com/rkingon/Oxbow/actions/runs/45940592978)

## Deep-backlog investigation

On the original revision, the burst investigator reproduced the deadline miss with durable writes. The fetch still used an index scan. Turning off the queue-depth observations made little difference. The investigator measured durable commits at 7–23 ms while idle and about 38 ms under load. The same 852,000-job workload with synchronous commits disabled drained in time and reconciled every job exactly once. This control supports a storage-throughput explanation on this host. The investigation closed without a product change. Qualification continues with synchronous commits enabled.

On subsequent runs the harness records latency and admission gate failures while collecting later cohorts, and still exits unsuccessfully if any gate failed. After a missed burst drain deadline it allows a further bounded drain for per-ID reconciliation. This preserves the original deadline as a release gate.

## Merged dev pack

The candidate uses six local NuGet packages at `1.0.0-dev.g2c79ae9` from clean product revision `2c79ae99c4a5f9f28cfd9e20569f1578b856a860`. The baseline control uses `1.0.0-dev.g6f77a34`. Both use harness `3493452`. Each harness resolved Oxbow and Oxbow.Abstractions as packages. The published DLLs matched their NuGet archives byte for byte and both archives identified the expected product commit.

The first merged-package Postgres 16 reproduction passed. Behind 10,000 older jobs, the second tenant's p99 wait was 88.3 ms, compared with 14.865 s in the original shared-host run. All 11,250 offers completed once and the measured burst equivalent was 64. This preliminary result precedes the matched runs with host headroom.

| Merged-package retest | Result |
|---|---|
| Matched PG16 rate-limited tenant run, 10,000 older jobs | Second-tenant p99 claim wait was 17.081 s on the baseline and 96 ms on the candidate. All 1,000 second-tenant jobs completed once on each revision. Both held the configured burst of 64. |
| PG16 and PG17 recovery | Across leader kill, graceful stop and database restart, all 24,075 acknowledged background jobs completed. The 1,425 unacknowledged offers during the outages were absent on reconciliation. Each crash probe was rescued within 54.3 s, and its batch callback ran once. |
| Matched PG17 rate-limited tenant run, 100,000 older jobs | Second-tenant p99 claim wait fell from 197.034 s on the baseline to 99.1 ms on the candidate. The candidate measured 101.2 ms with no older backlog. Each revision completed all 101,250 jobs once, and both held the configured burst of 64. |
| Durable 120-second burst, matched baseline on PG17 | With spare CPU at startup, the original revision completed all 852,000 offered jobs once. Outstanding work peaked at 242,597 and returned to 200 or fewer 252.15 s after the burst while 1,000 jobs/s continued arriving. Unlike the earlier shared-host run, this control met the drain gate with synchronous commits enabled. |
| Durable 120-second burst, merged dev pack on PG17 | All 852,000 offers completed once. Outstanding work peaked at 240,918 and returned to 200 or fewer in 348.73 s while 1,000 jobs/s continued arriving. Both revisions passed. The candidate took 96.58 s longer in these sequential shared-host runs. |
| Global concurrency, two PG17 worker nodes | With 100,000 jobs queued for one tenant, its peak simultaneous executions across both nodes was exactly 64, the configured limit. The second tenant's p99 claim wait was 144.8 ms under contention and 114.9 ms alone. All 101,250 offers completed once. |
| First 30-minute sustained run at 1,200 offers/s, shared host | Failed admission and latency. All 2,117,220 acknowledged jobs completed once. The producer skipped 42,780 of 2,160,000 offers, all from single inserts. Seven of 30 cohorts exceeded 1 s p99, peaking at 2.261 s. The job table and indexes grew from 301 MiB at minute 8 to 538 MiB at minute 30; Postgres estimated 373,273 live tuples and 1,079 dead tuples after 29 autovacuums at that sample. |
| Second 30-minute sustained run at 1,200 offers/s | All 2,130,055 acknowledged jobs completed on the first attempt. The producer skipped 29,945 of 2,160,000 offers, all single inserts. Six of 30 cohorts exceeded the one-second p99 target; the worst was 4.493 s. At the minute-30 sample the job table and indexes occupied 531 MiB after 28 autovacuums. |
| Third sustained run, interrupted | The first 19 cohorts reconciled all 1,344,060 acknowledged jobs out of 1,368,000 offers. The producer skipped 23,940 single inserts and ten cohorts exceeded one second p99 (maximum 2.781 s). The twentieth producer wrote receipts for all 72,000 jobs, but its attached Docker client exceeded the harness's exit timeout. Store cleanup ended that run before the twentieth cohort could be reconciled. Its results are retained separately from the full retry. |
| Third sustained run, first retry | Minute 19 exceeded the 30-second drain deadline after all 72,000 offers were acknowledged. Its final outcomes could not be checked after cleanup. In the first 18 minutes all 1,274,341 acknowledged jobs completed once, 21,659 single inserts were skipped and four minutes exceeded one second p99 (maximum 1.580 s). The last resource sample showed 12,965 outstanding jobs across cohorts and 404 MiB of job storage. Median host CPU idle was 0%. |
| Third sustained run, extended-drain controller, interim through minute 8 | All 576,000 offers were acknowledged and completed on the first attempt. Every cohort met the one-second queue-wait target and the 30-second drain deadline. The highest cohort p99 was 104.2 ms. Median host CPU idle was 81%. |
| Eight-hour soak at 1,200 offers/s, first attempt | Stopped at minute 58 on the 30-second cohort drain deadline. In the first 57 cohorts, all 3,878,530 acknowledged IDs completed once out of 4,104,000 offers. The producer skipped 225,125 offers. Another 345 had uncertain insert results and were confirmed absent. Thirty-nine cohorts exceeded one second p99, peaking at 15.470 s. Minute 58 receipts recorded 60,050 acknowledged and 11,950 uncertain offers. Cleanup prevented per-ID verification of that cohort. Median host CPU idle was 0%. |

Job storage reached 194 MiB at minute 5, 382 MiB at minute 17, 512 MiB at minute 27 and 678 MiB at minute 37. It reached 817 MiB at minute 47 and 924 MiB at minute 56. PostgreSQL estimated 360,479 live tuples after 32 autovacuums at minute 37. A subsequent catalog sample measured 195 MiB for the job arguments GIN index, 159 MiB for the metadata GIN index and 87 MiB for the fetch index. From 08:40 to 08:50 UTC the arguments index grew from 249 to 297 MiB and the metadata index grew from 203 to 243 MiB. The table itself stayed at about 209 MiB during that interval. The individual index measurements are saved in `overnight-index-samples.jsonl` in the run directory. The retention and index-growth investigation is in Paseo workspace `wks_e405e763fc4d725b`, agent `40a77458-1a5f-4d10-a0e6-cf19b96950ce`.

The investigator reproduced cumulative GIN growth on PostgreSQL 17 with 60,000 retained rows, 20,000 inserts per cycle and an explicit vacuum each cycle. After 1.8 million inserts with distinct JSON values, the arguments GIN held 1,800,005 entries and occupied 128.9 MB. Rebuilding it reduced it to 60,005 entries and 4.7 MB. Reusing 5,000 distinct values kept the control index at 6.2 MB. The metadata GIN reached 107.4 MB with distinct outputs and shrank to 4.4 MB on rebuild. The proposed v3 migration drops both default GIN indexes. The metadata-filter performance tradeoff and migration are under review in [PR #16](https://github.com/rkingon/Oxbow/pull/16). These changes are outside the packaged candidate measured here.

For subsequent soak retries, the controller records a missed 30-second cohort drain as a failed gate and allows another 90 seconds for per-ID reconciliation. The next producer starts on the minute schedule while verification runs. The first eight-hour attempt used controller `16d56d9`. A fresh 480-minute attempt using `165baa4` is queued after capacity and the 30-minute retry, with the same packaged binaries and 1,200 offers/s target.

### Five-minute capacity sweep

Each step offered work for 300 seconds. Batched inserts and COPY used groups of 50. The handler delay was 50 ms for batched inserts and 0 ms for single inserts and COPY. Host idle is the median across host samples taken during producer sampling.

| Nodes | Insert method | Offered/s | Completed | Skipped | p99 queue wait (ms) | p99 offer to ack (ms) | Jobs/s incl. drain | Host idle (%) |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | Single | 600 | 180,000 | 0 | 336.9 | 272.5 | 600.4 | 3 |
| 1 | Single | 1,200 | 294,984 | 64,215 | 72,691.5 | 1,574.1 | 793.2 | 0 |
| 1 | Batched | 600 | 180,000 | 0 | 272.3 | 149.2 | 601.9 | 0 |
| 1 | Batched | 1,200 | 360,000 | 0 | 66,007.6 | 44.4 | 981.4 | 3.5 |
| 1 | COPY | 600 | 180,000 | 0 | 133.3 | 64.6 | 600.4 | 7.5 |
| 1 | COPY | 1,200 | 360,000 | 0 | 61,303.7 | 9.7 | 995.0 | 86 |
| 2 | Single | 1,200 | 359,300 | 700 | 189.1 | 281.9 | 1,198.0 | 21 |
| 2 | Single | 2,400 | 715,385 | 4,615 | 61,429.1 | 188.3 | 1,976.7 | 54.5 |
| 2 | Batched | 1,200 | 360,000 | 0 | 100.8 | 24.3 | 1,200.6 | 71 |
| 2 | Batched | 2,400 | 720,000 | 0 | 62,042.1 | 12.2 | 1,985.8 | 79.5 |
| 2 | COPY | 1,200 | 360,000 | 0 | 100.8 | 10.4 | 1,200.5 | 79 |
| 2 | COPY | 2,400 | 720,000 | 0 | 61,568.5 | 25.2 | 1,988.3 | 70 |
| 4 | Single | 2,400 | 711,745 | 8,255 | 659.8 | 263.0 | 2,369.3 | 37 |
| 4 | Single | 4,800 | 1,418,630 | 21,370 | 62,224.4 | 137.9 | 3,914.4 | 48 |
| 4 | Batched | 2,400 | 720,000 | 0 | 100.3 | 10.9 | 2,400.2 | 86.5 |
| 4 | Batched | 4,800 | 1,440,000 | 0 | 71,717.4 | 39.3 | 3,868.2 | 82 |
| 4 | COPY | 2,400 | 720,000 | 0 | 83.2 | 27.6 | 2,400.5 | 57 |
| 4 | COPY | 4,800 | 1,440,000 | 0 | 71,975.8 | 47.1 | 3,865.7 | 81 |

All 18 steps drained. Of 11,340,000 offers, 11,240,044 were acknowledged and completed on the first attempt. The producer skipped 99,155 single-insert offers. Another 801 uncertain offers from the one-node single-insert step at 1,200/s were confirmed absent. That step failed the producer gate. The 1,200/s one-node single-insert run spanned approximately 372 seconds from first creation to last completion.

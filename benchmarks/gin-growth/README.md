# GIN growth and metadata filter cost

Evidence for schema version 3, which drops the GIN indexes on `oxbow_job.args` and `oxbow_job.metadata`. Both scripts
start their own throwaway Postgres container (`IMAGE`, default `postgres:17-alpine`), remove it on exit, and need only
Docker and `python3`.

## `growth.sh`: GIN keeps every value it has ever indexed

```sh
./growth.sh                        # CYCLES=90 PER_CYCLE=20000 KEEP=60000 REPORT_EVERY=10
```

Each cycle inserts `PER_CYCLE` job-shaped rows, moves them to running and then completed (adding `metadata.output`),
deletes all but the newest `KEEP` rows and vacuums. Three tables differ only in value cardinality and opclass:

- `u`: a unique `seq` per job, `jsonb_ops` (Oxbow's version 1 and 2 indexes)
- `r`: `seq % 5000`, `jsonb_ops` (control)
- `p`: a unique `seq`, `jsonb_path_ops`

`u_fetch` is a `(state, id)` B-tree under the same churn. Output columns: cycle, index, bytes, GIN entry count from
the metapage.

Measured with the defaults (PostgreSQL 17.12, `--cpus=1 --memory=1g`, autovacuum off, 60,000 live rows in every
table at every report):

| Index | cycle 10 (200k inserted) | 30 | 50 | 70 | 90 (1.8M inserted) | after `REINDEX` |
|---|---|---|---|---|---|---|
| `u_args` bytes / entries | 17.7 MB / 200,005 | 42.2 MB | 66.7 MB | 97.8 MB | 128.9 MB / 1,800,005 | 4.7 MB / 60,005 |
| `u_meta` bytes / entries | 12.4 MB / 200,002 | 32.6 MB | 53.3 MB | 80.4 MB | 107.4 MB / 1,800,002 | 4.4 MB / 60,001 |
| `r_args` bytes / entries | 6.2 MB / 5,004 | 6.2 MB | 6.2 MB | 6.2 MB | 6.2 MB / 5,004 | 6.2 MB |
| `r_meta` bytes / entries | 2.6 MB / 5,002 | 2.6 MB | 2.6 MB | 2.6 MB | 2.6 MB / 5,002 | 2.6 MB |
| `p_args` bytes / entries | 11.4 MB / 199,997 | 22.4 MB | 35.0 MB | 45.4 MB | 58.3 MB / 1,799,631 | 3.5 MB / 60,002 |
| `p_meta` bytes / entries | 8.9 MB / 199,996 | 19.7 MB | 33.9 MB | 40.4 MB | 56.9 MB / 1,799,630 | 3.4 MB / 60,000 |
| `u_fetch` (B-tree) bytes | 11.49 MB | 11.49 MB | 11.49 MB | 11.49 MB | 11.49 MB | 11.49 MB |

With unique values, every value ever inserted stays an entry: about 78 bytes per job for args and 67 for metadata,
linear and independent of the 60,000 rows kept. `jsonb_path_ops` halves the rate but keeps the shape. Only `REINDEX`
returns the indexes to the size of the live rows. The Oxbow soak (1,200 jobs/s, five-minute retention) showed the same
rates: 204 MB args and 166 MB metadata GIN after about 2.5M jobs, over about 370,000 live rows.

GIN vacuum (`ginVacuumEntryPage`) removes dead TIDs from an entry's posting list but leaves the entry in place, even
with none left. Only posting-tree pages are reclaimed. The B-tree recycles emptied pages and holds steady.

## `filter-cost.sh`: `JobFilter.Metadata` with and without an index

```sh
./filter-cost.sh                   # ROWS=1000000 RUNS=5
```

Installs versions 1 and 2 from `src/Oxbow/Schema`, loads `ROWS` retained jobs, then measures the SQL `ListAsync` and
the bulk operations generate. Then it upgrades to version 3, measures again, rolls back, upgrades again and adds the
opt-in index from [docs/managing-jobs.md](../../docs/managing-jobs.md#indexing-metadata-filters). A row checksum is
compared at every step.

The data: 95% completed with an output, 3% available (the newest), 2% discarded; 20% in queue `email`;
`metadata.tenant` over 1000 tenants (0.1% of rows each); 10 rows with `{"import": "batch-7"}`.

- `list`: the `ListTemplate` page of 100 in ID order. Literal values, as Npgsql's unprepared commands plan them.
- `bulk`: the `DeleteMany` locked-CTE walk, run as `SELECT`: batches of 1000 in ID order up to the starting max ID.
- `warm`: the median of `RUNS` runs. `cold`: the first run after a container restart, so shared buffers are empty but
  the host page cache isn't. Cold times swing with host load; `buffers` (pages touched) doesn't depend on caching.

PostgreSQL 17.12, `--cpus=2 --memory=2g`, `shared_buffers=256MB`, 1,000,000 rows, 210 MB heap, on a busy host.

| Filter | v2 metadata GIN (fresh) | v3, no index | v3 + opt-in `jsonb_path_ops` |
|---|---|---|---|
| list, tenant (0.1%) | 7.6 ms, 1,289 buffers | 176.8 ms, 26,981 buffers (seq scan) | 1.8 ms, 1,008 buffers |
| list, rare (10 rows) | 0.1 ms, 20 buffers | 182.6 ms, 26,981 buffers (seq scan) | 0.1 ms, 17 buffers |
| list, `available` + `default` + tenant | 7.1 ms, 1,289 buffers | 14.6 ms, 963 buffers (`fetch_idx`) | 0.6 ms, 1,008 buffers |
| bulk, tenant (1000 matches, 2 batches) | 31.3 ms | 257.0 ms | 8.5 ms |
| bulk, rare | 4.5 ms | 205.3 ms | 4.7 ms |
| bulk, `available` + `default` + tenant | 22.2 ms | 12.1 ms | 6.9 ms |

Planning time stayed between 0.08 and 0.23 ms everywhere. Cold first runs ranged from 14 ms to 2.1 s with no
consistent ordering between configurations.

Migration steps on the same 1M rows: upgrade to 3 (two `DROP INDEX`) took 401 ms. Rolling back to 2 rebuilt both GINs
non-concurrently in 16.2 s, blocking writes. `CREATE INDEX CONCURRENTLY` of the opt-in index took 5.3 s and produced
45 MB, against 69 MB for the version 2 metadata GIN. Row count and checksums matched after every step.

### Not covered

- The version 2 GIN here is freshly built, which is its best case. After churn it carries the growth above.
- Only PostgreSQL 17 here. The migration test in `SchemaTests` runs on 16 and 17 in CI.
- Prepared statements (Npgsql `Max Auto Prepare`) can switch to generic plans. Not measured.
- `DROP INDEX` needs a brief `ACCESS EXCLUSIVE` lock and waits behind open transactions on the job table. Not measured
  under load.

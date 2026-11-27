#!/usr/bin/env bash
# JobFilter.Metadata cost with and without the metadata GIN, on the real schema loaded at version 2, across the
# version 3 upgrade, its rollback, and an operator-added jsonb_path_ops index. Also checks that rows survive each step.
set -euo pipefail

IMAGE=${IMAGE:-postgres:17-alpine}
NAME=${NAME:-oxbow-filter-cost}
ROWS=${ROWS:-1000000}
RUNS=${RUNS:-5}   # warm runs per query, after one run with cold shared buffers
HERE=$(cd "$(dirname "$0")" && pwd)
SCHEMA_DIR=$HERE/../../src/Oxbow/Schema

docker run -d --rm --name "$NAME" --cpus=2 --memory=2g -e POSTGRES_HOST_AUTH_METHOD=trust "$IMAGE" \
    -c shared_buffers=256MB -c work_mem=4MB -c jit=off >/dev/null
trap 'docker rm -f "$NAME" >/dev/null' EXIT
ready() { until docker exec "$NAME" psql -U postgres -qAtc 'select 1' >/dev/null 2>&1; do sleep 1; done; }
ready
psql() { docker exec -i "$NAME" psql -U postgres -v ON_ERROR_STOP=1 -qAt "$@"; }
render() { sed 's/{{schema}}/public/g; s/{{prefix}}/oxbow_/g' "$SCHEMA_DIR/$1"; }
cold() { docker restart "$NAME" >/dev/null; ready; }

render 001_initial.up.sql | psql
render 002_v2.up.sql | psql

# Retained rows: 95% completed (with an output), 3% available (the newest), 2% discarded; 20% in queue 'email'.
# metadata.tenant cycles over 1000 tenants (~0.1% of rows each); 10 rows carry a rare {"import": "batch-7"}.
psql <<SQL
INSERT INTO oxbow_job (state, finalized_at, args, kind, queue, metadata)
SELECT st::oxbow_job_state,
       CASE WHEN st = 'available' THEN NULL ELSE now() END,
       jsonb_build_object('order_id', i, 'customer_id', i % 50000),
       'kind_' || i % 10,
       CASE WHEN i % 5 = 0 THEN 'email' ELSE 'default' END,
       jsonb_build_object('tenant', 't' || i % 1000)
         || CASE WHEN st = 'completed' THEN jsonb_build_object('output', i) ELSE '{}' END
         || CASE WHEN i % ($ROWS / 10) = 1 THEN '{"import": "batch-7"}'::jsonb ELSE '{}'::jsonb END
FROM generate_series(1, $ROWS) i,
     LATERAL (SELECT CASE WHEN i > $ROWS * 0.97 THEN 'available' WHEN i % 50 = 7 THEN 'discarded' ELSE 'completed' END) s(st);
VACUUM ANALYZE oxbow_job;
CREATE FUNCTION bulk_walk(filter text) RETURNS TABLE (batches int, matched bigint, ms numeric) LANGUAGE plpgsql AS \$\$
DECLARE after bigint := 0; until bigint; n bigint; last bigint; t0 timestamptz := clock_timestamp();
BEGIN
    -- OxbowClient.ManyAsync: batches of 1000 in ID order up to the max ID at the start (DeleteMany's locked CTE).
    SELECT coalesce(max(id), 0) INTO until FROM oxbow_job;
    batches := 0; matched := 0;
    LOOP
        EXECUTE format('SELECT count(*), max(id) FROM (SELECT id FROM oxbow_job WHERE %s AND id > \$1 AND id <= \$2
                        AND state <> ''running'' ORDER BY id LIMIT 1000) s', filter) INTO n, last USING after, until;
        batches := batches + 1; matched := matched + n;
        EXIT WHEN n < 1000;
        after := last;
    END LOOP;
    ms := round(extract(epoch FROM clock_timestamp() - t0) * 1000, 1);
    RETURN NEXT;
END \$\$;
SQL

checksum() { psql -c "SELECT count(*), sum(hashtext(metadata::text)::bigint), sum(hashtext(args::text)::bigint) FROM oxbow_job"; }
gins() { psql -c "SELECT coalesce(string_agg(i.relname || ':' || x.indisvalid || ':' || pg_relation_size(i.oid), ' ' ORDER BY i.relname), 'none')
                   FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid JOIN pg_am am ON am.oid = i.relam
                   WHERE x.indrelid = 'oxbow_job'::regclass AND am.amname = 'gin'"; }
marker() { psql -c "SELECT max(version) FROM oxbow_migration"; }

TENANT="metadata @> '{\"tenant\": \"t42\"}'"
RARE="metadata @> '{\"import\": \"batch-7\"}'"
SCOPE="state = ANY('{available}'::oxbow_job_state[]) AND queue = ANY('{default}')"
# The ListTemplate shape: a page of 100 in ID order, literal values as Npgsql's unprepared commands give the planner.
list() { echo "SELECT * FROM oxbow_job WHERE $1 AND (NULL::bigint IS NULL OR id > NULL) ORDER BY id ASC LIMIT 100"; }

measure() { # label sql
    for r in $(seq 0 "$RUNS"); do echo "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) $2;"; done | psql | python3 -I "$HERE/summarize.py" explain "$1"
}

walk() { # label filter
    local cold_ms
    cold_ms=$(psql -c "SELECT ms FROM bulk_walk(\$q\$$2\$q\$)")
    for r in $(seq 1 "$RUNS"); do psql -c "SELECT batches || ' ' || matched || ' ' || ms FROM bulk_walk(\$q\$$2\$q\$)"; done \
        | python3 -I "$HERE/summarize.py" walk "$1" "$cold_ms"
}

suite() {
    echo "-- $1"
    cold; measure "list tenant (0.1%)" "$(list "$TENANT")"
    cold; measure "list rare (10 rows)" "$(list "$RARE")"
    cold; measure "list available+default+tenant" "$(list "$SCOPE AND $TENANT")"
    cold; walk "bulk tenant" "$TENANT"
    cold; walk "bulk rare" "$RARE"
    cold; walk "bulk available+default+tenant" "$SCOPE AND $TENANT"
}

echo "rows=$ROWS heap=$(psql -c "SELECT pg_size_pretty(pg_relation_size('oxbow_job'))") $(psql -c "SELECT version()" | cut -d' ' -f1-2)"
echo "v2 marker=$(marker) gin=$(gins) checksum=$(checksum)"
suite "version 2 (metadata GIN freshly built)"

start=$(python3 -c 'import time; print(time.time())')
render 003_v3.up.sql | psql
echo "upgrade to 3: $(python3 -c "import time; print(round((time.time() - $start) * 1000))") ms, marker=$(marker) gin=$(gins) checksum=$(checksum)"
psql -c "ANALYZE oxbow_job"
suite "version 3 (no GIN)"

start=$(python3 -c 'import time; print(time.time())')
render 003_v3.down.sql | psql
echo "rollback to 2: $(python3 -c "import time; print(round((time.time() - $start) * 1000))") ms, marker=$(marker) gin=$(gins) checksum=$(checksum)"
cold; measure "after rollback: list tenant" "$(list "$TENANT")"
render 003_v3.up.sql | psql
echo "re-upgrade to 3: marker=$(marker) gin=$(gins) checksum=$(checksum)"

# The documented opt-in: a jsonb_path_ops GIN serves @>, at about half the size of the default opclass.
start=$(python3 -c 'import time; print(time.time())')
psql -c "CREATE INDEX CONCURRENTLY oxbow_job_metadata_path_idx ON oxbow_job USING gin (metadata jsonb_path_ops)"
echo "opt-in index build: $(python3 -c "import time; print(round((time.time() - $start) * 1000))") ms gin=$(gins)"
psql -c "ANALYZE oxbow_job"
suite "version 3 + operator jsonb_path_ops GIN"

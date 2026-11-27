#!/usr/bin/env bash
# GIN entry retention under insert/complete/delete/vacuum churn: three copies of a job-shaped table differing only in
# value cardinality and GIN opclass. Prints index bytes and the GIN metapage's entry count every REPORT_EVERY cycles.
set -euo pipefail

IMAGE=${IMAGE:-postgres:17-alpine}
NAME=${NAME:-oxbow-gin-growth}
CYCLES=${CYCLES:-90}            # cycles to run
PER_CYCLE=${PER_CYCLE:-20000}   # jobs inserted per cycle and table
KEEP=${KEEP:-60000}             # rows kept after each cycle's delete (the retention window)
REPORT_EVERY=${REPORT_EVERY:-10}

docker run -d --rm --name "$NAME" --cpus=1 --memory=1g -e POSTGRES_HOST_AUTH_METHOD=trust "$IMAGE" \
    -c autovacuum=off -c shared_buffers=128MB >/dev/null
trap 'docker rm -f "$NAME" >/dev/null' EXIT
until docker exec "$NAME" psql -U postgres -qAtc 'select 1' >/dev/null 2>&1; do sleep 1; done
psql() { docker exec -i "$NAME" psql -U postgres -v ON_ERROR_STOP=1 -qAtF' ' "$@"; }

# u: unique per-job values, jsonb_ops (Oxbow's v1/v2 indexes)
# r: values repeating with period 5000, jsonb_ops (control)
# p: unique per-job values, jsonb_path_ops
psql <<'SQL'
CREATE EXTENSION pageinspect;
CREATE TABLE u (id bigserial PRIMARY KEY, state text NOT NULL, args jsonb NOT NULL, metadata jsonb NOT NULL DEFAULT '{}');
CREATE TABLE r (id bigserial PRIMARY KEY, state text NOT NULL, args jsonb NOT NULL, metadata jsonb NOT NULL DEFAULT '{}');
CREATE TABLE p (id bigserial PRIMARY KEY, state text NOT NULL, args jsonb NOT NULL, metadata jsonb NOT NULL DEFAULT '{}');
CREATE INDEX u_args ON u USING gin (args);
CREATE INDEX u_meta ON u USING gin (metadata);
CREATE INDEX r_args ON r USING gin (args);
CREATE INDEX r_meta ON r USING gin (metadata);
CREATE INDEX p_args ON p USING gin (args jsonb_path_ops);
CREATE INDEX p_meta ON p USING gin (metadata jsonb_path_ops);
CREATE INDEX u_fetch ON u (state, id);
SQL

cycle() {
    for t in u r p; do
        seq='s'; [ "$t" = r ] && seq='s % 5000'
        cat <<SQL
INSERT INTO $t (state, args)
SELECT 'available', jsonb_build_object('seq', $seq, 'delay_ms', 0, 'mode', 'normal')
FROM generate_series((SELECT coalesce(max(id), 0) + 1 FROM $t), (SELECT coalesce(max(id), 0) + $PER_CYCLE FROM $t)) s;
UPDATE $t SET state = 'running' WHERE state = 'available';
UPDATE $t SET state = 'completed', metadata = metadata || jsonb_build_object('output', (args->>'seq')::bigint)
WHERE state = 'running';
DELETE FROM $t WHERE id <= (SELECT max(id) FROM $t) - $KEEP;
VACUUM $t;
SQL
    done
}

report() {
    psql -c "SELECT '$1', i, pg_relation_size(i::regclass),
                    CASE WHEN i LIKE '%fetch' THEN NULL ELSE (gin_metapage_info(get_raw_page(i, 0))).n_entries END
             FROM unnest(ARRAY['u_args','u_meta','r_args','r_meta','p_args','p_meta','u_fetch']) i"
    psql -c "SELECT '$1', 'live_rows', (SELECT count(*) FROM u), (SELECT count(*) FROM r), (SELECT count(*) FROM p)"
}

echo "cycle index bytes gin_entries"
for c in $(seq 1 "$CYCLES"); do
    cycle | psql
    if [ $((c % REPORT_EVERY)) -eq 0 ]; then report "$c"; fi
done
psql -c "REINDEX INDEX u_args; REINDEX INDEX u_meta; REINDEX INDEX p_args; REINDEX INDEX p_meta;"
report reindexed

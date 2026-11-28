#!/usr/bin/env bash
set -euo pipefail
export TMPDIR="$PWD/evidence/raw"
export TZ=UTC
{
  date -u
  uname -sm
  lscpu
  df -h .
  docker info --format '{{.NCPU}} CPUs {{.MemTotal}} bytes'
  docker ps --format '{{.Names}} {{.Image}}'
} > evidence/runner.txt
vmstat -w -t 5 > evidence/host.txt &
host_pid=$!
sample_pg() {
  local pg
  for ((i=0; i<90; i++)); do
    pg=$(docker ps --format '{{.Names}}' --filter 'name=oxbow-load-' | awk '/-pg$/ {print; exit}')
    if [[ -n "$pg" ]] && docker exec "$pg" pg_isready -U postgres >/dev/null 2>&1; then
      docker exec -i "$pg" psql -XqAt -U postgres <<'SQL'
SELECT clock_timestamp(), backend_type, state, coalesce(wait_event_type,'on CPU or runnable'), coalesce(wait_event,''), count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND (state IS DISTINCT FROM 'idle' OR backend_type='client backend') GROUP BY backend_type,state,wait_event_type,wait_event ORDER BY backend_type,state;
\watch 1
SQL
      return
    fi
    sleep 1
  done
}
sample_pg > evidence/postgres-waits.txt 2> evidence/sampler-errors.txt &
sampler_pid=$!
trap 'kill "$host_pid" "$sampler_pid" 2>/dev/null || true' EXIT
python3 -u "$RUNNER_TEMP/harness/benchmarks/Oxbow.LoadTests/run.py" soak --rate 1200 --minutes 30 2>&1 | tee evidence/profile.log

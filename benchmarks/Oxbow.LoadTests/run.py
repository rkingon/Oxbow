"""Run the Release build in disposable Docker containers. See README.md."""
import argparse
import json
import math
import subprocess
import statistics
import tempfile
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
DOTNET = "mcr.microsoft.com/dotnet/runtime:10.0"
DLL = "/app/Oxbow.LoadTests.dll"


def command(*args, **kwargs):
    result = subprocess.run(args, text=True, capture_output=True, timeout=120, **kwargs)
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed ({result.returncode}): {(result.stderr or result.stdout)[-2000:]}")
    return result.stdout.strip()


def wait_for(predicate, label, seconds=100, interval=0.25):
    begin = time.monotonic()
    while time.monotonic() - begin < seconds:
        if predicate():
            return round(time.monotonic() - begin, 3)
        time.sleep(interval)
    raise TimeoutError(label)


def require(test, message):
    if not test:
        raise RuntimeError(message)


class Run:
    def __init__(self, options):
        self.options = options
        self.name = "oxbow-load-" + uuid.uuid4().hex[:12]
        self.pg = self.name + "-pg"
        self.folder = Path(tempfile.mkdtemp(prefix=self.name + "-"))
        self.containers = []
        self.children = []
        self.phases = []
        self.workers = []
        self.failed_gates = set()
        self.next_seq = 1
        self.begin = time.monotonic()

    def report(self, event, **data):
        record = dict(event=event, elapsed=round(time.monotonic() - self.begin, 3), **data)
        print(json.dumps(record), flush=True)
        with (self.folder / "summary.jsonl").open("a") as out:
            print(json.dumps(record), file=out)

    def gate(self, condition, message):
        if not condition:
            self.failed_gates.add(message)
            self.report("gate-failed", reason=message)

    def start(self):
        command("docker", "network", "create", self.name)
        self.containers.append(self.pg)
        diagnostics = ["-c", "shared_preload_libraries=pg_stat_statements", "-c", "log_lock_waits=on",
                       "-c", "log_autovacuum_min_duration=0", "-c", "track_wal_io_timing=on"]
        command("docker", "run", "-d", "--name", self.pg, "--network", self.name,
                "--network-alias", "db", "-e", "POSTGRES_PASSWORD=loadtest", self.options.postgres,
                "-c", "max_connections=300", *(diagnostics if self.options.scenario == "burst" else []))
        wait_for(lambda: subprocess.run(["docker", "exec", self.pg, "pg_isready", "-U", "postgres", "-h", "127.0.0.1"],
                                        capture_output=True).returncode == 0, "Postgres startup")
        self.app("init")
        if self.options.scenario == "burst":
            self.sql("create extension pg_stat_statements")
        settings = self.sql("select version(), current_setting('fsync'), current_setting('synchronous_commit')")
        require(settings.endswith("|on|on"), settings)
        self.report("environment", postgres=settings, runtime=command("docker", "run", "--rm", DOTNET, "dotnet", "--list-runtimes"),
                    commit=command("git", "-C", str(HERE), "rev-parse", "HEAD"), artifacts=str(self.folder),
                    docker=command("docker", "info", "--format", "{{.NCPU}} CPU / {{.MemTotal}} bytes"))

    def sql(self, sql):
        return command("docker", "exec", "-i", self.pg, "psql", "-XqAt", "-v", "ON_ERROR_STOP=1",
                       "-U", "postgres", input=sql)

    def app(self, role, *args, background=False):
        ident = self.name + "-" + role + "-" + str(len(self.containers))
        self.containers.append(ident)
        cmd = ["docker", "run", "--ulimit", "core=0:0", "--name", ident, "--network", self.name,
               "-v", str(HERE / "bin/Release/publish") + ":/app:ro",
               "-e", "OXBOW_LOAD_DB=Host=db;Username=postgres;Password=loadtest;Database=postgres;Maximum Pool Size=64;Timeout=3",
               DOTNET, "dotnet", DLL, role, *map(str, args)]
        if role != "worker":
            cmd[2:2] = ["--rm"]
        if self.options.scenario == "soak":
            cmd[2:2] = ["-e", "OXBOW_LOAD_SHORT_RETENTION=1"]
        if role == "worker":
            command(*cmd[:2], "-d", *cmd[2:])
            return ident
        if not background:
            return command(*cmd)
        path = self.folder / (ident + ".jsonl")
        with path.open("w") as out, (self.folder / (ident + ".stderr")).open("w") as err:
            proc = subprocess.Popen(cmd, stdout=out, stderr=err)
        self.children.append(proc)
        return proc, path

    def worker(self, name):
        container = self.app("worker", name)
        self.workers.append(container)
        return container

    def cleanup(self):
        for ident in reversed(self.containers):
            if "-worker-" in ident or ident == self.pg:
                with (self.folder / (ident + ".log")).open("w") as out:
                    subprocess.run(["docker", "logs", "--tail", "200", ident], stdout=out, stderr=out)
            subprocess.run(["docker", "rm", "-fv", ident], capture_output=True)
        for proc in self.children:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                # The run-owned containers have been removed; release a lingering CLI attachment.
                proc.kill()
                proc.wait()
        subprocess.run(["docker", "network", "rm", self.name], capture_output=True)

    def phase(self, label, rate, seconds, batch=25, delay=50, api="many", queue="default", key="plain"):
        first = self.next_seq
        size = round(rate * seconds)
        self.next_seq += size
        proc, path = self.app("produce", first, size, rate, batch, delay, api, queue, key, background=True)
        phase = dict(label=label, first=first, size=size, rate=rate, queue=queue, proc=proc, path=path)
        self.phases.append(phase)
        return phase

    def queue_snapshot(self, queue):
        require(queue in ("default", "hot", "rate"), "Unknown test queue")
        count, timestamp = self.sql(f"""select count(*),extract(epoch from clock_timestamp())
            from oxbow_job where state in ('available','running') and queue='{queue}'""").split("|")
        return int(count), float(timestamp)

    def depth(self):
        return int(self.sql("select count(*) from oxbow_job where state in ('available','running') and queue='default'"))

    def sample(self, phase):
        deadline = time.monotonic() + phase["size"] / phase["rate"] + 120
        samples = []
        while phase["proc"].poll() is None:
            require(time.monotonic() < deadline, "Producer did not finish")
            depth, timestamp = self.queue_snapshot(phase.get("queue", "default"))
            samples.append((timestamp, depth))
            self.report("sample", phase=phase["label"], outstanding=depth, dbEpoch=timestamp)
            time.sleep(2)
        require(phase["proc"].returncode == 0, "Producer failed: " + str(phase["path"]))
        return samples

    def drain(self, seconds=180):
        return wait_for(lambda: self.sql("select exists(select from oxbow_job where finalized_at is null)") == "f",
                        "Queue did not drain", seconds, 2 if seconds > 180 else 0.25)

    def verify(self, phase, outage=False, allow_retries=False):
        if self.options.scenario != "soak":
            require(phase["proc"].wait(timeout=90) == 0, "Producer failed: " + str(phase["path"]))
        receipts = sorted((json.loads(line) for line in phase["path"].read_text().splitlines()
                           if line.startswith("{")), key=lambda r: r["start"])
        first = phase["first"]
        receipts = [r for r in receipts if first <= r["start"] < first + phase["size"]]
        end = first
        uncertain = []
        skipped = 0
        latencies = []
        for row in receipts:
            require(row["start"] == end and row["size"] > 0, "Gap or overlap in producer manifest")
            end += row["size"]
            if row["acknowledged"]:
                latencies.append(row["insertMs"])
            else:
                was_skipped = bool(row.get("skipped"))
                skipped += row["size"] if was_skipped else 0
                uncertain.append((row["start"], end, was_skipped))
        require(end == first + phase["size"], "Incomplete producer manifest")
        # Each uncertain call represents one atomic transaction. Resolve all its
        # logical IDs in one pass, including proving that skipped offers are absent.
        absent = 0
        if uncertain:
            ranges = ",".join(f"({a},{b},{str(skip).lower()})" for a,b,skip in uncertain)
            result = self.sql(f"""
                with ranges(a,b,skip) as (values {ranges}),
                expected as (select a,b,skip,generate_series(a,b-1) seq from ranges),
                resolved as (select a,b,skip,count(j.id) n from expected e left join oxbow_job j
                    on (j.args->>'seq')::int=e.seq group by a,b,skip)
                select coalesce(sum(b-a) filter(where n=0),0),
                    count(*) filter(where n not in (0,b-a) or (skip and n<>0)) from resolved
            """).split("|")
            absent, invalid = map(int, result)
            require(not invalid, "Partial ambiguous commit or unexpected execution of a skipped offer")
        unacknowledged = sum(b-a for a,b,_ in uncertain)
        stats = self.sql(f"""
            select count(*), count(distinct (args->>'seq')::int),
                count(*) filter (where state <> 'completed' or
                  (case when queue='hot' then metadata->'output'->0 else metadata->'output' end)
                       is distinct from args->'seq'),
                count(*) filter (where attempt > 1),
                round((1000 * percentile_cont(.99) within group (order by extract(epoch from attempted_at - scheduled_at))
                    filter (where attempt=1))::numeric, 1),
                round(extract(epoch from max(finalized_at)-min(created_at)), 3)
            from oxbow_job where (args->>'seq')::int >= {first} and (args->>'seq')::int < {end}
        """).split("|")
        count, distinct, bad, retried = map(int, stats[:4])
        require(count == distinct == phase["size"] - absent and bad == 0, "Manifest mismatch " + repr(stats))
        latencies.sort()
        offered_latencies = sorted(r["offeredToAckMs"] for r in receipts if r["acknowledged"])
        self.report("phase", label=phase["label"], offered=phase["size"], targetRate=phase["rate"],
                    acknowledged=phase["size"]-unacknowledged, completed=count,
                    producerSkipped=skipped, ambiguousAbsent=absent-skipped, ambiguousCommitted=unacknowledged-absent, retried=retried,
                    queueP99ms=float(stats[4]), jobsPerSecondIncludingDrain=round(count / float(stats[5]), 1),
                    offeredToAckP99ms=round(offered_latencies[math.ceil(.99*len(offered_latencies))-1], 1) if offered_latencies else None,
                    insertCallP99ms=round(latencies[math.ceil(.99*len(latencies))-1], 1) if latencies else None)
        self.gate(outage or unacknowledged == skipped, "Unexpected producer failure")
        self.gate(not skipped or self.options.scenario == "capacity", "Producer could not dispatch every offer")
        require(allow_retries or not retried, "Unexpected retried jobs")
        return float(stats[4])


def recovery(run):
    leader = run.worker("one")
    wait_for(lambda: run.sql("select leader_id from oxbow_leader") == "one", "Leader election")
    traffic = run.phase("leader-kill", 250, 25)
    wait_for(lambda: run.depth() > 0, "Background traffic")
    run.app("probe")
    wait_for(lambda: run.sql("""select exists(select from effects join oxbow_job on job=id
        where args->>'mode'='crash' and state='running' and node='one')""") == "t", "First crash effect")
    follower = run.worker("two")
    command("docker", "kill", leader)
    failover = wait_for(lambda: run.sql("select leader_id from oxbow_leader") == "two", "Follower election")
    rescue = wait_for(lambda: run.sql("select state from oxbow_job where args->>'mode'='crash'") == "completed",
                      "Crash probe rescue")
    run.sample(traffic)
    wait_for(lambda: run.sql("select count(*) from oxbow_job where args->>'mode'='callback' and state='completed'") == "1",
             "Batch callback after rescue", 15)
    run.drain()
    effects = run.sql("""select args->>'mode', attempt, state, (select string_agg(e.node, ',' order by e.attempt)
        from effects e where job=id) from oxbow_job where (args->>'seq')::int < 0 order by 1""")
    require(effects.splitlines()[0] == "callback|1|completed|two" and
            effects.splitlines()[1] == "crash|2|completed|one,two" and len(effects.splitlines()) == 3 and
            effects.splitlines()[2].startswith("retry|2|completed|"), effects)
    run.report("failover", electionSeconds=failover, rescueSecondsFromKill=round(failover+rescue, 3), probes=effects)
    run.verify(traffic, allow_retries=True)
    third = run.worker("three")
    traffic = run.phase("graceful-stop", 250, 8)
    wait_for(lambda: run.depth() > 0, "Traffic before stop")
    command("docker", "stop", "-t", "35", follower)
    require(command("docker", "inspect", "-f", "{{.State.ExitCode}}", follower) == "0", "Unclean shutdown")
    run.sample(traffic)
    run.drain()
    run.verify(traffic)
    traffic = run.phase("database-restart", 250, 18)
    wait_for(lambda: run.depth() > 0, "Traffic before database restart")
    command("docker", "stop", "-t", "10", run.pg)
    time.sleep(2)
    command("docker", "start", run.pg)
    wait_for(lambda: subprocess.run(["docker", "exec", run.pg, "pg_isready"], capture_output=True).returncode == 0,
             "Database restart")
    run.sample(traffic)
    run.drain()
    run.verify(traffic, outage=True, allow_retries=True)
    require(command("docker", "inspect", "-f", "{{.State.Running}}", third) == "true", "Worker exited")


def capacity(run):
    for nodes in sorted(set(run.options.nodes)):
        for node in range(len(run.workers), nodes):
            run.worker("node-" + str(node))
        for api, batch, delay in (("single", 1, 0), ("many", 50, 50), ("copy", 50, 0)):
            for rate in (int(600*nodes), int(1200*nodes)):
                phase = run.phase(f"{nodes}-nodes-{api}-{rate}", rate, run.options.seconds, batch, delay, api)
                run.sample(phase)
                run.drain()
                run.verify(phase)


def burst(run):
    for node in ("one", "two"):
        run.worker(node)
    segments = []
    seconds = run.options.burst_seconds
    duration_total = 8 + seconds + (3*seconds + 4)
    for label, rate, duration in (("baseline", 1000, 8), ("burst", 4000, seconds), ("recovery", 1000, 3*seconds + 4)):
        first = run.next_seq
        size = rate * duration
        run.next_seq += size
        run.phases.append(dict(label=label, first=first, size=size, rate=rate))
        segments.extend((first, size, rate, 50, 50, "many", "default", "plain"))
    proc, path = run.app("produce", *segments, background=True)
    for phase in run.phases:
        phase.update(proc=proc, path=path)
    samples = run.sample(dict(proc=proc, path=path, label="continuous-burst", size=duration_total, rate=1))
    try:
        run.drain()
    except TimeoutError:
        run.gate(False, "Queue missed the 180-second drain deadline")
        run.report("extended-drain", outstanding=run.depth())
        run.drain(1800)
    run.report("postgres", wal=run.sql("select wal_sync, wal_sync_time, wal_write, wal_write_time from pg_stat_wal"),
               fetch=run.sql("""select calls,rows,total_exec_time from pg_stat_statements
                   where query like 'WITH locked AS%' order by calls desc limit 1"""))
    for phase in run.phases:
        run.verify(phase)
    burst_start, burst_end = [float(run.sql(f"""
        select extract(epoch from min(created_at)) from oxbow_job
        where (args->>'seq')::int = {p['first']}""")) for p in run.phases[1:]]
    baseline = [depth for t,depth in samples if t < burst_start]
    threshold = max(max(baseline, default=0), 200)  # Two nodes, 100 worker slots each.
    recovered = next((t for (t,d), (_,d2) in zip(samples,samples[1:])
                      if t >= burst_end and max(d,d2) <= threshold), None)
    run.gate(recovered is not None, "Queue did not recover during baseline traffic")
    run.report("burst-drain", peakSampled=max(d for _,d in samples),
               baselineMedian=statistics.median(baseline) if baseline else None, threshold=threshold,
               secondsAfterBurst=round(recovered-burst_end, 2) if recovered is not None else None)


def hot(run):
    for node in ("one", "two"):
        run.worker(node)
    cold_p99 = {}
    for queue in run.options.hot_queues:
        control = run.phase(queue+"-control", 50, 5, 5, 50, "many", queue, "other")
        run.sample(control)
        run.drain()
        run.verify(control)
        backlog = run.phase(queue+"-backlog", run.options.hot_jobs, 1, 1000, 50, "copy", queue, "tenant")
        run.sample(backlog)
        cold = run.phase(queue+"-other", 50, 20, 5, 50, "many", queue, "other")
        run.sample(cold)
        run.drain(600)
        run.verify(backlog)
        cold_p99[queue] = run.verify(cold)
        if queue == "hot":
            # Both containers share the Docker VM's monotonic clock.
            peak = int(run.sql("""with events as (
                select (metadata->'output'->>1)::bigint t, 1 d from oxbow_job where queue='hot' and partition_key='tenant'
                union all select (metadata->'output'->>2)::bigint, -1 from oxbow_job where queue='hot' and partition_key='tenant'
                ), active as (select sum(d) over(order by t, d rows unbounded preceding) n from events)
                select max(n) from active"""))
            run.report("concurrency", peak=peak, limit=64)
            require(0 < peak <= 64, "Global concurrency cap exceeded")
        else:
            # GCRA inequality for every contiguous interval of starts at 2 ms/job.
            excess = float(run.sql("""with starts as (
                select row_number() over(order by attempted_at,id) - extract(epoch from attempted_at)*500 as balance
                ,attempted_at,id from oxbow_job where queue='rate' and partition_key='tenant'
                ), windows as (select balance-min(balance) over(order by attempted_at,id) as excess from starts)
                select max(excess) from windows"""))
            run.report("rate", maxBurstEquivalent=excess+1, limit=64)
            require(excess <= 63, "Per-key rate cap exceeded")
    require(max(cold_p99.values()) < 1000, f"Other-tenant p99 claim wait exceeded one second: {cold_p99}")


def soak(run):
    for name in ("one", "two"):
        run.worker(name)
    # Dispatch one-minute cohorts on a fixed wall-clock schedule. Reconcile each
    # while the next runs and before its five-minute retention expires.
    previous = None
    start = time.monotonic()
    for minute in range(run.options.minutes):
        time.sleep(max(0, start + minute*60 - time.monotonic()))
        api, batch, delay = (("many", 50, 50), ("copy", 50, 0), ("single", 1, 50))[minute % 3]
        phase = run.phase(f"minute-{minute+1}-{api}", run.options.rate, 60, batch, delay, api)
        if previous:
            check_cohort(run, previous)
        run.report("resources", minute=minute+1, outstanding=run.depth(),
                   containers=command("docker", "stats", "--no-stream", "--format",
                        "{{.Name}} {{.MemUsage}} {{.CPUPerc}}", run.pg, *run.workers),
                   pg=run.sql("""select n_live_tup,n_dead_tup,autovacuum_count,pg_total_relation_size(relid)
                       from pg_stat_user_tables where relname='oxbow_job'"""))
        previous = phase
    check_cohort(run, previous)
    run.drain()


def check_cohort(run, phase):
    # Docker's attached client may linger after the final insert returns.
    # Wait for the receipt stream, then verify its exact ranges and durable outcomes.
    offers = 0
    with phase["path"].open("rb") as stream:
        def receipts_ready():
            nonlocal offers
            status = phase["proc"].poll()
            while True:
                start = stream.tell()
                line = stream.readline()
                if not line.endswith(b"\n"):
                    stream.seek(start)
                    break
                if line.startswith(b"{"):
                    offers += json.loads(line)["size"]
            require(offers <= phase["size"], "Extra offers in cohort manifest")
            require(status in (None, 0), "Soak producer failed: " + str(phase["path"]))
            require(status is None or offers == phase["size"], "Incomplete cohort manifest")
            return offers == phase["size"]

        wait_for(receipts_ready, "Timed out waiting for soak receipts: " + str(phase["path"]), 100)
    first, end = phase["first"], phase["first"] + phase["size"]
    wait_for(lambda: run.sql(f"""select exists(select from oxbow_job where finalized_at is null
        and (args->>'seq')::int >= {first} and (args->>'seq')::int < {end})""") == "f", "Soak cohort drain", 30)
    p99 = run.verify(phase)
    run.gate(p99 < 1000, "Cohort exceeded the one-second queue-wait gate")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=["recovery", "capacity", "burst", "hot", "soak"])
    parser.add_argument("--postgres", choices=["postgres:17-alpine", "postgres:16-alpine"], default="postgres:17-alpine")
    parser.add_argument("--nodes", nargs="+", type=int, default=[1,2,4], choices=[1,2,4], help="Capacity worker counts")
    parser.add_argument("--seconds", default=10, type=int, help="Duration of each capacity step")
    parser.add_argument("--burst-seconds", type=int, default=12, help="Duration at 4000 jobs/second")
    parser.add_argument("--hot-jobs", type=int, default=100000, help="Size of the leading tenant backlog")
    parser.add_argument("--hot-queues", nargs="+", choices=["hot", "rate"], default=["hot", "rate"])
    parser.add_argument("--minutes", default=480, type=int, help="Soak duration")
    parser.add_argument("--rate", default=1000, type=int, help="Soak target in jobs/second")
    options = parser.parse_args()
    require(min(options.seconds, options.minutes, options.rate, options.hot_jobs, options.burst_seconds) > 0, "Step duration must be positive")
    run = Run(options)
    print("Artifacts:", run.folder, flush=True)
    try:
        run.start()
        {"recovery": recovery, "capacity": capacity, "burst": burst, "hot": hot, "soak": soak}[options.scenario](run)
        require(not run.failed_gates, ", ".join(sorted(run.failed_gates)))
        run.report("passed", scenario=options.scenario)
    except Exception as error:
        run.report("failed", scenario=options.scenario, reason=str(error))
        raise
    finally:
        run.cleanup()


if __name__ == "__main__":
    main()

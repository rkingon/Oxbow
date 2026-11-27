namespace Oxbow.Internal;

/// <summary>
/// Every runtime query, rendered once for a schema and prefix. Types are sent as text or primitives and cast in SQL so
/// the queries never depend on type mappings registered on the consumer's data source.
/// </summary>
internal sealed class OxbowSql
{
    public OxbowSql(string schema, string prefix)
    {
        Identifiers.Validate(schema, prefix);
        var job = $"{schema}.{prefix}job";
        var state = $"{schema}.{prefix}job_state";
        var bitmask = $"{schema}.{prefix}job_state_in_bitmask";
        var leader = $"{schema}.{prefix}leader";
        var queue = $"{schema}.{prefix}queue";
        var periodic = $"{schema}.{prefix}periodic_job";
        var batch = $"{schema}.{prefix}batch";
        var rateLimit = $"{schema}.{prefix}rate_limit";

        InsertChannel = $"{schema}.{prefix}insert";
        ControlChannel = $"{schema}.{prefix}control";
        LeadershipChannel = $"{schema}.{prefix}leadership";
        SequenceChannel = $"{schema}.{prefix}sequence";

        SchemaVersion = $"SELECT coalesce(max(version), 0) FROM {schema}.{prefix}migration";

        InsertMany = $"""
            INSERT INTO {job} (args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states, batch_id,
                sequence_key, sequence_continue_on, partition_key)
            SELECT
                i.args::jsonb,
                i.kind,
                i.max_attempts,
                i.metadata::jsonb,
                i.priority,
                i.queue,
                coalesce(i.scheduled_at, now()),
                i.state::{state},
                coalesce(string_to_array(nullif(i.tags, ''), ','), '{"{}"}')::varchar(255)[],
                i.unique_key,
                nullif(i.unique_states, 0)::bit(8),
                @batch_id::bigint,
                i.sequence_key,
                i.sequence_continue_on,
                i.partition_key
            FROM unnest(
                @args::text[], @kind::text[], @max_attempts::smallint[], @metadata::text[], @priority::smallint[],
                @queue::text[], @scheduled_at::timestamptz[], @state::text[], @tags::text[], @unique_key::bytea[],
                @unique_states::int[], @sequence_key::text[], @sequence_continue_on::smallint[], @partition_key::text[]
            ) AS i(args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states,
                sequence_key, sequence_continue_on, partition_key)
            ON CONFLICT (unique_key)
                WHERE unique_key IS NOT NULL
                  AND unique_states IS NOT NULL
                  AND {bitmask}(unique_states, state)
                -- A row is only returned on conflict if something is updated.
                DO UPDATE SET kind = {prefix}job.kind
            RETURNING {Columns(prefix + "job")}, (xmax <> 0) AS unique_skipped_as_duplicate
            """;

        CopyJobs = $"COPY {job} (args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, sequence_key, sequence_continue_on, partition_key) FROM STDIN (FORMAT BINARY)";
        StateTypeName = state;

        Notify = "SELECT pg_notify(@channel, payload) FROM unnest(@payloads::text[]) AS payload";

        GetAvailable = $"""
            WITH locked AS (
                SELECT id
                FROM {job}
                WHERE state = 'available'
                  AND queue = @queue
                  AND scheduled_at <= now()
                ORDER BY priority, scheduled_at, id
                LIMIT @max
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {job} AS j
            SET state = 'running',
                attempt = j.attempt + 1,
                attempted_at = now(),
                attempted_by = array_append(
                    CASE WHEN array_length(j.attempted_by, 1) >= 100
                        THEN j.attempted_by[array_length(j.attempted_by, 1) - 98:]
                        ELSE j.attempted_by
                    END,
                    @client_id)
            FROM locked
            WHERE j.id = locked.id
            RETURNING {Columns("j")}
            """;

        // Serializes fetches for a queue with a global limit. DO UPDATE locks the conflicting row even when its WHERE is
        // false, so this creates the row if it's missing but never writes a new version of it. It must be a statement of
        // its own: a statement's snapshot predates any lock it waits for, so a count taken in the same statement would
        // miss the jobs the previous holder just started.
        QueueLock = $"""
            INSERT INTO {queue} (name, updated_at)
            VALUES (@queue, now())
            ON CONFLICT (name) DO UPDATE SET updated_at = now() WHERE false
            """;

        // GetAvailable for a queue with a concurrency or rate limit. A key's room is the least of the global limit minus
        // its running jobs, the local limit minus this node's (sent by the producer), and the starts its rate limit
        // allows. A fetch takes at most `room` jobs per key, and the queue's rate limit caps the total.
        //
        // Rate limits are GCRA, run under the queue lock: with emission interval T and burst b, a limiter whose
        // theoretical arrival time is tat allows b - ceil((tat - t) / T) starts (tat no earlier than t), and each start
        // pushes tat T later. Only starts actually made advance it. t is statement_timestamp(), which this statement
        // takes after QueueLock returns; now() is the transaction's start, before the wait for the lock, so a fetch
        // that waited would judge itself by a time earlier than the previous holder's starts.
        GetAvailableLimited = $"""
            WITH RECURSIVE queue_rate AS (
                SELECT greatest(q.rate_limit_tat, statement_timestamp()) AS tat0
                FROM {queue} AS q
                WHERE @rate_interval::bigint IS NOT NULL
                  AND q.name = @queue
            ),
            allowed AS (
                SELECT least(@max::int, (
                    SELECT greatest(0, @rate_burst::int
                        - ceil(extract(epoch FROM tat0 - statement_timestamp()) * 1000000 / @rate_interval::bigint)::bigint)
                    FROM queue_rate)) AS n
            ),
            running AS (
                SELECT partition_key, count(*)::int AS n
                FROM {job}
                WHERE @global_limit::int IS NOT NULL
                  AND state = 'running'
                  AND queue = @queue
                  AND partition_key IS NOT NULL
                GROUP BY partition_key
            ),
            local_running AS (
                SELECT * FROM unnest(@local_keys::text[], @local_running::int[]) AS l(partition_key, n)
            ),
            -- Keys whose tat has passed have their full burst, so they need no row here.
            key_rate AS (
                SELECT partition_key, tat AS tat0,
                    greatest(0, @key_rate_burst::int
                        - ceil(extract(epoch FROM tat - statement_timestamp()) * 1000000 / @key_rate_interval::bigint)::bigint)::int AS allowance
                FROM {rateLimit}
                WHERE @key_rate_interval::bigint IS NOT NULL
                  AND queue = @queue
                  AND tat > statement_timestamp()
            ),
            room AS (
                SELECT partition_key,
                    least(
                        @global_limit::int - coalesce(r.n, 0),
                        @local_limit::int - coalesce(l.n, 0),
                        coalesce(k.allowance, @key_rate_burst::int)) AS room
                FROM running AS r
                FULL JOIN local_running AS l USING (partition_key)
                FULL JOIN key_rate AS k USING (partition_key)
            ),
            -- Walks the queue in order, @scan jobs a round, each round picking up where the last stopped and skipping
            -- keys that have used their room, until it has enough jobs or reaches the end. So a deep backlog with a
            -- little room left can't fill the scan ahead of every other key, yet is read only once. A key's rank counts
            -- its jobs taken in earlier rounds. Each full round takes at least one job (the first of each key in it), so
            -- there are at most @max + 1.
            scans (skip, ids, keys, after_priority, after_scheduled_at, after_id, done) AS (
                SELECT ARRAY(SELECT partition_key FROM room WHERE room <= 0), ARRAY[]::bigint[], ARRAY[]::text[],
                    0::smallint, '-infinity'::timestamptz, 0::bigint, (SELECT n FROM allowed) = 0
                UNION ALL
                SELECT s.skip || w.full_keys, s.ids || w.ids, s.keys || w.keys,
                    w.last_priority, w.last_scheduled_at, w.last_id,
                    w.scanned < @scan OR cardinality(s.ids) + cardinality(w.ids) >= (SELECT n FROM allowed)
                FROM scans AS s
                CROSS JOIN LATERAL (
                    SELECT count(*) AS scanned,
                        min(priority) FILTER (WHERE last) AS last_priority,
                        min(scheduled_at) FILTER (WHERE last) AS last_scheduled_at,
                        min(id) FILTER (WHERE last) AS last_id,
                        coalesce(array_agg(id) FILTER (WHERE fits), ARRAY[]::bigint[]) AS ids,
                        coalesce(array_agg(partition_key) FILTER (WHERE fits), ARRAY[]::text[]) AS keys,
                        coalesce(array_agg(DISTINCT partition_key) FILTER (WHERE partition_key IS NOT NULL AND total >= cap),
                            ARRAY[]::text[]) AS full_keys
                    FROM (
                        SELECT c.id, c.partition_key, c.priority, c.scheduled_at,
                            row_number() OVER (ORDER BY c.priority DESC, c.scheduled_at DESC, c.id DESC) = 1 AS last,
                            c.partition_key IS NULL OR coalesce(t.n, 0)
                                + row_number() OVER (PARTITION BY c.partition_key ORDER BY c.priority, c.scheduled_at, c.id) <= cap.n AS fits,
                            coalesce(t.n, 0) + count(*) OVER (PARTITION BY c.partition_key) AS total,
                            cap.n AS cap
                        FROM (
                            SELECT id, partition_key, priority, scheduled_at
                            FROM {job}
                            WHERE state = 'available'
                              AND queue = @queue
                              AND (priority, scheduled_at, id) > (s.after_priority, s.after_scheduled_at, s.after_id)
                              AND scheduled_at <= now()
                              AND (partition_key IS NULL OR partition_key <> ALL (s.skip))
                            ORDER BY priority, scheduled_at, id
                            LIMIT @scan
                        ) AS c
                        LEFT JOIN room USING (partition_key)
                        LEFT JOIN (SELECT k, count(*) AS n FROM unnest(s.keys) AS k GROUP BY k) AS t ON t.k = c.partition_key
                        CROSS JOIN LATERAL (SELECT coalesce(room.room,
                            least(@global_limit::int, @local_limit::int, @key_rate_burst::int), @max::int) AS n) AS cap
                    ) AS ranked
                ) AS w
                WHERE NOT s.done
            ),
            -- LIMIT after SKIP LOCKED, so a row someone else holds doesn't use up a slot.
            locked AS (
                SELECT j.id
                FROM {job} AS j
                WHERE j.id = ANY ((SELECT ids FROM scans WHERE done)::bigint[])
                  AND j.state = 'available'
                ORDER BY j.priority, j.scheduled_at, j.id
                LIMIT (SELECT n FROM allowed)
                FOR UPDATE OF j SKIP LOCKED
            ),
            fetched AS (
                UPDATE {job} AS j
                SET state = 'running',
                    attempt = j.attempt + 1,
                    attempted_at = statement_timestamp(),
                    attempted_by = array_append(
                        CASE WHEN array_length(j.attempted_by, 1) >= 100
                            THEN j.attempted_by[array_length(j.attempted_by, 1) - 98:]
                            ELSE j.attempted_by
                        END,
                        @client_id)
                FROM locked
                WHERE j.id = locked.id
                RETURNING {Columns("j")}
            ),
            queue_advance AS (
                UPDATE {queue} AS q
                SET rate_limit_tat = r.tat0 + interval '1 microsecond' * ((SELECT count(*) FROM fetched) * @rate_interval::bigint)
                FROM queue_rate AS r
                WHERE q.name = @queue
                  AND EXISTS (SELECT 1 FROM fetched)
            ),
            key_advance AS (
                INSERT INTO {rateLimit} (queue, partition_key, tat)
                SELECT @queue::text, f.partition_key,
                    coalesce(k.tat0, statement_timestamp()) + interval '1 microsecond' * (count(*) * @key_rate_interval::bigint)
                FROM fetched AS f
                LEFT JOIN key_rate AS k USING (partition_key)
                WHERE @key_rate_interval::bigint IS NOT NULL
                  AND f.partition_key IS NOT NULL
                GROUP BY f.partition_key, k.tat0
                ON CONFLICT (queue, partition_key) DO UPDATE SET tat = excluded.tat
            )
            SELECT * FROM fetched
            """;

        // Leads the limited fetch's batch. Its scan is recursive, which the planner costs at ten rounds, enough to JIT
        // compile a fetch of a long queue on every call; compiling costs more than any fetch saves.
        FetchWithoutJit = "SELECT set_config('jit', 'off', true)";

        // Run after GetAvailableLimited in its transaction: seconds until the queue's limiter next allows a start, and
        // until the first throttled key's does; NULL or negative when there's nothing to wait for. Relative to the
        // database's clock, so the producer's never meets it.
        RateLimitWait = $"""
            SELECT
                (SELECT extract(epoch FROM rate_limit_tat - statement_timestamp())::float8
                        - (@rate_burst::int - 1) * @rate_interval::bigint / 1e6::float8
                 FROM {queue}
                 WHERE @rate_interval::bigint IS NOT NULL
                   AND name = @queue),
                (SELECT extract(epoch FROM min(tat) - statement_timestamp())::float8
                        - (@key_rate_burst::int - 1) * @key_rate_interval::bigint / 1e6::float8
                 FROM {rateLimit}
                 WHERE @key_rate_interval::bigint IS NOT NULL
                   AND queue = @queue
                   AND tat > statement_timestamp() + interval '1 microsecond' * ((@key_rate_burst::int - 1) * @key_rate_interval::bigint))
            """;

        // A row whose tat has passed allows a full burst, exactly as no row does; the minute's margin keeps that true for
        // a fetch whose clock reads earlier than this transaction's. SKIP LOCKED leaves rows a fetch is upserting alone,
        // so the two can't deadlock.
        RateLimitDeleteExpired = $"""
            DELETE FROM {rateLimit}
            WHERE (queue, partition_key) IN (
                SELECT queue, partition_key FROM {rateLimit}
                WHERE tat < now() - interval '1 minute'
                LIMIT @max
                FOR UPDATE SKIP LOCKED)
            """;

        IsRunningAttempt = $"SELECT EXISTS (SELECT 1 FROM {job} WHERE id = @id AND state = 'running' AND attempted_at = @attempted_at)";

        // River's JobSetStateIfRunningMany: only a running job moves, and a retry, snooze or soft stop of a job whose
        // cancellation was requested becomes a cancellation instead.
        SetStateIfRunning = $"""
            WITH input AS (
                SELECT * FROM unnest(
                    @id::bigint[], @attempted_at::timestamptz[], @attempt_do_update::boolean[], @attempt::int[],
                    @errors_do_update::boolean[], @errors::text[], @finalized_at_do_update::boolean[], @finalized_at::timestamptz[],
                    @metadata_do_merge::boolean[], @metadata_updates::text[], @scheduled_at_do_update::boolean[],
                    @scheduled_at::timestamptz[], @state::text[]
                ) WITH ORDINALITY AS u(id, attempted_at, attempt_do_update, attempt, errors_do_update, errors,
                    finalized_at_do_update, finalized_at, metadata_do_merge, metadata_updates, scheduled_at_do_update,
                    scheduled_at, state, ordinal)
            )
            UPDATE {job} AS j
            SET
                attempt = CASE
                    WHEN input.attempt_do_update
                         AND NOT (input.state IN ('available', 'retryable', 'scheduled') AND j.metadata ? 'cancel_attempted_at')
                    THEN input.attempt ELSE j.attempt END,
                errors = CASE WHEN input.errors_do_update THEN array_append(j.errors, input.errors::jsonb) ELSE j.errors END,
                finalized_at = CASE
                    WHEN input.state IN ('available', 'retryable', 'scheduled') AND j.metadata ? 'cancel_attempted_at' THEN now()
                    WHEN input.finalized_at_do_update THEN input.finalized_at ELSE j.finalized_at END,
                metadata = CASE WHEN input.metadata_do_merge THEN j.metadata || input.metadata_updates::jsonb ELSE j.metadata END,
                scheduled_at = CASE
                    WHEN input.scheduled_at_do_update
                         AND NOT (input.state IN ('available', 'retryable', 'scheduled') AND j.metadata ? 'cancel_attempted_at')
                    THEN input.scheduled_at ELSE j.scheduled_at END,
                state = CASE
                    WHEN input.state IN ('available', 'retryable', 'scheduled') AND j.metadata ? 'cancel_attempted_at'
                    THEN 'cancelled'::{state} ELSE input.state::{state} END
            FROM input
            WHERE j.id = input.id AND j.state = 'running' AND j.attempted_at = input.attempted_at
            RETURNING input.ordinal::int - 1, j.state::text, j.sequence_key
            """;

        Cancel = $"""
            WITH locked AS (
                SELECT id, queue, state, finalized_at
                FROM {job}
                WHERE id = @id
                FOR UPDATE
            ),
            notification AS (
                SELECT id, pg_notify(@channel, json_build_object('action', 'cancel', 'job_id', id, 'queue', queue)::text)
                FROM locked
                WHERE state NOT IN ('cancelled', 'completed', 'discarded')
                  AND finalized_at IS NULL
            ),
            updated AS (
                UPDATE {job} AS j
                SET
                    -- A running job is cancelled by the producer working it; anything else is cancelled now.
                    state = CASE WHEN j.state = 'running' THEN j.state ELSE 'cancelled' END,
                    finalized_at = CASE WHEN j.state = 'running' THEN j.finalized_at ELSE now() END,
                    -- Also tells the rescuer to cancel rather than retry the job if it gets stuck.
                    metadata = jsonb_set(j.metadata, '{"{cancel_attempted_at}"}', to_jsonb(now()), true)
                FROM notification
                WHERE j.id = notification.id
                RETURNING {Columns("j")}
            )
            SELECT * FROM updated
            UNION ALL
            SELECT {Columns(prefix + "job")} FROM {job} WHERE id = @id AND NOT EXISTS (SELECT 1 FROM updated)
            """;

        Retry = $"""
            WITH locked AS (
                SELECT id FROM {job} WHERE id = @id FOR UPDATE
            ),
            updated AS (
                UPDATE {job} AS j
                SET
                    -- A sequence job rejoins the line; only promotion makes a sequence job live.
                    state = CASE
                        WHEN j.sequence_key IS NOT NULL AND j.state IN ('cancelled', 'completed', 'discarded', 'pending')
                        THEN 'pending'::{state}
                        ELSE 'available'::{state}
                    END,
                    max_attempts = CASE WHEN j.attempt = j.max_attempts THEN j.max_attempts + 1 ELSE j.max_attempts END,
                    finalized_at = NULL,
                    -- A cancellation requested during an earlier attempt must not cancel the next one.
                    metadata = j.metadata - 'cancel_attempted_at',
                    scheduled_at = now()
                FROM locked
                WHERE j.id = locked.id
                  AND j.state <> 'running'
                  AND NOT (j.state = 'available' AND j.scheduled_at < now())
                RETURNING {Columns("j")}
            )
            SELECT * FROM updated
            UNION ALL
            SELECT {Columns(prefix + "job")} FROM {job} WHERE id = @id AND NOT EXISTS (SELECT 1 FROM updated)
            """;

        Delete = $"""
            WITH deleted AS (
                DELETE FROM {job} WHERE id = @id AND state <> 'running'
                RETURNING {Columns(prefix + "job")}
            )
            SELECT * FROM deleted
            UNION ALL
            SELECT {Columns(prefix + "job")} FROM {job} WHERE id = @id AND NOT EXISTS (SELECT 1 FROM deleted)
            """;

        Get = $"SELECT {Columns(prefix + "job")} FROM {job} WHERE id = @id";

        GetMany = $"SELECT {Columns(prefix + "job")} FROM {job} WHERE id = ANY(@ids::bigint[])";

        ListTemplate = $"""
            SELECT {Columns(prefix + "job")}
            FROM {job}
            WHERE {"{0}"}
              AND (@after::bigint IS NULL OR id {"{1}"} @after)
            ORDER BY id {"{2}"}
            LIMIT @limit
            """;

        // The bulk operations run one batch per statement, walking IDs upward from @after to @until, the highest ID when
        // the operation started, so a stream of matching inserts can't keep it going. Each returns scanned, last_id,
        // affected, then cancel requests or skipped unique conflicts, then the sequences it may have freed. Rows are locked in ID order and waited for rather
        // than skipped, so a job mid-fetch isn't missed; fetches use SKIP LOCKED, so they're never blocked by a batch.
        CancelManyTemplate = $"""
            WITH locked AS (
                SELECT id
                FROM {job}
                WHERE {"{0}"}
                  AND id > @after
                  AND id <= @until
                  AND state NOT IN ('cancelled', 'completed', 'discarded')
                  AND finalized_at IS NULL
                ORDER BY id
                LIMIT @limit
                FOR UPDATE
            ),
            updated AS (
                UPDATE {job} AS j
                SET
                    state = CASE WHEN j.state = 'running' THEN j.state ELSE 'cancelled' END,
                    finalized_at = CASE WHEN j.state = 'running' THEN j.finalized_at ELSE now() END,
                    metadata = jsonb_set(j.metadata, ARRAY['cancel_attempted_at'], to_jsonb(now()), true)
                FROM locked
                WHERE j.id = locked.id
                RETURNING j.id, j.queue, j.state = 'running' AS running, j.sequence_key
            ),
            -- One NOTIFY per queue per 300 running jobs: 300 IDs plus the longest queue name stay under the 8000-byte
            -- payload limit.
            notified AS (
                SELECT pg_notify(@channel, json_build_object('action', 'cancel', 'queue', queue, 'job_ids', json_agg(id ORDER BY id))::text)
                FROM (
                    SELECT id, queue, (row_number() OVER (PARTITION BY queue ORDER BY id) - 1) / 300 AS chunk
                    FROM updated
                    WHERE running
                ) AS r
                GROUP BY queue, chunk
            )
            SELECT
                (SELECT count(*) FROM locked),
                (SELECT max(id) FROM locked),
                (SELECT count(*) FROM updated WHERE NOT running),
                (SELECT count(*) FROM updated WHERE running),
                (SELECT array_agg(DISTINCT sequence_key) FROM updated WHERE NOT running AND sequence_key IS NOT NULL),
                -- Referenced so the NOTIFYs run; an unreferenced CTE is never evaluated.
                (SELECT count(*) FROM notified)
            """;

        // A job that would take a unique key another live job holds is skipped (RetryAsync would throw instead), as are
        // all but the lowest ID of batch rows that would take the same key. A holder committed after the statement's
        // snapshot still raises a unique violation; the client reruns the batch, whose new snapshot sees it.
        RetryManyTemplate = $"""
            WITH locked AS (
                SELECT id, state, unique_key, unique_states
                FROM {job}
                WHERE {"{0}"}
                  AND id > @after
                  AND id <= @until
                  AND state <> 'running'
                  AND NOT (state = 'available' AND scheduled_at < now())
                ORDER BY id
                LIMIT @limit
                FOR UPDATE
            ),
            claiming AS (
                SELECT id, unique_key, row_number() OVER (PARTITION BY unique_key ORDER BY id) AS row_num
                FROM locked
                WHERE unique_key IS NOT NULL
                  AND unique_states IS NOT NULL
                  AND {bitmask}(unique_states, 'available')
                  AND NOT {bitmask}(unique_states, state)
            ),
            conflicts AS (
                SELECT c.id
                FROM claiming AS c
                WHERE c.row_num > 1
                   OR EXISTS (
                       SELECT 1 FROM {job} AS o
                       WHERE o.unique_key = c.unique_key
                         AND o.id <> c.id
                         AND o.unique_states IS NOT NULL
                         AND {bitmask}(o.unique_states, o.state))
            ),
            updated AS (
                UPDATE {job} AS j
                SET
                    -- As in Retry: a sequence job rejoins the line.
                    state = CASE
                        WHEN j.sequence_key IS NOT NULL AND j.state IN ('cancelled', 'completed', 'discarded', 'pending')
                        THEN 'pending'::{state}
                        ELSE 'available'::{state}
                    END,
                    max_attempts = CASE WHEN j.attempt = j.max_attempts THEN j.max_attempts + 1 ELSE j.max_attempts END,
                    finalized_at = NULL,
                    metadata = j.metadata - 'cancel_attempted_at',
                    scheduled_at = now()
                FROM locked
                WHERE j.id = locked.id
                  AND locked.id NOT IN (SELECT id FROM conflicts)
                RETURNING j.id, j.sequence_key
            )
            SELECT
                (SELECT count(*) FROM locked),
                (SELECT max(id) FROM locked),
                (SELECT count(*) FROM updated),
                (SELECT count(*) FROM conflicts),
                (SELECT array_agg(DISTINCT sequence_key) FROM updated WHERE sequence_key IS NOT NULL)
            """;

        DeleteManyTemplate = $"""
            WITH locked AS (
                SELECT id
                FROM {job}
                WHERE {"{0}"}
                  AND id > @after
                  AND id <= @until
                  AND state <> 'running'
                ORDER BY id
                LIMIT @limit
                FOR UPDATE
            ),
            deleted AS (
                DELETE FROM {job} AS j
                USING locked
                WHERE j.id = locked.id
                RETURNING j.id, j.sequence_key
            )
            SELECT
                (SELECT count(*) FROM locked),
                (SELECT max(id) FROM locked),
                (SELECT count(*) FROM deleted),
                0::bigint,
                (SELECT array_agg(DISTINCT sequence_key) FROM deleted WHERE sequence_key IS NOT NULL)
            """;

        MaxJobId = $"SELECT coalesce(max(id), 0) FROM {job}";

        GetCancelRequested = $"""
            SELECT id FROM {job}
            WHERE id = ANY(@ids::bigint[])
              AND state = 'running'
              AND metadata ? 'cancel_attempted_at'
            """;

        // River's JobSchedule: due jobs become available, except that only one of several sharing a unique key may,
        // and none may if another live job already holds the key; the rest are discarded.
        Schedule = $"""
            WITH jobs_to_schedule AS (
                SELECT id, unique_key, unique_states, priority, scheduled_at
                FROM {job}
                WHERE state IN ('retryable', 'scheduled')
                  AND scheduled_at <= now()
                ORDER BY priority, scheduled_at, id
                LIMIT @max
                FOR UPDATE
            ),
            jobs_with_rownum AS (
                SELECT *,
                    CASE WHEN unique_key IS NOT NULL AND unique_states IS NOT NULL
                        THEN row_number() OVER (PARTITION BY unique_key ORDER BY priority, scheduled_at, id)
                    END AS row_num
                FROM jobs_to_schedule
            ),
            unique_conflicts AS (
                SELECT j.unique_key
                FROM {job} AS j
                JOIN jobs_with_rownum AS s ON j.unique_key = s.unique_key AND j.id <> s.id
                WHERE j.unique_key IS NOT NULL
                  AND j.unique_states IS NOT NULL
                  AND {bitmask}(j.unique_states, j.state)
            ),
            job_updates AS (
                SELECT
                    s.id,
                    CASE
                        WHEN s.row_num IS NULL THEN 'available'::{state}
                        WHEN uc.unique_key IS NOT NULL THEN 'discarded'::{state}
                        WHEN s.row_num = 1 THEN 'available'::{state}
                        ELSE 'discarded'::{state}
                    END AS new_state
                FROM jobs_with_rownum AS s
                LEFT JOIN unique_conflicts AS uc ON s.unique_key = uc.unique_key
            )
            UPDATE {job} AS j
            SET
                state = u.new_state,
                finalized_at = CASE WHEN u.new_state = 'discarded' THEN now() ELSE j.finalized_at END,
                metadata = CASE WHEN u.new_state = 'discarded'
                    THEN j.metadata || '{"{\"unique_key_conflict\": \"scheduler_discarded\"}"}'::jsonb
                    ELSE j.metadata
                END
            FROM job_updates AS u
            WHERE j.id = u.id
            RETURNING j.queue, u.new_state::text, j.id
            """;

        GetStuck = $"""
            SELECT {Columns(prefix + "job")}
            FROM {job}
            WHERE state = 'running'
              AND attempted_at < @stuck_horizon
              AND {LastSeen(prefix + "job")} < @stuck_horizon
              AND id > @after_id
            ORDER BY id
            LIMIT @max
            """;

        RescueMany = $"""
            UPDATE {job} AS j
            SET
                errors = array_append(j.errors, r.error::jsonb),
                finalized_at = r.finalized_at,
                scheduled_at = r.scheduled_at,
                metadata = j.metadata || jsonb_build_object(
                    'oxbow:rescue_count',
                    coalesce(CASE WHEN jsonb_typeof(j.metadata -> 'oxbow:rescue_count') = 'number'
                        THEN (j.metadata ->> 'oxbow:rescue_count')::int END, 0) + 1),
                state = r.state::{state}
            FROM unnest(@id::bigint[], @error::text[], @finalized_at::timestamptz[], @scheduled_at::timestamptz[], @state::text[])
                AS r(id, error, finalized_at, scheduled_at, state)
            WHERE j.id = r.id
              AND j.state = 'running'
              AND {LastSeen("j")} < @stuck_horizon
            RETURNING {Columns("j")}
            """;

        // The attempted_at guard keeps a finished, rescued or refetched attempt from writing.
        Heartbeat = $"""
            UPDATE {job}
            SET metadata = metadata || jsonb_build_object('oxbow:heartbeat_at', now())
            WHERE id = @id
              AND state = 'running'
              AND attempted_at = @attempted_at
            """;

        ReportProgress = $"""
            UPDATE {job}
            SET metadata = metadata || jsonb_build_object(
                'oxbow:heartbeat_at', now(),
                'oxbow:progress', jsonb_build_object(
                    'percent', @percent::float8, 'message', @message::text, 'attempt', attempt, 'at', now()))
            WHERE id = @id
              AND state = 'running'
              AND attempted_at = @attempted_at
            """;

        DeleteFinalized = $"""
            DELETE FROM {job}
            WHERE id IN (
                SELECT id FROM {job} AS j
                WHERE ((state = 'cancelled' AND @cancelled_do_delete AND finalized_at < @cancelled_horizon)
                   OR (state = 'completed' AND @completed_do_delete AND finalized_at < @completed_horizon)
                   OR (state = 'discarded' AND @discarded_do_delete AND finalized_at < @discarded_horizon))
                  -- A batch's members outlive their own retention until the batch row is cleaned.
                  AND NOT EXISTS (SELECT 1 FROM {batch} AS b WHERE b.id = j.batch_id)
                  -- A halted sequence stays halted until its job is retried or deleted.
                  AND NOT {Halts("j.")}
                ORDER BY id
                LIMIT @max
            )
            """;

        CountByQueueAndState = $"""
            SELECT queue, state::text, count(*)
            FROM {job}
            WHERE state IN ('available', 'running', 'scheduled', 'retryable')
            GROUP BY queue, state
            """;

        LeaderDeleteExpired = $"DELETE FROM {leader} WHERE expires_at < now()";

        LeaderAttemptElect = $"""
            INSERT INTO {leader} (leader_id, elected_at, expires_at)
            VALUES (@leader_id, now(), now() + make_interval(secs => @ttl))
            ON CONFLICT (name) DO NOTHING
            RETURNING elected_at
            """;

        LeaderAttemptReelect = $"""
            UPDATE {leader}
            SET expires_at = now() + make_interval(secs => @ttl)
            WHERE leader_id = @leader_id
              AND elected_at = @elected_at
              AND expires_at >= now()
            RETURNING elected_at
            """;

        LeaderResign = $"""
            WITH resigned AS (
                DELETE FROM {leader}
                WHERE leader_id = @leader_id AND elected_at = @elected_at
                RETURNING leader_id
            )
            SELECT pg_notify(@channel, json_build_object('action', 'resigned', 'leader_id', leader_id)::text)
            FROM resigned
            """;

        QueueUpsert = $"""
            INSERT INTO {queue} (name, updated_at)
            VALUES (@name, now())
            ON CONFLICT (name) DO UPDATE SET updated_at = now()
            RETURNING paused_at IS NOT NULL
            """;

        QueueIsPaused = $"SELECT paused_at IS NOT NULL FROM {queue} WHERE name = @name";

        QueuePause = $"""
            WITH paused AS (
                INSERT INTO {queue} (name, paused_at, updated_at)
                SELECT @name, now(), now()
                WHERE @name <> '*'
                ON CONFLICT (name) DO UPDATE
                    SET paused_at = coalesce({prefix}queue.paused_at, now()), updated_at = now()
                RETURNING name
            ),
            paused_all AS (
                UPDATE {queue}
                SET paused_at = coalesce(paused_at, now()), updated_at = now()
                WHERE @name = '*'
                RETURNING name
            )
            SELECT pg_notify(@channel, json_build_object('action', 'pause', 'queue', @name)::text)
            """;

        QueueResume = $"""
            WITH resumed AS (
                UPDATE {queue}
                SET paused_at = NULL, updated_at = now()
                WHERE name = @name OR @name = '*'
                RETURNING name
            )
            SELECT pg_notify(@channel, json_build_object('action', 'resume', 'queue', @name)::text)
            """;

        // A queue whose rate limiter is still ahead of now() keeps its row, or a restart would get a fresh allowance.
        QueueDeleteExpired = $"""
            DELETE FROM {queue}
            WHERE updated_at < @horizon
              AND (rate_limit_tat IS NULL OR rate_limit_tat < now())
            """;

        // A loose index scan: one probe of the pending index per sequence, however long its backlog.
        SequencePendingKeys = $"""
            WITH RECURSIVE pending AS (
                (SELECT sequence_key FROM {job}
                 WHERE state = 'pending' AND sequence_key IS NOT NULL AND sequence_key > @after
                 ORDER BY sequence_key LIMIT 1)
                UNION ALL
                SELECT (SELECT j.sequence_key FROM {job} AS j
                        WHERE j.state = 'pending' AND j.sequence_key IS NOT NULL AND j.sequence_key > p.sequence_key
                        ORDER BY j.sequence_key LIMIT 1)
                FROM pending AS p
                WHERE p.sequence_key IS NOT NULL
            )
            SELECT sequence_key FROM pending WHERE sequence_key IS NOT NULL LIMIT @max
            """;

        // The oldest pending job of each sequence goes live when nothing in the sequence is live or halts it. Heads are
        // locked in id order and skipped if another promoter holds them, so promoters don't deadlock. One working from
        // a stale snapshot can still pick a different head; the live index admits one job per sequence and it retries.
        // Identical notifications in one transaction are delivered once, so this wakes each queue once.
        SequencePromote = $"""
            WITH heads AS (
                SELECT next.id
                FROM unnest(@keys::text[]) AS k(sequence_key)
                CROSS JOIN LATERAL (
                    SELECT id FROM {job}
                    WHERE sequence_key = k.sequence_key AND state = 'pending'
                    ORDER BY id
                    LIMIT 1
                ) AS next
                WHERE NOT EXISTS (
                    SELECT 1 FROM {job} AS l
                    WHERE l.sequence_key = k.sequence_key
                      AND l.state IN ('available', 'retryable', 'running', 'scheduled')
                )
                  AND NOT EXISTS (
                    SELECT 1 FROM {job} AS f
                    WHERE f.sequence_key = k.sequence_key
                      AND f.state IN ('cancelled', 'discarded')
                      AND {Halts("f.")}
                )
            ),
            locked AS (
                SELECT j.id
                FROM {job} AS j
                JOIN heads ON j.id = heads.id
                WHERE j.state = 'pending'
                ORDER BY j.id
                FOR UPDATE OF j SKIP LOCKED
            )
            UPDATE {job} AS j
            SET state = CASE WHEN j.scheduled_at > now() THEN 'scheduled'::{state} ELSE 'available'::{state} END
            FROM locked
            WHERE j.id = locked.id
            RETURNING j.queue, j.state::text,
                CASE WHEN j.state = 'available'
                    THEN pg_notify(@channel, json_build_object('queue', j.queue)::text)::text
                END
            """;

        SequenceKeyOf = $"SELECT sequence_key FROM {job} WHERE id = @id";
        BatchNextId = $"SELECT nextval('{schema}.{prefix}batch_id_seq')";

        // The batch row is written in the same statement as its members, so no finisher can see a batch without them,
        // whether or not the caller's connection is in a transaction.
        InsertBatch = $"""
            WITH jobs AS (
            {InsertMany}
            ),
            new_batch AS (
                INSERT INTO {batch} (id, total, on_complete, on_success)
                SELECT @batch_id, count(*) FILTER (WHERE NOT unique_skipped_as_duplicate), @on_complete::jsonb, @on_success::jsonb
                FROM jobs
            )
            SELECT * FROM jobs
            """;

        const string liveStates = "'available', 'pending', 'retryable', 'running', 'scheduled'";

        BatchGetFinishable = $"""
            SELECT b.id
            FROM {batch} AS b
            WHERE b.finalized_at IS NULL
              AND b.id > @after
              AND NOT EXISTS (SELECT 1 FROM {job} AS j WHERE j.batch_id = b.id AND j.state IN ({liveStates}))
            ORDER BY b.id
            LIMIT @max
            """;

        // The row lock and the finalized_at check make this exactly-once: a second leader running the same batch
        // skips it while it's locked and sees it finalized afterwards. A deleted member counts as cancelled.
        BatchFinish = $"""
            WITH finished AS (
                SELECT b.id, b.total, b.on_complete, b.on_success
                FROM {batch} AS b
                WHERE b.id = @id
                  AND b.finalized_at IS NULL
                  AND NOT EXISTS (SELECT 1 FROM {job} AS j WHERE j.batch_id = b.id AND j.state IN ({liveStates}))
                FOR UPDATE OF b SKIP LOCKED
            ),
            counts AS (
                SELECT
                    count(*) FILTER (WHERE j.state = 'completed') AS completed,
                    count(*) FILTER (WHERE j.state = 'discarded') AS discarded
                FROM {job} AS j
                WHERE j.batch_id IN (SELECT id FROM finished)
            ),
            complete_job AS (
            {InsertCallback("on_complete", "finished AS f", "TRUE", "complete")}
            ),
            success_job AS (
            {InsertCallback("on_success", "finished AS f, counts AS c", "c.completed = f.total", "success")}
            ),
            updated AS (
                UPDATE {batch} AS b
                SET finalized_at = now(),
                    completed = c.completed,
                    discarded = c.discarded,
                    cancelled = f.total - c.completed - c.discarded,
                    on_complete_job_id = (SELECT id FROM complete_job),
                    on_success_job_id = (SELECT id FROM success_job)
                FROM finished AS f, counts AS c
                WHERE b.id = f.id
            )
            SELECT queue, state, sequence_key FROM complete_job WHERE NOT duplicate
            UNION ALL
            SELECT queue, state, sequence_key FROM success_job WHERE NOT duplicate
            """;

        string InsertCallback(string column, string from, string condition, string callback) => $"""
                INSERT INTO {job} (args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states,
                    sequence_key, sequence_continue_on, partition_key)
                SELECT
                    f.{column} -> 'args',
                    f.{column} ->> 'kind',
                    (f.{column} ->> 'max_attempts')::smallint,
                    f.{column} -> 'metadata' || jsonb_build_object('oxbow:batch_id', f.id, 'oxbow:batch_callback', '{callback}'),
                    (f.{column} ->> 'priority')::smallint,
                    f.{column} ->> 'queue',
                    coalesce((f.{column} ->> 'scheduled_at')::timestamptz, now()),
                    CASE
                        WHEN f.{column} ->> 'sequence_key' IS NOT NULL THEN 'pending'
                        WHEN (f.{column} ->> 'scheduled_at')::timestamptz > now() THEN 'scheduled'
                        ELSE 'available'
                    END::{state},
                    ARRAY(SELECT jsonb_array_elements_text(f.{column} -> 'tags'))::varchar(255)[],
                    decode(f.{column} ->> 'unique_key', 'base64'),
                    nullif((f.{column} ->> 'unique_states')::int, 0)::bit(8),
                    f.{column} ->> 'sequence_key',
                    (f.{column} ->> 'sequence_continue_on')::smallint,
                    f.{column} ->> 'partition_key'
                FROM {from}
                WHERE f.{column} IS NOT NULL AND {condition}
                ON CONFLICT (unique_key)
                    WHERE unique_key IS NOT NULL
                      AND unique_states IS NOT NULL
                      AND {bitmask}(unique_states, state)
                    DO UPDATE SET kind = {prefix}job.kind
                RETURNING id, queue, state::text AS state, (xmax <> 0) AS duplicate, sequence_key
            """;

        // Unfinished, the counts come from the members, which the cleaner keeps while the batch row exists.
        BatchGet = $"""
            SELECT
                b.id, b.created_at, b.finalized_at, b.total,
                coalesce(b.completed, m.completed)::int,
                coalesce(b.discarded, m.discarded)::int,
                coalesce(b.cancelled, b.total - m.present + m.cancelled)::int,
                b.on_complete_job_id, b.on_success_job_id
            FROM {batch} AS b
            CROSS JOIN LATERAL (
                SELECT
                    count(*) FILTER (WHERE j.state = 'completed') AS completed,
                    count(*) FILTER (WHERE j.state = 'discarded') AS discarded,
                    count(*) FILTER (WHERE j.state = 'cancelled') AS cancelled,
                    count(*) AS present
                FROM {job} AS j
                WHERE j.batch_id = b.id AND b.finalized_at IS NULL
            ) AS m
            WHERE b.id = @id
            """;

        // A batch stays while its callbacks may still be running and asking about it.
        BatchDeleteExpired = $"""
            DELETE FROM {batch}
            WHERE id IN (
                SELECT b.id FROM {batch} AS b
                WHERE b.finalized_at < @horizon
                  AND NOT EXISTS (
                      SELECT 1 FROM {job} AS j
                      WHERE j.id IN (b.on_complete_job_id, b.on_success_job_id) AND j.finalized_at IS NULL)
                ORDER BY b.id
                LIMIT @max
            )
            """;

        // Durable periodic jobs. A stored schedule is a cron expression with its time zone, or an interval.
        const string everyParam = "@every_seconds::float8 * interval '1 second'";

        PeriodicJobUpsert = $"""
            INSERT INTO {periodic} AS p (name, kind, args, options, cron, time_zone, every, next_run_at)
            VALUES (@name, @kind, @args::jsonb, @options::jsonb, @cron, @time_zone, {everyParam}, @next_run_at)
            ON CONFLICT (name) DO UPDATE SET
                kind = excluded.kind,
                args = excluded.args,
                options = excluded.options,
                cron = excluded.cron,
                time_zone = excluded.time_zone,
                every = excluded.every,
                next_run_at = CASE
                    WHEN (p.cron, p.time_zone, p.every) IS DISTINCT FROM (excluded.cron, excluded.time_zone, excluded.every)
                    THEN excluded.next_run_at
                    ELSE coalesce(p.next_run_at, excluded.next_run_at)
                END,
                updated_at = now()
            WHERE NOT p.registered_in_code
            RETURNING {PeriodicColumns("p")}
            """;

        PeriodicJobCreate = $"""
            INSERT INTO {periodic} AS p (name, kind, args, options, cron, time_zone, every, next_run_at)
            VALUES (@name, @kind, @args::jsonb, @options::jsonb, @cron, @time_zone, {everyParam}, @next_run_at)
            ON CONFLICT (name) DO NOTHING
            RETURNING {PeriodicColumns("p")}
            """;

        PeriodicJobPause = $"""
            UPDATE {periodic} AS p
            SET paused_at = coalesce(p.paused_at, now()), updated_at = now()
            WHERE name = @name
            RETURNING {PeriodicColumns("p")}
            """;

        PeriodicJobLock = $"SELECT {PeriodicColumns(prefix + "periodic_job")} FROM {periodic} WHERE name = @name FOR UPDATE";

        PeriodicJobResume = $"""
            UPDATE {periodic} AS p
            SET paused_at = NULL, next_run_at = @next_run_at, updated_at = now()
            WHERE name = @name AND paused_at IS NOT NULL
            RETURNING {PeriodicColumns("p")}
            """;

        PeriodicJobDelete = $"DELETE FROM {periodic} AS p WHERE name = @name RETURNING {PeriodicColumns("p")}";

        PeriodicJobGet = $"SELECT {PeriodicColumns(prefix + "periodic_job")} FROM {periodic} WHERE name = @name";

        PeriodicJobList = $"""
            SELECT {PeriodicColumns(prefix + "periodic_job")}
            FROM {periodic}
            WHERE @after::text IS NULL OR name > @after
            ORDER BY name
            LIMIT @limit
            """;

        // The leader's sync of the jobs registered in its code: create missing rows, take up schedule changes, give a
        // resumed job its next run, and refresh the rest hourly so rows for jobs no longer in any leader's code can be told apart and deleted.
        PeriodicJobSyncCode = $"""
            INSERT INTO {periodic} (name, registered_in_code, cron, time_zone, every, next_run_at)
            SELECT i.name, true, i.cron, i.time_zone, i.every_seconds * interval '1 second', i.first_run_at
            FROM unnest(@name::text[], @cron::text[], @time_zone::text[], @every_seconds::float8[], @first_run_at::timestamptz[])
                AS i(name, cron, time_zone, every_seconds, first_run_at)
            ON CONFLICT (name) DO NOTHING;

            UPDATE {periodic} AS p
            SET cron = i.cron,
                time_zone = i.time_zone,
                every = i.every_seconds * interval '1 second',
                next_run_at = CASE
                    WHEN (p.cron, p.time_zone, p.every) IS DISTINCT FROM (i.cron, i.time_zone, i.every_seconds * interval '1 second')
                      OR p.next_run_at IS NULL
                    THEN i.next_occurrence
                    ELSE p.next_run_at
                END,
                updated_at = now()
            FROM unnest(@name::text[], @cron::text[], @time_zone::text[], @every_seconds::float8[], @next_occurrence::timestamptz[])
                AS i(name, cron, time_zone, every_seconds, next_occurrence)
            WHERE p.name = i.name
              AND p.registered_in_code
              AND ((p.cron, p.time_zone, p.every) IS DISTINCT FROM (i.cron, i.time_zone, i.every_seconds * interval '1 second')
                   OR (p.next_run_at IS NULL AND i.next_occurrence IS NOT NULL AND p.paused_at IS NULL)
                   OR p.updated_at < now() - interval '1 hour');

            DELETE FROM {periodic} WHERE registered_in_code AND updated_at < now() - interval '24 hours';
            """;

        PeriodicJobCodeConflicts = $"SELECT name FROM {periodic} WHERE name = ANY(@name) AND NOT registered_in_code";

        PeriodicJobGetDue = $"""
            SELECT {PeriodicColumns(prefix + "periodic_job")}, now()
            FROM {periodic}
            WHERE paused_at IS NULL
              AND next_run_at <= now()
              AND (NOT registered_in_code OR name = ANY(@code_names))
            ORDER BY next_run_at
            LIMIT @max
            """;

        PeriodicJobUntilNext = $"""
            SELECT extract(epoch FROM min(next_run_at) - now())::float8
            FROM {periodic}
            WHERE paused_at IS NULL
              AND (NOT registered_in_code OR name = ANY(@code_names))
            """;

        // Guarded on the row as read, so of two leaders across a failover only one advances it and enqueues the run,
        // and a definition changed since it was read isn't run with the old one.
        PeriodicJobAdvance = $"""
            UPDATE {periodic}
            SET next_run_at = @next_run_at,
                last_run_at = CASE WHEN @ran THEN now() ELSE last_run_at END,
                kind = coalesce(@kind, kind)
            WHERE name = @name
              AND next_run_at = @expected_next_run_at
              AND updated_at = @expected_updated_at
              AND paused_at IS NULL
            """;
    }

    public string InsertChannel { get; }

    public string ControlChannel { get; }

    public string LeadershipChannel { get; }

    public string SequenceChannel { get; }

    public string SchemaVersion { get; }

    public string InsertMany { get; }

    public string CopyJobs { get; }

    public string StateTypeName { get; }

    public string Notify { get; }

    public string GetAvailable { get; }

    public string QueueLock { get; }

    public string GetAvailableLimited { get; }

    public string FetchWithoutJit { get; }

    public string RateLimitWait { get; }

    public string RateLimitDeleteExpired { get; }

    public string IsRunningAttempt { get; }

    public string SetStateIfRunning { get; }

    public string Cancel { get; }

    public string Retry { get; }

    public string Delete { get; }

    public string Get { get; }

    public string GetMany { get; }

    /// <summary>
    /// Format with the <see cref="JobFilterSql"/> clause, the keyset comparison (<c>&gt;</c> or <c>&lt;</c>) and the
    /// order (<c>ASC</c> or <c>DESC</c>).
    /// </summary>
    public string ListTemplate { get; }

    /// <summary>Format with the <see cref="JobFilterSql"/> clause.</summary>
    public string CancelManyTemplate { get; }

    /// <summary>Format with the <see cref="JobFilterSql"/> clause.</summary>
    public string RetryManyTemplate { get; }

    /// <summary>Format with the <see cref="JobFilterSql"/> clause.</summary>
    public string DeleteManyTemplate { get; }

    public string MaxJobId { get; }

    public string GetCancelRequested { get; }

    public string Schedule { get; }

    public string GetStuck { get; }

    public string RescueMany { get; }

    public string Heartbeat { get; }

    public string ReportProgress { get; }

    public string DeleteFinalized { get; }

    public string CountByQueueAndState { get; }

    public string LeaderDeleteExpired { get; }

    public string LeaderAttemptElect { get; }

    public string LeaderAttemptReelect { get; }

    public string LeaderResign { get; }

    public string QueueUpsert { get; }

    public string QueueIsPaused { get; }

    public string QueuePause { get; }

    public string QueueResume { get; }

    public string QueueDeleteExpired { get; }

    public string SequencePendingKeys { get; }

    public string SequencePromote { get; }

    public string SequenceKeyOf { get; }

    /// <summary>
    /// A cancelled or discarded sequence job that ran, and whose policy doesn't continue past that state, halts its
    /// sequence. One that never started was only taken out of the line. attempted_at, not attempt: a snooze or soft
    /// stop hands the first attempt back, but the job still ran.
    /// </summary>
    private static string Halts(string alias) =>
        $"({alias}sequence_key IS NOT NULL AND {alias}state IN ('cancelled', 'discarded') AND {alias}attempted_at IS NOT NULL " +
        $"AND (coalesce({alias}sequence_continue_on, 0) & CASE {alias}state WHEN 'cancelled' THEN 1 ELSE 2 END) = 0)";

    public string BatchNextId { get; }

    public string InsertBatch { get; }

    public string BatchGetFinishable { get; }

    public string BatchFinish { get; }

    public string BatchGet { get; }

    public string BatchDeleteExpired { get; }

    /// <summary>
    /// The later of a job's attempted_at and its heartbeat. The heartbeat is metadata anyone can write, so a value that
    /// isn't a timestamp is ignored rather than cast, which would fail the whole rescue.
    /// </summary>
    private static string LastSeen(string alias) =>
        $"greatest({alias}.attempted_at, CASE WHEN jsonb_typeof({alias}.metadata -> 'oxbow:heartbeat_at') = 'string' " +
        $"AND pg_input_is_valid({alias}.metadata ->> 'oxbow:heartbeat_at', 'timestamptz') " +
        $"THEN ({alias}.metadata ->> 'oxbow:heartbeat_at')::timestamptz END)";

    public string PeriodicJobUpsert { get; }

    public string PeriodicJobCreate { get; }

    public string PeriodicJobPause { get; }

    public string PeriodicJobLock { get; }

    public string PeriodicJobResume { get; }

    public string PeriodicJobDelete { get; }

    public string PeriodicJobGet { get; }

    public string PeriodicJobList { get; }

    public string PeriodicJobSyncCode { get; }

    public string PeriodicJobCodeConflicts { get; }

    public string PeriodicJobGetDue { get; }

    public string PeriodicJobUntilNext { get; }

    public string PeriodicJobAdvance { get; }

    /// <summary>The periodic job columns, in the order <see cref="PeriodicJobRowReader"/> reads them.</summary>
    private static string PeriodicColumns(string alias) =>
        $"{alias}.name, {alias}.registered_in_code, {alias}.kind, {alias}.args::text, {alias}.options::text, {alias}.cron, " +
        $"{alias}.time_zone, extract(epoch FROM {alias}.every)::float8, {alias}.paused_at, {alias}.next_run_at, " +
        $"{alias}.last_run_at, {alias}.created_at, {alias}.updated_at";

    /// <summary>The row columns, in the order <see cref="JobRowReader"/> reads them.</summary>
    private static string Columns(string alias) =>
        $"{alias}.id, {alias}.state::text, {alias}.attempt, {alias}.max_attempts, {alias}.attempted_at, {alias}.created_at, " +
        $"{alias}.finalized_at, {alias}.scheduled_at, {alias}.priority, {alias}.args::text, {alias}.attempted_by, " +
        $"{alias}.errors::text[], {alias}.kind, {alias}.metadata::text, {alias}.queue, {alias}.tags::text[], " +
        $"{alias}.sequence_key, {alias}.partition_key";
}

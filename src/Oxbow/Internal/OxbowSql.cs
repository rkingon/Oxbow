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
        var batch = $"{schema}.{prefix}batch";

        InsertChannel = $"{schema}.{prefix}insert";
        ControlChannel = $"{schema}.{prefix}control";
        LeadershipChannel = $"{schema}.{prefix}leadership";

        SchemaVersion = $"SELECT coalesce(max(version), 0) FROM {schema}.{prefix}migration";

        InsertMany = $"""
            INSERT INTO {job} (args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states, batch_id)
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
                @batch_id::bigint
            FROM unnest(
                @args::text[], @kind::text[], @max_attempts::smallint[], @metadata::text[], @priority::smallint[],
                @queue::text[], @scheduled_at::timestamptz[], @state::text[], @tags::text[], @unique_key::bytea[],
                @unique_states::int[]
            ) AS i(args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states)
            ON CONFLICT (unique_key)
                WHERE unique_key IS NOT NULL
                  AND unique_states IS NOT NULL
                  AND {bitmask}(unique_states, state)
                -- A row is only returned on conflict if something is updated.
                DO UPDATE SET kind = {prefix}job.kind
            RETURNING {Columns(prefix + "job")}, (xmax <> 0) AS unique_skipped_as_duplicate
            """;

        CopyJobs = $"COPY {job} (args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags) FROM STDIN (FORMAT BINARY)";
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

        // River's JobSetStateIfRunningMany: only a running job moves, and a retry, snooze or soft stop of a job whose
        // cancellation was requested becomes a cancellation instead.
        SetStateIfRunning = $"""
            WITH input AS (
                SELECT
                    unnest(@id::bigint[]) AS id,
                    unnest(@attempt_do_update::boolean[]) AS attempt_do_update,
                    unnest(@attempt::int[]) AS attempt,
                    unnest(@errors_do_update::boolean[]) AS errors_do_update,
                    unnest(@errors::text[])::jsonb AS errors,
                    unnest(@finalized_at_do_update::boolean[]) AS finalized_at_do_update,
                    unnest(@finalized_at::timestamptz[]) AS finalized_at,
                    unnest(@metadata_do_merge::boolean[]) AS metadata_do_merge,
                    unnest(@metadata_updates::text[])::jsonb AS metadata_updates,
                    unnest(@scheduled_at_do_update::boolean[]) AS scheduled_at_do_update,
                    unnest(@scheduled_at::timestamptz[]) AS scheduled_at,
                    unnest(@state::text[])::{state} AS state
            ),
            updated AS (
                UPDATE {job} AS j
                SET
                    attempt = CASE
                        WHEN j.state = 'running'
                             AND NOT (input.state IN ('available', 'retryable', 'scheduled') AND j.metadata ? 'cancel_attempted_at')
                             AND input.attempt_do_update
                        THEN input.attempt
                        ELSE j.attempt
                    END,
                    errors = CASE
                        WHEN j.state = 'running' AND input.errors_do_update
                        THEN array_append(j.errors, input.errors)
                        ELSE j.errors
                    END,
                    finalized_at = CASE
                        WHEN j.state = 'running'
                             AND input.state IN ('available', 'retryable', 'scheduled') AND j.metadata ? 'cancel_attempted_at'
                        THEN now()
                        WHEN j.state = 'running' AND input.finalized_at_do_update
                        THEN input.finalized_at
                        ELSE j.finalized_at
                    END,
                    metadata = CASE
                        WHEN input.metadata_do_merge THEN j.metadata || input.metadata_updates
                        ELSE j.metadata
                    END,
                    scheduled_at = CASE
                        WHEN j.state = 'running'
                             AND NOT (input.state IN ('available', 'retryable', 'scheduled') AND j.metadata ? 'cancel_attempted_at')
                             AND input.scheduled_at_do_update
                        THEN input.scheduled_at
                        ELSE j.scheduled_at
                    END,
                    state = CASE
                        WHEN j.state = 'running'
                             AND input.state IN ('available', 'retryable', 'scheduled') AND j.metadata ? 'cancel_attempted_at'
                        THEN 'cancelled'::{state}
                        WHEN j.state = 'running' THEN input.state
                        ELSE j.state
                    END
                FROM input
                WHERE j.id = input.id
                  AND (j.state = 'running' OR input.metadata_do_merge)
                RETURNING j.id, j.state::text AS state, j.queue, j.kind
            )
            SELECT id, state, queue, kind FROM updated
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
                    state = 'available',
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

        ListTemplate = $"""
            SELECT {Columns(prefix + "job")}
            FROM {job}
            WHERE (@states::text[] IS NULL OR state::text = ANY(@states))
              AND (@queues::text[] IS NULL OR queue = ANY(@queues))
              AND (@kinds::text[] IS NULL OR kind = ANY(@kinds))
              AND (@after::bigint IS NULL OR id {"{0}"} @after)
              AND (@batch_id::bigint IS NULL OR batch_id = @batch_id)
            ORDER BY id {"{1}"}
            LIMIT @limit
            """;

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
            RETURNING j.queue, u.new_state::text
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

        QueueDeleteExpired = $"DELETE FROM {queue} WHERE updated_at < @horizon";

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
            SELECT queue, state FROM complete_job WHERE NOT duplicate
            UNION ALL
            SELECT queue, state FROM success_job WHERE NOT duplicate
            """;

        string InsertCallback(string column, string from, string condition, string callback) => $"""
                INSERT INTO {job} (args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states)
                SELECT
                    f.{column} -> 'args',
                    f.{column} ->> 'kind',
                    (f.{column} ->> 'max_attempts')::smallint,
                    f.{column} -> 'metadata' || jsonb_build_object('oxbow:batch_id', f.id, 'oxbow:batch_callback', '{callback}'),
                    (f.{column} ->> 'priority')::smallint,
                    f.{column} ->> 'queue',
                    coalesce((f.{column} ->> 'scheduled_at')::timestamptz, now()),
                    CASE WHEN (f.{column} ->> 'scheduled_at')::timestamptz > now() THEN 'scheduled' ELSE 'available' END::{state},
                    ARRAY(SELECT jsonb_array_elements_text(f.{column} -> 'tags'))::varchar(255)[],
                    decode(f.{column} ->> 'unique_key', 'base64'),
                    nullif((f.{column} ->> 'unique_states')::int, 0)::bit(8)
                FROM {from}
                WHERE f.{column} IS NOT NULL AND {condition}
                ON CONFLICT (unique_key)
                    WHERE unique_key IS NOT NULL
                      AND unique_states IS NOT NULL
                      AND {bitmask}(unique_states, state)
                    DO UPDATE SET kind = {prefix}job.kind
                RETURNING id, queue, state::text AS state, (xmax <> 0) AS duplicate
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
    }

    public string InsertChannel { get; }

    public string ControlChannel { get; }

    public string LeadershipChannel { get; }

    public string SchemaVersion { get; }

    public string InsertMany { get; }

    public string CopyJobs { get; }

    public string StateTypeName { get; }

    public string Notify { get; }

    public string GetAvailable { get; }

    public string SetStateIfRunning { get; }

    public string Cancel { get; }

    public string Retry { get; }

    public string Delete { get; }

    public string Get { get; }

    /// <summary>Format with the keyset comparison (<c>&gt;</c> or <c>&lt;</c>) and the order (<c>ASC</c> or <c>DESC</c>).</summary>
    public string ListTemplate { get; }

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

    /// <summary>The row columns, in the order <see cref="JobRowReader"/> reads them.</summary>
    private static string Columns(string alias) =>
        $"{alias}.id, {alias}.state::text, {alias}.attempt, {alias}.max_attempts, {alias}.attempted_at, {alias}.created_at, " +
        $"{alias}.finalized_at, {alias}.scheduled_at, {alias}.priority, {alias}.args::text, {alias}.attempted_by, " +
        $"{alias}.errors::text[], {alias}.kind, {alias}.metadata::text, {alias}.queue, {alias}.tags::text[]";
}

-- durable periodic jobs
CREATE TABLE {{schema}}.{{prefix}}periodic_job (
    name text PRIMARY KEY,
    registered_in_code boolean NOT NULL DEFAULT false,
    kind text,
    args jsonb,
    options jsonb,
    cron text,
    time_zone text,
    every interval,
    paused_at timestamptz,
    next_run_at timestamptz,
    last_run_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT {{prefix}}periodic_job_name_length CHECK (char_length(name) > 0 AND char_length(name) < 256),
    CONSTRAINT {{prefix}}periodic_job_one_schedule CHECK ((cron IS NULL) <> (every IS NULL)),
    CONSTRAINT {{prefix}}periodic_job_stored_definition CHECK (registered_in_code OR (kind IS NOT NULL AND args IS NOT NULL))
);

CREATE INDEX {{prefix}}periodic_job_next_run_at_idx ON {{schema}}.{{prefix}}periodic_job USING btree (next_run_at)
    WHERE paused_at IS NULL;

-- batches
CREATE TABLE {{schema}}.{{prefix}}batch (
    id bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    finalized_at timestamptz,
    total integer NOT NULL,
    -- Member outcomes, snapshotted when the batch finishes so they survive the job cleaner.
    completed integer,
    discarded integer,
    cancelled integer,
    -- Callbacks, prepared at insert; the finisher inserts them as jobs.
    on_complete jsonb,
    on_success jsonb,
    on_complete_job_id bigint,
    on_success_job_id bigint,
    CONSTRAINT {{prefix}}batch_total_not_negative CHECK (total >= 0)
);

CREATE INDEX {{prefix}}batch_unfinished_idx ON {{schema}}.{{prefix}}batch USING btree (id) WHERE finalized_at IS NULL;

CREATE INDEX {{prefix}}batch_finalized_idx ON {{schema}}.{{prefix}}batch USING btree (finalized_at)
    WHERE finalized_at IS NOT NULL;

ALTER TABLE {{schema}}.{{prefix}}job ADD COLUMN batch_id bigint;

CREATE INDEX {{prefix}}job_batch_idx ON {{schema}}.{{prefix}}job USING btree (batch_id, state) WHERE batch_id IS NOT NULL;

INSERT INTO {{schema}}.{{prefix}}migration (version) VALUES (2);

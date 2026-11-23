INSERT INTO {{schema}}.{{prefix}}migration (version) VALUES (2);

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

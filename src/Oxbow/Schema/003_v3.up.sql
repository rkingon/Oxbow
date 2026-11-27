-- GIN never removes an entry once a key has been indexed: vacuum empties its posting list and leaves it in place. Every
-- distinct value ever written to args or metadata (IDs, outputs, heartbeat times) stays in these indexes, so they grow
-- with every job inserted, not with the jobs kept, until a REINDEX. Nothing in Oxbow queries args by containment, and a
-- metadata filter scans only the retained rows.
DROP INDEX IF EXISTS {{schema}}.{{prefix}}job_args_idx;
DROP INDEX IF EXISTS {{schema}}.{{prefix}}job_metadata_idx;

-- job steps

-- A step's recorded result. The first one recorded wins, and it goes with its job.
CREATE TABLE {{schema}}.{{prefix}}job_step (
    job_id bigint NOT NULL REFERENCES {{schema}}.{{prefix}}job (id) ON DELETE CASCADE,
    name text NOT NULL,
    output jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, name),
    CONSTRAINT {{prefix}}job_step_name_length CHECK (char_length(name) > 0 AND char_length(name) < 256)
);

INSERT INTO {{schema}}.{{prefix}}migration (version) VALUES (3);

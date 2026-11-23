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

INSERT INTO {{schema}}.{{prefix}}migration (version) VALUES (2);

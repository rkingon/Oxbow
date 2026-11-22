CREATE TYPE {{schema}}.{{prefix}}job_state AS ENUM (
    'available',
    'cancelled',
    'completed',
    'discarded',
    'pending',
    'retryable',
    'running',
    'scheduled'
);

CREATE TABLE {{schema}}.{{prefix}}job (
    id bigserial PRIMARY KEY,
    state {{schema}}.{{prefix}}job_state NOT NULL DEFAULT 'available',
    attempt smallint NOT NULL DEFAULT 0,
    max_attempts smallint NOT NULL DEFAULT 25,
    attempted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    finalized_at timestamptz,
    scheduled_at timestamptz NOT NULL DEFAULT now(),
    priority smallint NOT NULL DEFAULT 1,
    args jsonb NOT NULL,
    attempted_by text[],
    errors jsonb[],
    kind text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    queue text NOT NULL DEFAULT 'default',
    tags varchar(255)[] NOT NULL DEFAULT '{}',
    unique_key bytea,
    unique_states bit(8),
    CONSTRAINT {{prefix}}job_finalized_or_finalized_at_null CHECK (
        (finalized_at IS NULL AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
        (finalized_at IS NOT NULL AND state IN ('cancelled', 'completed', 'discarded'))
    ),
    CONSTRAINT {{prefix}}job_max_attempts_is_positive CHECK (max_attempts > 0),
    CONSTRAINT {{prefix}}job_priority_in_range CHECK (priority >= 1 AND priority <= 4),
    CONSTRAINT {{prefix}}job_queue_length CHECK (char_length(queue) > 0 AND char_length(queue) < 128),
    CONSTRAINT {{prefix}}job_kind_length CHECK (char_length(kind) > 0 AND char_length(kind) < 128)
);

CREATE INDEX {{prefix}}job_kind_idx ON {{schema}}.{{prefix}}job USING btree (kind);

CREATE INDEX {{prefix}}job_finalized_idx ON {{schema}}.{{prefix}}job USING btree (state, finalized_at)
    WHERE finalized_at IS NOT NULL;

CREATE INDEX {{prefix}}job_fetch_idx ON {{schema}}.{{prefix}}job USING btree (state, queue, priority, scheduled_at, id);

CREATE INDEX {{prefix}}job_args_idx ON {{schema}}.{{prefix}}job USING gin (args);

CREATE INDEX {{prefix}}job_metadata_idx ON {{schema}}.{{prefix}}job USING gin (metadata);

-- Bit positions match the int the client sends: available = 1, cancelled = 2, ..., scheduled = 128.
CREATE FUNCTION {{schema}}.{{prefix}}job_state_in_bitmask(bitmask bit(8), state {{schema}}.{{prefix}}job_state)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE state
        WHEN 'available' THEN get_bit(bitmask, 7)
        WHEN 'cancelled' THEN get_bit(bitmask, 6)
        WHEN 'completed' THEN get_bit(bitmask, 5)
        WHEN 'discarded' THEN get_bit(bitmask, 4)
        WHEN 'pending'   THEN get_bit(bitmask, 3)
        WHEN 'retryable' THEN get_bit(bitmask, 2)
        WHEN 'running'   THEN get_bit(bitmask, 1)
        WHEN 'scheduled' THEN get_bit(bitmask, 0)
        ELSE 0
    END = 1;
$$;

CREATE UNIQUE INDEX {{prefix}}job_unique_idx ON {{schema}}.{{prefix}}job (unique_key)
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND {{schema}}.{{prefix}}job_state_in_bitmask(unique_states, state);

CREATE UNLOGGED TABLE {{schema}}.{{prefix}}leader (
    elected_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    leader_id text NOT NULL,
    name text PRIMARY KEY DEFAULT 'default',
    CONSTRAINT {{prefix}}leader_name_default CHECK (name = 'default'),
    CONSTRAINT {{prefix}}leader_leader_id_length CHECK (char_length(leader_id) > 0 AND char_length(leader_id) < 128)
);

CREATE TABLE {{schema}}.{{prefix}}queue (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}',
    paused_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE {{schema}}.{{prefix}}migration (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO {{schema}}.{{prefix}}migration (version) VALUES (1);

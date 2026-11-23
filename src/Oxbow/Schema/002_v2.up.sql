INSERT INTO {{schema}}.{{prefix}}migration (version) VALUES (2);

-- sequences
ALTER TABLE {{schema}}.{{prefix}}job ADD COLUMN sequence_key text;

-- Bits: 1 = continue past a cancelled job, 2 = past a discarded one. NULL halts on both.
ALTER TABLE {{schema}}.{{prefix}}job ADD COLUMN sequence_continue_on smallint;

-- The oldest pending job of each sequence: the next one to promote.
CREATE INDEX {{prefix}}job_sequence_pending_idx ON {{schema}}.{{prefix}}job (sequence_key, id)
    WHERE state = 'pending' AND sequence_key IS NOT NULL;

-- At most one live job per sequence. Promotion is the only way in, and concurrent promoters that race lose here.
CREATE UNIQUE INDEX {{prefix}}job_sequence_live_idx ON {{schema}}.{{prefix}}job (sequence_key)
    WHERE state IN ('available', 'retryable', 'running', 'scheduled') AND sequence_key IS NOT NULL;

-- Cancelled and discarded jobs, which may halt their sequence.
CREATE INDEX {{prefix}}job_sequence_failed_idx ON {{schema}}.{{prefix}}job (sequence_key)
    WHERE state IN ('cancelled', 'discarded') AND sequence_key IS NOT NULL;

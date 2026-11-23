DELETE FROM {{schema}}.{{prefix}}migration WHERE version = 2;

-- rate limiting
DROP TABLE {{schema}}.{{prefix}}rate_limit;
ALTER TABLE {{schema}}.{{prefix}}queue DROP COLUMN rate_limit_tat;

-- partitioned concurrency limits
DROP INDEX {{schema}}.{{prefix}}job_partition_idx;
ALTER TABLE {{schema}}.{{prefix}}job DROP COLUMN partition_key;

-- sequences
-- Version 1 never promotes pending jobs, so release them unordered rather than strand them.
UPDATE {{schema}}.{{prefix}}job
SET state = CASE WHEN scheduled_at > now() THEN 'scheduled' ELSE 'available' END::{{schema}}.{{prefix}}job_state
WHERE state = 'pending' AND sequence_key IS NOT NULL;
DROP INDEX {{schema}}.{{prefix}}job_sequence_failed_idx;
DROP INDEX {{schema}}.{{prefix}}job_sequence_live_idx;
DROP INDEX {{schema}}.{{prefix}}job_sequence_pending_idx;
ALTER TABLE {{schema}}.{{prefix}}job DROP COLUMN sequence_continue_on;
ALTER TABLE {{schema}}.{{prefix}}job DROP COLUMN sequence_key;

-- batches
DROP INDEX {{schema}}.{{prefix}}job_batch_idx;
ALTER TABLE {{schema}}.{{prefix}}job DROP COLUMN batch_id;
DROP TABLE {{schema}}.{{prefix}}batch;

-- durable periodic jobs
DROP TABLE {{schema}}.{{prefix}}periodic_job;

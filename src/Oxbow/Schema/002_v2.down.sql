DELETE FROM {{schema}}.{{prefix}}migration WHERE version = 2;

-- sequences
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

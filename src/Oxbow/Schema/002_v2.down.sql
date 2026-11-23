DELETE FROM {{schema}}.{{prefix}}migration WHERE version = 2;

-- batches
DROP INDEX {{schema}}.{{prefix}}job_batch_idx;
ALTER TABLE {{schema}}.{{prefix}}job DROP COLUMN batch_id;
DROP TABLE {{schema}}.{{prefix}}batch;

-- durable periodic jobs
DROP TABLE {{schema}}.{{prefix}}periodic_job;

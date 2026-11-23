DELETE FROM {{schema}}.{{prefix}}migration WHERE version = 2;

-- durable periodic jobs
DROP TABLE {{schema}}.{{prefix}}periodic_job;

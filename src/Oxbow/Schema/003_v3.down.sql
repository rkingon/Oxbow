DELETE FROM {{schema}}.{{prefix}}migration WHERE version = 3;

DROP TABLE IF EXISTS {{schema}}.{{prefix}}job_step;

CREATE INDEX IF NOT EXISTS {{prefix}}job_args_idx ON {{schema}}.{{prefix}}job USING gin (args);
CREATE INDEX IF NOT EXISTS {{prefix}}job_metadata_idx ON {{schema}}.{{prefix}}job USING gin (metadata);

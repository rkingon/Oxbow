DROP TABLE {{schema}}.{{prefix}}migration;
DROP TABLE {{schema}}.{{prefix}}queue;
DROP TABLE {{schema}}.{{prefix}}leader;
DROP TABLE {{schema}}.{{prefix}}job;
DROP FUNCTION {{schema}}.{{prefix}}job_state_in_bitmask(bit(8), {{schema}}.{{prefix}}job_state);
DROP TYPE {{schema}}.{{prefix}}job_state;

GRANT USAGE ON SCHEMA {{schema}} TO {{role}};
GRANT SELECT, INSERT, UPDATE, DELETE ON {{schema}}.{{prefix}}job, {{schema}}.{{prefix}}queue, {{schema}}.{{prefix}}leader TO {{role}};
GRANT SELECT ON {{schema}}.{{prefix}}migration TO {{role}};
GRANT USAGE ON SEQUENCE {{schema}}.{{prefix}}job_id_seq TO {{role}};
GRANT EXECUTE ON FUNCTION {{schema}}.{{prefix}}job_state_in_bitmask(bit(8), {{schema}}.{{prefix}}job_state) TO {{role}};

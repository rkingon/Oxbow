GRANT SELECT, INSERT, UPDATE, DELETE ON {{schema}}.{{prefix}}batch TO {{role}};
GRANT USAGE ON SEQUENCE {{schema}}.{{prefix}}batch_id_seq TO {{role}};

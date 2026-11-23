-- durable periodic jobs
GRANT SELECT, INSERT, UPDATE, DELETE ON {{schema}}.{{prefix}}periodic_job TO {{role}};

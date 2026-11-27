-- GIN never removes an entry once a key has been indexed: vacuum empties its posting list and leaves it in place. Every
-- distinct value ever written to args or metadata (IDs, outputs, heartbeat times) stays in these indexes, so they grow
-- with every job inserted, not with the jobs kept, until a REINDEX. Nothing in Oxbow queries args by containment, and a
-- metadata filter scans only the retained rows.
DROP INDEX IF EXISTS {{schema}}.{{prefix}}job_args_idx;
DROP INDEX IF EXISTS {{schema}}.{{prefix}}job_metadata_idx;

INSERT INTO {{schema}}.{{prefix}}migration (version) VALUES (3);

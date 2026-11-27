-- GIN never removes an entry once a key has been indexed: vacuum empties its posting list and leaves it in place. Every
-- distinct value ever written to args stays in this index, so it grows with every job inserted, not with the jobs kept,
-- until a REINDEX. Nothing in Oxbow queries args by containment.
DROP INDEX IF EXISTS {{schema}}.{{prefix}}job_args_idx;

INSERT INTO {{schema}}.{{prefix}}migration (version) VALUES (3);

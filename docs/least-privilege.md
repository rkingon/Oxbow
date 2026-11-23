# Least privilege

Workers and clients run as a role that isn't a superuser, doesn't own Oxbow's tables, and can't create objects
(not even temporary tables). This is the exact set of grants, from `OxbowSchema.GetGrants("app_role")`:

```sql
GRANT USAGE ON SCHEMA public TO "app_role";
GRANT SELECT, INSERT, UPDATE, DELETE ON public.oxbow_job, public.oxbow_queue, public.oxbow_leader TO "app_role";
GRANT SELECT ON public.oxbow_migration TO "app_role";
GRANT USAGE ON SEQUENCE public.oxbow_job_id_seq TO "app_role";
GRANT EXECUTE ON FUNCTION public.oxbow_job_state_in_bitmask(bit(8), public.oxbow_job_state) TO "app_role";
-- version 2: batches
GRANT SELECT, INSERT, UPDATE, DELETE ON public.oxbow_batch TO "app_role";
GRANT USAGE ON SEQUENCE public.oxbow_batch_id_seq TO "app_role";
-- version 2: durable periodic jobs
GRANT SELECT, INSERT, UPDATE, DELETE ON public.oxbow_periodic_job TO "app_role";
-- version 2: rate limiting
GRANT SELECT, INSERT, UPDATE, DELETE ON public.oxbow_rate_limit TO "app_role";
```

LISTEN and NOTIFY need no grant.

## What Oxbow assumes about your session

Nothing. Every identifier is schema-qualified, so `search_path` doesn't matter. Oxbow never calls `SET` or
`set_config`, never creates temporary tables, and sends enum, JSON and bit values as text or integers cast in SQL,
so it doesn't need type mappings registered on your `NpgsqlDataSource`.

## Row-level security

Oxbow's tables don't enable RLS. Your other tables can use forced RLS freely; the integration tests insert jobs in the
same transaction as writes to a `FORCE ROW LEVEL SECURITY` table, under a tenant setting.

If you enable RLS on Oxbow's own tables, add a policy that lets the runtime role see and change every row, since
workers fetch jobs for all tenants:

```sql
CREATE POLICY oxbow_runtime ON public.oxbow_job TO app_role USING (true) WITH CHECK (true);
```

## How it's tested

The integration suite creates a role with `LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE`, revokes `CREATE` on `public`
and `TEMPORARY` on the database, grants exactly the above, and runs every test as that role.

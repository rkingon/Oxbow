using Npgsql;

namespace Oxbow.IntegrationTests;

public sealed class SchemaTests(PostgresFixture database)
{
    [Fact]
    public async Task Up_then_down_removes_everything_it_created()
    {
        const string prefix = "roundtrip_";
        foreach (var migration in OxbowSchema.GetMigrations(0, tablePrefix: prefix))
        {
            await database.ExecuteAsOwnerAsync(migration.Sql);
        }

        Assert.Equal(OxbowSchema.LatestVersion, await database.ScalarAsOwnerAsync<int>($"SELECT max(version) FROM public.{prefix}migration"));

        foreach (var migration in OxbowSchema.GetMigrations(OxbowSchema.LatestVersion, 0, tablePrefix: prefix))
        {
            await database.ExecuteAsOwnerAsync(migration.Sql);
        }

        var remaining = await database.ScalarAsOwnerAsync<long>($"""
            SELECT (SELECT count(*) FROM pg_class WHERE relname LIKE '{prefix}%')
                 + (SELECT count(*) FROM pg_type WHERE typname LIKE '{prefix}%')
                 + (SELECT count(*) FROM pg_proc WHERE proname LIKE '{prefix}%')
            """);
        Assert.Equal(0, remaining);
    }

    [Fact]
    public async Task Each_version_upgrades_from_the_previous_one_with_its_grants_and_back()
    {
        // Replays migrations the way a consumer's history does: each version's scripts and grants, in order.
        const string prefix = "upgrade_";
        for (var version = 1; version <= OxbowSchema.LatestVersion; version++)
        {
            foreach (var migration in OxbowSchema.GetMigrations(version - 1, version, tablePrefix: prefix))
            {
                await database.ExecuteAsOwnerAsync(migration.Sql);
            }

            var grants = OxbowSchema.GetGrants(PostgresFixture.AppRole, version, tablePrefix: prefix);
            if (!string.IsNullOrWhiteSpace(grants))
            {
                await database.ExecuteSerializedAsOwnerAsync(grants);
            }

            Assert.Equal(version, await MarkerAsync(prefix));
        }

        for (var version = OxbowSchema.LatestVersion; version >= 1; version--)
        {
            foreach (var migration in OxbowSchema.GetMigrations(version, version - 1, tablePrefix: prefix))
            {
                await database.ExecuteAsOwnerAsync(migration.Sql);
            }

            Assert.Equal(version - 1, await MarkerAsync(prefix));
        }

        Assert.Equal(0L, await database.ScalarAsOwnerAsync<long>($"SELECT count(*) FROM pg_class WHERE relname LIKE '{prefix}%'"));
    }

    [Fact]
    public async Task Start_on_an_older_schema_names_the_upgrade()
    {
        const string prefix = "older_";
        foreach (var migration in OxbowSchema.GetMigrations(0, OxbowSchema.LatestVersion - 1, tablePrefix: prefix))
        {
            await database.ExecuteAsOwnerAsync(migration.Sql);
        }

        for (var version = 1; version < OxbowSchema.LatestVersion; version++)
        {
            await database.ExecuteSerializedAsOwnerAsync(OxbowSchema.GetGrants(PostgresFixture.AppRole, version, tablePrefix: prefix));
        }

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            TestApp.StartAsync(database, prefix, register: b => b.AddWorker<RecordArgs, RecordWorker>()));
        Assert.Contains(
            $"OxbowSchema.GetMigrations({OxbowSchema.LatestVersion - 1}, {OxbowSchema.LatestVersion})",
            ex.Message,
            StringComparison.Ordinal);
    }

    private async Task<int> MarkerAsync(string prefix) =>
        await database.ScalarAsOwnerAsync<bool>($"SELECT to_regclass('public.{prefix}migration') IS NOT NULL")
            ? await database.ScalarAsOwnerAsync<int>($"SELECT coalesce(max(version), 0) FROM public.{prefix}migration")
            : 0;

    [Fact]
    public async Task Every_created_object_carries_the_prefix()
    {
        var prefix = await database.CreateSchemaAsync();
        var unprefixed = await database.ScalarAsOwnerAsync<long>($"""
            SELECT count(*) FROM pg_indexes
            WHERE tablename LIKE '{prefix}%' AND indexname NOT LIKE '{prefix}%'
            """);
        Assert.Equal(0, unprefixed);
    }

    [Fact]
    public async Task No_job_index_is_a_GIN()
    {
        // A GIN keeps an entry for every distinct value ever indexed, so one over per-job values grows without bound.
        Assert.Equal("", await GinIndexesAsync(await database.CreateSchemaAsync()));
    }

    [Fact]
    public async Task Version_3_upgrades_and_rolls_back_with_jobs_in_place()
    {
        const string prefix = "v3data_";
        await MigrateAsync(prefix, 0, 2);
        await database.ExecuteAsOwnerAsync($"""
            INSERT INTO public.{prefix}job (args, kind, metadata, state, finalized_at)
            SELECT jsonb_build_object('n', i), 'k', jsonb_build_object('tenant', 't' || i % 3, 'output', i),
                   CASE WHEN i % 2 = 0 THEN 'completed' ELSE 'available' END::public.{prefix}job_state,
                   CASE WHEN i % 2 = 0 THEN now() END
            FROM generate_series(1, 300) i
            """);
        var jobs = await FingerprintAsync(prefix);

        await MigrateAsync(prefix, 2, 3);
        Assert.Equal("", await GinIndexesAsync(prefix));
        Assert.Equal(jobs, await FingerprintAsync(prefix));
        await database.ExecuteAsOwnerAsync($"""
            INSERT INTO public.{prefix}job_step (job_id, name, output)
            SELECT id, 'step', jsonb_build_object('n', id) FROM public.{prefix}job
            """);
        Assert.Equal(100L, await database.ScalarAsOwnerAsync<long>(
            $$"""SELECT count(*) FROM public.{{prefix}}job WHERE metadata @> '{"tenant": "t1"}'"""));

        await MigrateAsync(prefix, 3, 2);
        Assert.Equal($"{prefix}job_args_idx:valid,{prefix}job_metadata_idx:valid", await GinIndexesAsync(prefix));
        Assert.Equal(jobs, await FingerprintAsync(prefix));
        Assert.Equal(2, await MarkerAsync(prefix));

        await MigrateAsync(prefix, 2, 3);
        Assert.Equal("", await GinIndexesAsync(prefix));
        Assert.Equal(jobs, await FingerprintAsync(prefix));

        await MigrateAsync(prefix, 3, 0);
    }

    private async Task MigrateAsync(string prefix, int from, int to)
    {
        foreach (var migration in OxbowSchema.GetMigrations(from, to, tablePrefix: prefix))
        {
            await database.ExecuteAsOwnerAsync(migration.Sql);
        }
    }

    private async Task<string?> FingerprintAsync(string prefix) =>
        await database.ScalarAsOwnerAsync<string>($"""
            SELECT count(*) || ':' || sum(hashtext(id || state::text || args::text || metadata::text))
            FROM public.{prefix}job
            """);

    private async Task<string?> GinIndexesAsync(string prefix) =>
        await database.ScalarAsOwnerAsync<string>($"""
            SELECT coalesce(string_agg(i.relname || CASE WHEN x.indisvalid THEN ':valid' ELSE ':invalid' END, ',' ORDER BY i.relname), '')
            FROM pg_index x
            JOIN pg_class i ON i.oid = x.indexrelid
            JOIN pg_am am ON am.oid = i.relam
            WHERE x.indrelid = 'public.{prefix}job'::regclass AND am.amname = 'gin'
            """);

    [Fact]
    public async Task Start_fails_with_instructions_when_the_schema_is_missing()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            TestApp.StartAsync(database, "missing_", register: b => b.AddWorker<RecordArgs, RecordWorker>()));
        Assert.Contains($"OxbowSchema.GetMigrations(0, {OxbowSchema.LatestVersion})", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_app_role_cannot_create_objects()
    {
        await using var connection = new NpgsqlConnection(database.AppConnectionString);
        await connection.OpenAsync();
        await using var create = new NpgsqlCommand("CREATE TABLE public.should_fail (id int)", connection);
        var ex = await Assert.ThrowsAsync<PostgresException>(() => create.ExecuteNonQueryAsync());
        Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, ex.SqlState);

        await using var temp = new NpgsqlCommand("CREATE TEMP TABLE should_fail (id int)", connection);
        ex = await Assert.ThrowsAsync<PostgresException>(() => temp.ExecuteNonQueryAsync());
        Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, ex.SqlState);
    }
}

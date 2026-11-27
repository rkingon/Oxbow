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
        var prefix = await database.CreateSchemaAsync();
        var gin = await database.ScalarAsOwnerAsync<string>($"""
            SELECT coalesce(string_agg(i.relname, ',' ORDER BY i.relname), '')
            FROM pg_index x
            JOIN pg_class i ON i.oid = x.indexrelid
            JOIN pg_am am ON am.oid = i.relam
            WHERE x.indrelid = 'public.{prefix}job'::regclass AND am.amname = 'gin'
            """);
        Assert.Equal("", gin);
    }

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

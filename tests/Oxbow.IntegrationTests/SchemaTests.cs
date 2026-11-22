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

        Assert.Equal(1, await database.ScalarAsOwnerAsync<int>($"SELECT max(version) FROM public.{prefix}migration"));

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
    public async Task Start_fails_with_instructions_when_the_schema_is_missing()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            TestApp.StartAsync(database, "missing_", register: b => b.AddWorker<RecordArgs, RecordWorker>()));
        Assert.Contains("OxbowSchema.GetMigrations(0, 1)", ex.Message, StringComparison.Ordinal);
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

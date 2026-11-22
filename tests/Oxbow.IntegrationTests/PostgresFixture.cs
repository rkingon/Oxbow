using Npgsql;
using Testcontainers.PostgreSql;

[assembly: AssemblyFixture(typeof(Oxbow.IntegrationTests.PostgresFixture))]

namespace Oxbow.IntegrationTests;

/// <summary>
/// One Postgres container for the run. The superuser plays the schema owner; everything Oxbow does at runtime runs as
/// <see cref="AppRole"/>: no superuser, no ownership, no CREATE anywhere, no TEMP.
/// </summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    public const string AppRole = "oxbow_app";
    private const string AppPassword = "oxbow_app_password";
    private static int prefixCounter;

    // Concurrent GRANTs on the same schema race on its catalog row ("tuple concurrently updated").
    private readonly SemaphoreSlim _ddl = new(1, 1);

    private readonly PostgreSqlContainer _container =
        new PostgreSqlBuilder(Environment.GetEnvironmentVariable("OXBOW_POSTGRES_IMAGE") ?? "postgres:17-alpine").Build();

    public string OwnerConnectionString => _container.GetConnectionString();

    public string AppConnectionString => new NpgsqlConnectionStringBuilder(OwnerConnectionString)
    {
        Username = AppRole,
        Password = AppPassword,
    }.ConnectionString;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        var database = new NpgsqlConnectionStringBuilder(OwnerConnectionString).Database;
        await ExecuteAsOwnerAsync($"""
            CREATE ROLE {AppRole} LOGIN PASSWORD '{AppPassword}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
            REVOKE CREATE ON SCHEMA public FROM PUBLIC;
            REVOKE TEMPORARY ON DATABASE "{database}" FROM PUBLIC;

            -- A consumer table under forced row-level security, written in the same transactions as job inserts.
            CREATE TABLE orders (id bigserial PRIMARY KEY, tenant text NOT NULL, item text NOT NULL);
            ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
            ALTER TABLE orders FORCE ROW LEVEL SECURITY;
            CREATE POLICY orders_tenant ON orders USING (tenant = current_setting('app.tenant', true))
                WITH CHECK (tenant = current_setting('app.tenant', true));
            GRANT SELECT, INSERT ON orders TO {AppRole};
            GRANT USAGE ON SEQUENCE orders_id_seq TO {AppRole};

            CREATE TABLE widgets (id bigserial PRIMARY KEY, name text NOT NULL);
            GRANT SELECT, INSERT ON widgets TO {AppRole};
            GRANT USAGE ON SEQUENCE widgets_id_seq TO {AppRole};
            """);
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    /// <summary>Installs a fresh copy of the schema under a unique prefix and grants it to the app role.</summary>
    public async Task<string> CreateSchemaAsync()
    {
        var prefix = $"t{Interlocked.Increment(ref prefixCounter)}_";
        await _ddl.WaitAsync();
        try
        {
            foreach (var migration in OxbowSchema.GetMigrations(0, tablePrefix: prefix))
            {
                await ExecuteAsOwnerAsync(migration.Sql);
            }

            await ExecuteAsOwnerAsync(OxbowSchema.GetGrants(AppRole, tablePrefix: prefix));
        }
        finally
        {
            _ddl.Release();
        }

        return prefix;
    }

    public async Task ExecuteAsOwnerAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(OwnerConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<T?> ScalarAsOwnerAsync<T>(string sql)
    {
        await using var connection = new NpgsqlConnection(OwnerConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync();
        return result is DBNull or null ? default : (T)result;
    }
}

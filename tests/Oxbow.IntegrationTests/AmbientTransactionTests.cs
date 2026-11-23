using Microsoft.EntityFrameworkCore;
using Npgsql;
using Oxbow.Abstractions.Sample;
using static Oxbow.IntegrationTests.EntityFrameworkCoreTests;

namespace Oxbow.IntegrationTests;

public sealed class AmbientTransactionTests(PostgresFixture database)
{
    public sealed class TenantContext
    {
        public string? Tenant { get; set; }
    }

    private sealed class StampTenant(TenantContext tenant) : IJobInsertMiddleware
    {
        public Task<IReadOnlyList<InsertResult>> InvokeAsync(JobInsertContext context, JobInsertDelegate next)
        {
            foreach (var job in context.Jobs)
            {
                job.Metadata["tenant"] = tenant.Tenant ?? "none";
            }

            return next(context);
        }
    }

    private Task<TestApp> StartAsync(string prefix, bool requireAmbient = false) =>
        TestApp.StartAsync(
            database,
            prefix,
            o =>
            {
                o.Queues["default"] = new QueueOptions();
                o.RequireAmbientTransaction = requireAmbient;
            },
            b =>
            {
                b.Services.AddDbContext<ShopContext>(o => o.UseNpgsql(database.AppConnectionString));
                b.Services.AddScoped<TenantContext>();
                b.Services.AddSingleton<ShippedOrders>();
                b.Services.AddScoped<PlaceOrder>();
                b.AddWorker<ShipOrder, ShipOrderWorker>()
                    .AddInsertMiddleware<StampTenant>(ServiceLifetime.Scoped)
                    .AddDbContextTransaction<ShopContext>();
            });

    [Fact]
    public async Task A_use_case_written_against_the_abstractions_commits_its_job_with_the_unit_of_work()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix);

        long jobId;
        await using (var scope = app.Host.Services.CreateAsyncScope())
        {
            scope.ServiceProvider.GetRequiredService<TenantContext>().Tenant = "acme";
            var db = scope.ServiceProvider.GetRequiredService<ShopContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            db.Widgets.Add(new Widget { Name = "order 1" });
            await db.SaveChangesAsync();
            jobId = await scope.ServiceProvider.GetRequiredService<PlaceOrder>().ExecuteAsync(1);
            Assert.Null(await app.Client.GetAsync(jobId));
            await transaction.CommitAsync();
        }

        var job = await app.WaitForStateAsync(jobId, JobState.Completed);
        Assert.Equal("acme", job.Metadata.GetProperty("tenant").GetString());
        Assert.Equal([1L], app.Host.Services.GetRequiredService<ShippedOrders>().Ids);
    }

    [Fact]
    public async Task A_use_cases_job_rolls_back_with_the_unit_of_work()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix);

        long jobId;
        await using (var scope = app.Host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<ShopContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            db.Widgets.Add(new Widget { Name = "order 2" });
            await db.SaveChangesAsync();
            jobId = await scope.ServiceProvider.GetRequiredService<PlaceOrder>().ExecuteAsync(2);
            await transaction.RollbackAsync();
        }

        Assert.Null(await app.Client.GetAsync(jobId));
    }

    [Fact]
    public async Task Concurrent_scopes_insert_in_their_own_transactions_with_their_own_tenants()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix);

        async Task<long> PlaceAsync(string tenant, long orderId, bool commit)
        {
            await using var scope = app.Host.Services.CreateAsyncScope();
            scope.ServiceProvider.GetRequiredService<TenantContext>().Tenant = tenant;
            var db = scope.ServiceProvider.GetRequiredService<ShopContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            var jobId = await scope.ServiceProvider.GetRequiredService<PlaceOrder>().ExecuteAsync(orderId);
            await Task.Delay(100);
            if (commit)
            {
                await transaction.CommitAsync();
            }

            return jobId;
        }

        var ids = await Task.WhenAll(PlaceAsync("acme", 10, commit: true), PlaceAsync("globex", 11, commit: false));

        var committed = await app.WaitForStateAsync(ids[0], JobState.Completed);
        Assert.Equal("acme", committed.Metadata.GetProperty("tenant").GetString());
        Assert.Null(await app.Client.GetAsync(ids[1]));
    }

    [Fact]
    public async Task Every_ambient_insert_joins_the_transaction()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix);

        await using var scope = app.Host.Services.CreateAsyncScope();
        var oxbow = scope.ServiceProvider.GetRequiredService<IOxbowClient>();
        var db = scope.ServiceProvider.GetRequiredService<ShopContext>();
        await using (var transaction = await db.Database.BeginTransactionAsync())
        {
            // EF's pooled connections loaded their types before this test created its schema's enum, which COPY writes.
            await ((NpgsqlConnection)db.Database.GetDbConnection()).ReloadTypesAsync();
            await oxbow.InsertManyAsync([new InsertManyItem(new ShipOrder(3))]);
            await oxbow.InsertManyFastAsync([new InsertManyItem(new ShipOrder(4))]);
            await oxbow.InsertBatchAsync([new InsertManyItem(new ShipOrder(5))]);
            await transaction.RollbackAsync();
        }

        Assert.Empty((await app.Client.ListAsync(new JobListQuery())).Jobs);
    }

    [Fact]
    public async Task Without_a_transaction_an_insert_commits_on_its_own()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix);

        await using var scope = app.Host.Services.CreateAsyncScope();
        var jobId = await scope.ServiceProvider.GetRequiredService<PlaceOrder>().ExecuteAsync(6);

        var job = await app.WaitForStateAsync(jobId, JobState.Completed);
        Assert.Equal("none", job.Metadata.GetProperty("tenant").GetString());
    }

    [Fact]
    public async Task Requiring_an_ambient_transaction_throws_without_one()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, requireAmbient: true);

        await using var scope = app.Host.Services.CreateAsyncScope();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => scope.ServiceProvider.GetRequiredService<PlaceOrder>().ExecuteAsync(7));
        Assert.Contains("RequireAmbientTransaction", ex.Message, StringComparison.Ordinal);
        await Assert.ThrowsAsync<InvalidOperationException>(() => app.Client.InsertAsync(new ShipOrder(7)));

        var db = scope.ServiceProvider.GetRequiredService<ShopContext>();
        await using var transaction = await db.Database.BeginTransactionAsync();
        var jobId = await scope.ServiceProvider.GetRequiredService<PlaceOrder>().ExecuteAsync(8);
        await transaction.CommitAsync();
        await app.WaitForStateAsync(jobId, JobState.Completed);
    }

    [Fact]
    public async Task Requiring_an_ambient_transaction_without_an_accessor_fails_on_start()
    {
        var prefix = await database.CreateSchemaAsync();

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => TestApp.StartAsync(database, prefix, o => o.RequireAmbientTransaction = true));

        Assert.Contains("IOxbowTransactionAccessor", ex.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Scoped_insert_middleware_runs_in_a_scope_of_its_own_outside_one()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix);

        var result = await app.Client.InsertAsync(new ShipOrder(9));

        Assert.Equal("none", result.Job.Metadata.GetProperty("tenant").GetString());
    }

    [Fact]
    public void Hosts_resolve_the_scoped_client_only_from_a_scope()
    {
        using var validated = new ServiceCollection()
            .AddLogging()
            .AddNpgsqlDataSource(database.AppConnectionString)
            .AddOxbow()
            .AddDbContextTransaction<ShopContext>()
            .Services
            .AddDbContext<ShopContext>(o => o.UseNpgsql(database.AppConnectionString))
            .BuildServiceProvider(validateScopes: true);

        Assert.Throws<InvalidOperationException>(() => validated.GetRequiredService<IOxbowClient>());
        Assert.NotNull(validated.GetRequiredService<IOxbowNpgsqlClient>());
    }
}

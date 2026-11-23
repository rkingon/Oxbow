using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace Oxbow.IntegrationTests;

public sealed class EntityFrameworkCoreTests(PostgresFixture database)
{
    public sealed class Widget
    {
        public long Id { get; set; }

        public required string Name { get; set; }
    }

    public sealed class ShopContext(DbContextOptions<ShopContext> options) : DbContext(options)
    {
        public DbSet<Widget> Widgets => Set<Widget>();

        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.Entity<Widget>(w =>
            {
                w.ToTable("widgets");
                w.Property(x => x.Id).HasColumnName("id");
                w.Property(x => x.Name).HasColumnName("name");
            });
    }

    private ShopContext CreateContext() =>
        new(new DbContextOptionsBuilder<ShopContext>().UseNpgsql(database.AppConnectionString).Options);

    [Fact]
    public async Task A_job_commits_with_the_contexts_changes()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(),
            b => b.AddWorker<RecordArgs, RecordWorker>());
        await using var db = CreateContext();

        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Widgets.Add(new Widget { Name = "sprocket" });
        await db.SaveChangesAsync();
        var result = await app.Client.InsertAsync(db, new RecordArgs("ef commit"));
        await transaction.CommitAsync();

        await app.WaitForStateAsync(result.Job.Id, JobState.Completed);
    }

    [Fact]
    public async Task A_job_rolls_back_with_the_contexts_changes()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        await using var db = CreateContext();

        long jobId;
        await using (var transaction = await db.Database.BeginTransactionAsync())
        {
            db.Widgets.Add(new Widget { Name = "rolled back" });
            await db.SaveChangesAsync();
            jobId = (await app.Client.InsertAsync(db, new RecordArgs("ef rollback"))).Job.Id;
            await transaction.RollbackAsync();
        }

        Assert.Null(await app.Client.GetAsync(jobId));
    }

    [Fact]
    public async Task Inserting_without_a_transaction_throws_and_names_the_fix()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        await using var db = CreateContext();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => app.Client.InsertAsync(db, new RecordArgs("no tx")));
        Assert.Contains("BeginTransactionAsync", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_migration_helpers_emit_the_schema_sql()
    {
        var builder = new MigrationBuilder("Npgsql.EntityFrameworkCore.PostgreSQL");
        builder.AddOxbowSchema(1, tablePrefix: "ef_").GrantOxbowAccess("app", 1, tablePrefix: "ef_");
        builder.RemoveOxbowSchema(1, tablePrefix: "ef_");

        var sql = builder.Operations.Cast<SqlOperation>().Select(o => o.Sql).ToList();
        Assert.Equal(OxbowSchema.GetMigrations(0, 1, tablePrefix: "ef_")[0].Sql, sql[0]);
        Assert.Equal(OxbowSchema.GetGrants("app", 1, tablePrefix: "ef_"), sql[1]);
        Assert.Equal(OxbowSchema.GetMigrations(1, 0, tablePrefix: "ef_")[0].Sql, sql[2]);
    }
}

using Oxbow.Testing;

namespace Oxbow.IntegrationTests;

public sealed class TestingPackageTests(PostgresFixture database)
{
    [Fact]
    public async Task Assertions_see_jobs_inside_an_uncommitted_transaction()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        var assert = new OxbowAssert(tablePrefix: prefix);

        await using var connection = await app.DataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await app.Client.InsertAsync(connection, transaction, new RecordArgs("asserted"), new InsertOptions { Queue = "q1", Tags = ["tag-1"] });

        var job = await assert.InsertedAsync<RecordArgs>(connection, transaction, new() { Args = a => a.Value == "asserted", Queue = "q1", Tags = ["tag-1"] });
        Assert.Equal("asserted", job.Args.Value);

        await assert.NotInsertedAsync<RecordArgs>(connection, transaction, new() { Queue = "other" });
        await assert.NotInsertedAsync<FailArgs>(connection, transaction);
        await Assert.ThrowsAsync<OxbowAssertionException>(() => assert.NotInsertedAsync<RecordArgs>(connection, transaction));

        await transaction.RollbackAsync();
        await Assert.ThrowsAsync<OxbowAssertionException>(() => assert.InsertedAsync<RecordArgs>(connection));
    }
}

namespace Oxbow.IntegrationTests;

public sealed class InsertManyFastTests(PostgresFixture database)
{
    [Fact]
    public async Task Copied_jobs_are_worked()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(),
            b => b.AddWorker<RecordArgs, RecordWorker>());

        var count = await app.Client.InsertManyFastAsync(Enumerable.Range(0, 200)
            .Select(i => new InsertManyItem(new RecordArgs($"copy-{i}"), new InsertOptions { Tags = ["fast-path"], Priority = (i % 4) + 1 })));
        Assert.Equal(200, count);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (app.Recorder.RunCounts.Count < 200)
        {
            await Task.Delay(50, cts.Token);
        }

        var page = await app.Client.ListAsync(new JobListQuery { Limit = 1 });
        Assert.Equal(["fast-path"], page.Jobs[0].Tags);
    }

    [Fact]
    public async Task Copied_jobs_roll_back_with_the_callers_transaction_and_keep_their_schedule()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        await using var connection = await app.DataSource.OpenConnectionAsync();
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await app.Client.InsertManyFastAsync(connection, transaction, [new InsertManyItem(new RecordArgs("rolled back"))]);
            await transaction.RollbackAsync();
        }

        Assert.Empty((await app.Client.ListAsync(new JobListQuery())).Jobs);

        await app.Client.InsertManyFastAsync([new InsertManyItem(new RecordArgs("later"), new InsertOptions { ScheduledAt = DateTimeOffset.UtcNow.AddHours(1) })]);
        Assert.Equal(JobState.Scheduled, (await app.Client.ListAsync(new JobListQuery())).Jobs.Single().State);
    }

    [Fact]
    public async Task Unique_jobs_are_rejected()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => app.Client.InsertManyFastAsync([new InsertManyItem(new UniqueArgs("c", "t"))]));
        Assert.Contains("InsertManyAsync", ex.Message, StringComparison.Ordinal);
    }
}

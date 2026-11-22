namespace Oxbow.IntegrationTests;

public sealed class UniqueTests(PostgresFixture database)
{
    [Fact]
    public async Task A_duplicate_returns_the_existing_job()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        var first = await app.Client.InsertAsync(new UniqueArgs("customer-1", "trace-a"));
        var second = await app.Client.InsertAsync(new UniqueArgs("customer-1", "trace-b"));
        var other = await app.Client.InsertAsync(new UniqueArgs("customer-2", "trace-a"));

        Assert.False(first.UniqueSkippedAsDuplicate);
        Assert.True(second.UniqueSkippedAsDuplicate);
        Assert.Equal(first.Job.Id, second.Job.Id);
        Assert.NotEqual(first.Job.Id, other.Job.Id);
    }

    [Fact]
    public async Task Concurrent_inserts_produce_exactly_one_job()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        var results = await Task.WhenAll(Enumerable.Range(0, 25)
            .Select(i => app.Client.InsertAsync(new UniqueArgs("race", $"trace-{i}"))));

        Assert.Single(results.Select(r => r.Job.Id).Distinct());
        Assert.Single(results, r => !r.UniqueSkippedAsDuplicate);
    }

    [Fact]
    public async Task A_cancelled_job_does_not_block_a_new_one()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        var first = await app.Client.InsertAsync(new UniqueArgs("customer-1", "a"));
        await app.Client.CancelAsync(first.Job.Id);
        var second = await app.Client.InsertAsync(new UniqueArgs("customer-1", "b"));

        Assert.False(second.UniqueSkippedAsDuplicate);
        Assert.NotEqual(first.Job.Id, second.Job.Id);
    }

    [Fact]
    public async Task By_period_allows_one_per_period()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        var unique = new InsertOptions { Unique = new UniqueOptions { ByPeriod = TimeSpan.FromHours(1) } };
        var hour = new DateTimeOffset(2030, 1, 1, 10, 0, 0, TimeSpan.Zero);

        var a = await app.Client.InsertAsync(new RecordArgs("a"), unique with { ScheduledAt = hour.AddMinutes(5) });
        var b = await app.Client.InsertAsync(new RecordArgs("b"), unique with { ScheduledAt = hour.AddMinutes(55) });
        var c = await app.Client.InsertAsync(new RecordArgs("c"), unique with { ScheduledAt = hour.AddMinutes(65) });

        Assert.True(b.UniqueSkippedAsDuplicate);
        Assert.False(c.UniqueSkippedAsDuplicate);
        Assert.Equal(a.Job.Id, b.Job.Id);
    }
}

namespace Oxbow.IntegrationTests;

public sealed class ConcurrencyTests(PostgresFixture database)
{
    [Fact]
    public async Task Many_workers_on_two_nodes_run_every_job_exactly_once()
    {
        var prefix = await database.CreateSchemaAsync();
        var recorder = new JobRecorder();
        void Configure(OxbowOptions o) => o.Queues["default"] = new QueueOptions { MaxWorkers = 20 };
        await using var a = await TestApp.StartAsync(database, prefix, Configure, b => b.AddWorker<RecordArgs, RecordWorker>(), recorder);
        await using var b = await TestApp.StartAsync(database, prefix, Configure, b => b.AddWorker<RecordArgs, RecordWorker>(), recorder);

        var results = await a.Client.InsertManyAsync(Enumerable.Range(0, 500).Select(i => new InsertManyItem(new RecordArgs($"job-{i}"))));
        Assert.Equal(500, results.Count);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (recorder.RunCounts.Count < 500 && !cts.IsCancellationRequested)
        {
            await Task.Delay(100);
        }

        Assert.Equal(500, recorder.RunCounts.Count);
        Assert.All(recorder.RunCounts.Values, count => Assert.Equal(1, count));
    }
}

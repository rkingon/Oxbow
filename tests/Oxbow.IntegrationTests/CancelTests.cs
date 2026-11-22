namespace Oxbow.IntegrationTests;

public sealed class CancelTests(PostgresFixture database)
{
    [Fact]
    public async Task A_job_cancelled_before_it_starts_never_runs()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var inserter = await TestApp.StartAsync(database, prefix);
        var result = await inserter.Client.InsertAsync(new RecordArgs("never"));

        var cancelled = await inserter.Client.CancelAsync(result.Job.Id);
        Assert.Equal(JobState.Cancelled, cancelled!.State);

        var recorder = new JobRecorder();
        await using var worker = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(),
            b => b.AddWorker<RecordArgs, RecordWorker>(), recorder);
        await Task.Delay(1000);
        Assert.False(recorder.RunCounts.ContainsKey(result.Job.Id));
    }

    [Fact]
    public async Task A_running_job_is_signalled_across_nodes_and_ends_cancelled()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var worker = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(),
            b => b.AddWorker<BlockArgs, BlockWorker>());
        await using var other = await TestApp.StartAsync(database, prefix);

        var result = await other.Client.InsertAsync(new BlockArgs());
        await worker.Recorder.WaitStartedAsync(result.Job.Id);

        var afterRequest = await other.Client.CancelAsync(result.Job.Id);
        Assert.Equal(JobState.Running, afterRequest!.State);

        var job = await other.WaitForStateAsync(result.Job.Id, JobState.Cancelled, TimeSpan.FromSeconds(5));
        Assert.NotNull(job.FinalizedAt);
    }

    [Fact]
    public async Task A_running_job_is_cancelled_by_polling_when_notifications_are_off()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var worker = await TestApp.StartAsync(database, prefix, o =>
        {
            o.Queues["default"] = new QueueOptions();
            o.PollOnly = true;
        }, b => b.AddWorker<BlockArgs, BlockWorker>());

        var result = await worker.Client.InsertAsync(new BlockArgs());
        await worker.Recorder.WaitStartedAsync(result.Job.Id);
        await worker.Client.CancelAsync(result.Job.Id);

        await worker.WaitForStateAsync(result.Job.Id, JobState.Cancelled, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task A_worker_that_ignores_the_signal_finishes_and_stays_completed()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(),
            b => b.AddWorker<RecordArgs, RecordWorker>());

        var result = await app.Client.InsertAsync(new RecordArgs("fast"));
        await app.WaitForStateAsync(result.Job.Id, JobState.Completed);

        var afterCancel = await app.Client.CancelAsync(result.Job.Id);
        Assert.Equal(JobState.Completed, afterCancel!.State);
    }
}

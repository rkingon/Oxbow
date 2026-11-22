namespace Oxbow.IntegrationTests;

public sealed class WorkTests(PostgresFixture database)
{
    private static void Register(IOxbowBuilder b) => b
        .AddWorker<RecordArgs, RecordWorker>()
        .AddWorker<FailArgs, FailWorker>()
        .AddWorker<SnoozeArgs, SnoozeWorker>()
        .AddWorker<SelfCancelArgs, SelfCancelWorker>()
        .AddWorker<SlowArgs, SlowWorker>();

    private async Task<TestApp> StartAsync(Action<OxbowOptions>? configure = null) =>
        await TestApp.StartAsync(database, await database.CreateSchemaAsync(), o =>
        {
            o.Queues["default"] = new QueueOptions { MaxWorkers = 10 };
            configure?.Invoke(o);
        }, Register);

    [Fact]
    public async Task A_job_is_worked_and_completed()
    {
        await using var app = await StartAsync();
        var result = await app.Client.InsertAsync(new RecordArgs("hello"));

        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Completed);
        Assert.Equal(1, job.Attempt);
        Assert.NotNull(job.FinalizedAt);
        Assert.Contains("hello", app.Recorder.Events);
    }

    [Fact]
    public async Task A_failing_job_records_each_error_and_is_discarded_after_max_attempts()
    {
        await using var app = await StartAsync();
        var result = await app.Client.InsertAsync(new FailArgs("boom"), new InsertOptions { MaxAttempts = 3 });

        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Discarded);
        Assert.Equal(3, job.Attempt);
        Assert.Equal(3, job.Errors.Count);
        Assert.Equal([1, 2, 3], job.Errors.Select(e => e.Attempt));
        Assert.All(job.Errors, e => Assert.Equal("InvalidOperationException: boom", e.Error));
    }

    [Fact]
    public async Task Snoozing_does_not_consume_attempts()
    {
        await using var app = await StartAsync();
        var result = await app.Client.InsertAsync(new SnoozeArgs(3), new InsertOptions { MaxAttempts = 1 });

        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Completed);
        Assert.Equal(1, job.Attempt);
        Assert.Equal(3, job.Metadata.GetProperty("snoozes").GetInt32());
        Assert.Empty(job.Errors);
    }

    [Fact]
    public async Task A_worker_can_cancel_its_own_job()
    {
        await using var app = await StartAsync();
        var result = await app.Client.InsertAsync(new SelfCancelArgs());

        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Cancelled);
        Assert.Single(job.Errors);
        Assert.Contains("Not worth retrying.", job.Errors[0].Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_timeout_cancels_the_token_and_fails_the_attempt()
    {
        await using var app = await StartAsync();
        var result = await app.Client.InsertAsync(new SlowArgs(200));

        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Retryable);
        Assert.StartsWith("TimeoutException", job.Errors.Single().Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Inserting_a_kind_with_no_worker_throws_unless_the_check_is_skipped()
    {
        await using var app = await StartAsync();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => app.Client.InsertAsync(new OrphanArgs()));
        Assert.Contains("SkipUnknownKindCheck", ex.Message, StringComparison.Ordinal);

        await using var skipping = await StartAsync(o => o.SkipUnknownKindCheck = true);
        await skipping.Client.InsertAsync(new OrphanArgs());
    }

    [Fact]
    public async Task A_fetched_job_with_no_worker_fails_its_attempt_and_retries()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var inserter = await TestApp.StartAsync(database, prefix);
        await using var worker = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(), Register);

        var result = await inserter.Client.InsertAsync(new OrphanArgs());

        var job = await worker.WaitForStateAsync(result.Job.Id, JobState.Retryable);
        Assert.Equal(1, job.Attempt);
        Assert.StartsWith("UnknownJobKindException", job.Errors.Single().Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Scheduled_jobs_wait_for_their_time()
    {
        await using var app = await StartAsync();
        var result = await app.Client.InsertAsync(new RecordArgs("later"), new InsertOptions { ScheduledAt = DateTimeOffset.UtcNow.AddSeconds(2) });
        Assert.Equal(JobState.Scheduled, result.Job.State);

        await Task.Delay(1000);
        Assert.Equal(JobState.Scheduled, (await app.Client.GetAsync(result.Job.Id))!.State);

        await app.WaitForStateAsync(result.Job.Id, JobState.Completed);
    }

    [Fact]
    public async Task Multiple_queues_are_worked_independently()
    {
        await using var app = await StartAsync(o => o.Queues["email"] = new QueueOptions { MaxWorkers = 2 });
        var a = await app.Client.InsertAsync(new RecordArgs("default queue"));
        var b = await app.Client.InsertAsync(new RecordArgs("email queue"), new InsertOptions { Queue = "email" });

        await app.WaitForStateAsync(a.Job.Id, JobState.Completed);
        var job = await app.WaitForStateAsync(b.Job.Id, JobState.Completed);
        Assert.Equal("email", job.Queue);
    }

    [Fact]
    public async Task Jobs_in_queues_this_node_does_not_run_are_left_alone()
    {
        await using var app = await StartAsync();
        var result = await app.Client.InsertAsync(new RecordArgs("elsewhere"), new InsertOptions { Queue = "elsewhere" });

        await Task.Delay(1000);
        Assert.Equal(JobState.Available, (await app.Client.GetAsync(result.Job.Id))!.State);
    }
}

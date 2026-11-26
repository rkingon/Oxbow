using Oxbow.Processing;

namespace Oxbow.IntegrationTests;

public sealed class WorkTests(PostgresFixture database)
{
    private static void Register(IOxbowBuilder b) => b
        .AddWorker<RecordArgs, RecordWorker>()
        .AddWorker<FailArgs, FailWorker>()
        .AddWorker<SnoozeArgs, SnoozeWorker>()
        .AddWorker<SelfCancelArgs, SelfCancelWorker>()
        .AddWorker<SlowArgs, SlowWorker>()
        .AddWorker<RetryAfterArgs, RetryAfterWorker>();

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
    public async Task A_worker_chosen_retry_records_the_error_and_waits_exactly_its_delay_over_GetNextRetry()
    {
        await using var app = await StartAsync();
        var delay = TimeSpan.FromMinutes(30);
        var before = DateTimeOffset.UtcNow;
        var result = await app.Client.InsertAsync(new RetryAfterArgs(delay));

        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Retryable);
        var after = DateTimeOffset.UtcNow;
        Assert.Equal(1, job.Attempt);
        var error = Assert.Single(job.Errors);
        Assert.Equal(1, error.Attempt);
        Assert.Equal("JobRetryException: vendor.rate_limited", error.Error);
        Assert.Contains(nameof(RetryAfterWorker.WorkAsync), error.Trace, StringComparison.Ordinal);
        Assert.InRange(job.ScheduledAt, before + delay, after + delay);
    }

    [Fact]
    public async Task A_worker_chosen_retry_is_traced_as_an_error_with_its_delay()
    {
        var spans = new System.Collections.Concurrent.ConcurrentQueue<System.Diagnostics.Activity>();
        using var listener = new System.Diagnostics.ActivityListener
        {
            ShouldListenTo = source => source.Name == "Oxbow",
            Sample = (ref System.Diagnostics.ActivityCreationOptions<System.Diagnostics.ActivityContext> _) =>
                System.Diagnostics.ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Enqueue,
        };
        System.Diagnostics.ActivitySource.AddActivityListener(listener);
        await using var app = await StartAsync();

        // Job IDs repeat across the tests' schemas, so the span is found by the trace its insert started.
        using var source = new System.Diagnostics.ActivitySource("Oxbow");
        InsertResult result;
        System.Diagnostics.ActivityTraceId traceId;
        using (var test = source.StartActivity("test")!)
        {
            traceId = test.TraceId;
            result = await app.Client.InsertAsync(new RetryAfterArgs(TimeSpan.FromMinutes(30)));
        }

        await app.WaitForStateAsync(result.Job.Id, JobState.Retryable);

        var span = Assert.Single(spans, s => s.OperationName == "oxbow.work" && s.Links.Any(l => l.Context.TraceId == traceId));
        Assert.Equal("errored", span.GetTagItem("oxbow.job.outcome"));
        Assert.Equal(System.Diagnostics.ActivityStatusCode.Error, span.Status);
        Assert.Equal(TimeSpan.FromMinutes(30).TotalSeconds, (double)span.GetTagItem("oxbow.job.retry_delay")!);
    }

    [Fact]
    public async Task A_worker_chosen_retry_records_the_cause_type_and_trace()
    {
        await using var app = await StartAsync();
        var result = await app.Client.InsertAsync(new RetryAfterArgs(TimeSpan.FromMinutes(30), WithCause: true));

        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Retryable);
        var error = Assert.Single(job.Errors);
        Assert.Equal("HttpRequestException: vendor.rate_limited (429 Too Many Requests)", error.Error);
        Assert.Contains("CallVendor", error.Trace, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_worker_chosen_retry_spends_attempts_and_is_discarded_when_they_run_out()
    {
        await using var app = await StartAsync();
        var result = await app.Client.InsertAsync(new RetryAfterArgs(TimeSpan.Zero), new InsertOptions { MaxAttempts = 3 });

        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Discarded);
        Assert.Equal(3, job.Attempt);
        Assert.Equal([1, 2, 3], job.Errors.Select(e => e.Attempt));
        Assert.All(job.Errors, e => Assert.Equal("JobRetryException: vendor.rate_limited", e.Error));
        Assert.Equal(3, app.Recorder.RunCounts[result.Job.Id]);
    }

    [Fact]
    public async Task A_worker_chosen_retry_beyond_the_calendar_waits_until_the_latest_schedulable_time()
    {
        await using var app = await StartAsync();
        var result = await app.Client.InsertAsync(new RetryAfterArgs(TimeSpan.MaxValue));

        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Retryable);
        Assert.Equal(JobExecutor.MaxScheduledAt, job.ScheduledAt);
        Assert.Single(job.Errors);
    }

    [Fact]
    public async Task A_snooze_beyond_the_calendar_waits_until_the_latest_schedulable_time()
    {
        await using var app = await StartAsync();
        var result = await app.Client.InsertAsync(new SnoozeArgs(1, TimeSpan.MaxValue));

        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Scheduled);
        Assert.Equal(JobExecutor.MaxScheduledAt, job.ScheduledAt);
        Assert.Equal(0, job.Attempt);
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

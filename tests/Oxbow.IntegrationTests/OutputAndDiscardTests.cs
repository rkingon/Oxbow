using System.Collections.Concurrent;

namespace Oxbow.IntegrationTests;

public sealed record OutputResult(int Total, string Note);

[JobKind("output")]
public sealed record OutputArgs(string Mode) : IJobArgs;

public sealed class OutputWorker : IWorker<OutputArgs>
{
    public Task WorkAsync(Job<OutputArgs> job, CancellationToken cancellationToken)
    {
        switch (job.Args.Mode)
        {
            case "big":
                job.SetOutput(new string('x', JobRow.MaxOutputSize));
                break;
            case "fail":
                job.SetOutput(new OutputResult(1, "partial"));
                throw new InvalidOperationException("Failed after recording output.");
            default:
                job.SetOutput(new OutputResult(42, "done"));
                break;
        }

        return Task.CompletedTask;
    }
}

public sealed class DiscardRecorder
{
    private readonly ConcurrentDictionary<long, TaskCompletionSource> _called = new();

    public ConcurrentQueue<JobDiscardContext> Calls { get; } = new();

    public TaskCompletionSource Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Record(JobDiscardContext context)
    {
        Calls.Enqueue(context);
        Signal(context.Job.Id).TrySetResult();
    }

    public async Task<JobDiscardContext> WaitForAsync(long jobId)
    {
        await Signal(jobId).Task.WaitAsync(TimeSpan.FromSeconds(15));

        // Long enough for a duplicate call to show up.
        await Task.Delay(500);
        return Assert.Single(Calls, c => c.Job.Id == jobId);
    }

    private TaskCompletionSource Signal(long jobId) =>
        _called.GetOrAdd(jobId, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
}

public sealed class RecordingDiscardHandler(DiscardRecorder recorder) : IJobDiscardHandler
{
    public Task HandleAsync(JobDiscardContext context, CancellationToken cancellationToken)
    {
        recorder.Record(context);
        return Task.CompletedTask;
    }
}

public sealed class ThrowingDiscardHandler : IJobDiscardHandler
{
    public Task HandleAsync(JobDiscardContext context, CancellationToken cancellationToken) =>
        throw new InvalidOperationException("Handler failed.");
}

public sealed class GatedDiscardHandler(DiscardRecorder recorder) : IJobDiscardHandler
{
    public async Task HandleAsync(JobDiscardContext context, CancellationToken cancellationToken)
    {
        recorder.Entered.TrySetResult();
        await recorder.Gate.Task.WaitAsync(cancellationToken);
        recorder.Record(context);
    }
}

public sealed class OutputAndDiscardTests(PostgresFixture database)
{
    private static readonly InsertOptions OneAttempt = new() { MaxAttempts = 1 };

    private static Task<TestApp> StartAsync(
        PostgresFixture database,
        string prefix,
        DiscardRecorder recorder,
        Action<IOxbowBuilder>? handlers = null,
        Action<OxbowOptions>? configure = null,
        bool start = true) =>
        TestApp.StartAsync(database, prefix, o =>
        {
            o.Queues["default"] = new QueueOptions();
            configure?.Invoke(o);
        }, b =>
        {
            b.Services.AddSingleton(recorder);
            b.AddWorker<OutputArgs, OutputWorker>()
                .AddWorker<FailArgs, FailWorker>()
                .AddWorker<SelfCancelArgs, SelfCancelWorker>()
                .AddWorker<BlockArgs, BlockWorker>()
                .AddWorker<RecordArgs, RecordWorker>()
                .AddWorker<UniqueArgs, UniqueWorker>();
            if (handlers is null)
            {
                b.AddDiscardHandler<RecordingDiscardHandler>();
            }
            else
            {
                handlers(b);
            }
        }, start: start);

    [Fact]
    public async Task Output_is_stored_on_completion_and_read_back_by_get_and_list()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(database, prefix, new DiscardRecorder());

        var result = await app.Client.InsertAsync(new OutputArgs("ok"));
        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Completed);

        Assert.Equal(new OutputResult(42, "done"), job.GetOutput<OutputResult>());
        Assert.Equal(42, job.Metadata.GetProperty("output").GetProperty("total").GetInt32());
        var listed = await app.Client.ListAsync(new JobListQuery { States = [JobState.Completed] });
        Assert.Equal(new OutputResult(42, "done"), Assert.Single(listed.Jobs).GetOutput<OutputResult>());
    }

    [Fact]
    public async Task Output_over_the_limit_fails_the_attempt_and_is_not_stored()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(database, prefix, new DiscardRecorder());

        var result = await app.Client.InsertAsync(new OutputArgs("big"), OneAttempt);
        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Discarded);

        Assert.Contains("byte limit", Assert.Single(job.Errors).Error, StringComparison.Ordinal);
        Assert.Null(job.Output);
    }

    [Fact]
    public async Task Output_is_not_stored_when_the_attempt_fails()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(database, prefix, new DiscardRecorder());

        var result = await app.Client.InsertAsync(new OutputArgs("fail"), OneAttempt);
        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Discarded);

        Assert.Null(job.Output);
    }

    [Fact]
    public async Task The_handler_is_called_once_when_attempts_run_out()
    {
        var prefix = await database.CreateSchemaAsync();
        var recorder = new DiscardRecorder();
        await using var app = await StartAsync(database, prefix, recorder);

        var result = await app.Client.InsertAsync(new FailArgs("boom"), new InsertOptions { MaxAttempts = 2 });
        var call = await recorder.WaitForAsync(result.Job.Id);

        Assert.Equal(JobState.Discarded, call.State);
        Assert.Equal(JobDiscardReason.AttemptsExhausted, call.Reason);
        Assert.IsType<InvalidOperationException>(call.Exception);
        Assert.Equal(JobState.Discarded, call.Job.State);
        Assert.Equal(2, call.Job.Errors.Count);
    }

    [Fact]
    public async Task The_handler_is_called_once_when_the_worker_cancels()
    {
        var prefix = await database.CreateSchemaAsync();
        var recorder = new DiscardRecorder();
        await using var app = await StartAsync(database, prefix, recorder);

        var result = await app.Client.InsertAsync(new SelfCancelArgs());
        var call = await recorder.WaitForAsync(result.Job.Id);

        Assert.Equal(JobState.Cancelled, call.State);
        Assert.Equal(JobDiscardReason.WorkerCancelled, call.Reason);
        Assert.IsType<JobCancelException>(call.Exception);
    }

    [Fact]
    public async Task The_handler_sees_a_retry_that_a_cancel_request_turned_into_a_cancellation()
    {
        var prefix = await database.CreateSchemaAsync();
        var recorder = new DiscardRecorder();
        await using var app = await StartAsync(database, prefix, recorder);

        var result = await app.Client.InsertAsync(new BlockArgs());
        await app.Recorder.WaitStartedAsync(result.Job.Id);
        await app.Client.CancelAsync(result.Job.Id);
        var call = await recorder.WaitForAsync(result.Job.Id);

        Assert.Equal(JobState.Cancelled, call.State);
        Assert.Equal(JobDiscardReason.CancelRequested, call.Reason);
        Assert.Equal(JobState.Cancelled, call.Job.State);
        Assert.True(call.Job.Metadata.TryGetProperty("cancel_attempted_at", out _));
    }

    [Fact]
    public async Task The_handler_is_called_once_for_the_rescuers_discards_and_cancellations()
    {
        var prefix = await database.CreateSchemaAsync();
        var recorder = new DiscardRecorder();
        await using var app = await StartAsync(database, prefix, recorder, configure: o => o.RescueStuckJobsAfter = TimeSpan.FromSeconds(1));

        // Workers that died mid-job: one on its last attempt, one whose cancellation was requested.
        var discarded = await database.ScalarAsOwnerAsync<long>($"""
            INSERT INTO public.{prefix}job (args, kind, queue, state, attempt, attempted_at, max_attempts)
            VALUES ('{"{}"}', 'nobody_works_this', 'default', 'running', 1, now() - interval '1 hour', 1)
            RETURNING id
            """);
        var cancelled = await database.ScalarAsOwnerAsync<long>($"""
            INSERT INTO public.{prefix}job (args, kind, queue, state, attempt, attempted_at, max_attempts, metadata)
            VALUES ('{"{}"}', 'nobody_works_this', 'default', 'running', 1, now() - interval '1 hour', 5,
                    jsonb_build_object('cancel_attempted_at', now()))
            RETURNING id
            """);

        var discard = await recorder.WaitForAsync(discarded);
        Assert.Equal(JobDiscardReason.AttemptsExhausted, discard.Reason);
        Assert.Equal(JobState.Discarded, discard.Job.State);
        Assert.Null(discard.Exception);

        var cancel = await recorder.WaitForAsync(cancelled);
        Assert.Equal(JobDiscardReason.CancelRequested, cancel.Reason);
        Assert.Equal(JobState.Cancelled, cancel.Job.State);
    }

    [Fact]
    public async Task The_handler_is_called_once_for_the_schedulers_unique_key_conflicts()
    {
        var prefix = await database.CreateSchemaAsync();
        var recorder = new DiscardRecorder();
        await using var app = await StartAsync(database, prefix, recorder, start: false);

        // A retryable job that stops holding its key, and a new job that takes it before the retry is due.
        var unique = new InsertOptions
        {
            Queue = "parked",
            Unique = new UniqueOptions { ByArgs = true, ByState = [JobState.Available, JobState.Pending, JobState.Running, JobState.Scheduled] },
        };
        var retrying = await app.Client.InsertAsync(new UniqueArgs("customer-1", "a"), unique);
        await database.ExecuteAsOwnerAsync(
            $"UPDATE public.{prefix}job SET state = 'retryable', scheduled_at = now() - interval '1 second' WHERE id = {retrying.Job.Id}");
        var holder = await app.Client.InsertAsync(new UniqueArgs("customer-1", "b"), unique);
        Assert.False(holder.UniqueSkippedAsDuplicate);

        await app.Host.StartAsync();
        var call = await recorder.WaitForAsync(retrying.Job.Id);

        Assert.Equal(JobDiscardReason.UniqueKeyConflict, call.Reason);
        Assert.Equal(JobState.Discarded, call.Job.State);
        Assert.Equal("scheduler_discarded", call.Job.Metadata.GetProperty("unique_key_conflict").GetString());
    }

    [Fact]
    public async Task A_throwing_handler_does_not_stop_other_handlers_or_processing()
    {
        var prefix = await database.CreateSchemaAsync();
        var recorder = new DiscardRecorder();
        await using var app = await StartAsync(database, prefix, recorder,
            b => b.AddDiscardHandler<ThrowingDiscardHandler>().AddDiscardHandler<RecordingDiscardHandler>());

        var first = await app.Client.InsertAsync(new FailArgs("first"), OneAttempt);
        await recorder.WaitForAsync(first.Job.Id);
        var second = await app.Client.InsertAsync(new FailArgs("second"), OneAttempt);
        await recorder.WaitForAsync(second.Job.Id);

        var record = await app.Client.InsertAsync(new RecordArgs("still working"));
        await app.WaitForStateAsync(record.Job.Id, JobState.Completed);
    }

    [Fact]
    public async Task A_slow_handler_does_not_hold_the_worker_slot()
    {
        var prefix = await database.CreateSchemaAsync();
        var recorder = new DiscardRecorder();
        await using var app = await StartAsync(database, prefix, recorder,
            b => b.AddDiscardHandler<GatedDiscardHandler>(),
            o => o.Queues["default"] = new QueueOptions { MaxWorkers = 1 });

        var discarded = await app.Client.InsertAsync(new FailArgs("boom"), OneAttempt);
        await recorder.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15));

        var next = await app.Client.InsertAsync(new RecordArgs("next"));
        await app.WaitForStateAsync(next.Job.Id, JobState.Completed);
        Assert.Empty(recorder.Calls);

        recorder.Gate.SetResult();
        await recorder.WaitForAsync(discarded.Job.Id);
    }

    [Fact]
    public async Task Shutdown_waits_for_an_in_flight_handler()
    {
        var prefix = await database.CreateSchemaAsync();
        var recorder = new DiscardRecorder();
        var app = await StartAsync(database, prefix, recorder, b => b.AddDiscardHandler<GatedDiscardHandler>());

        var result = await app.Client.InsertAsync(new FailArgs("boom"), OneAttempt);
        await recorder.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var stopping = app.DisposeAsync();
        await Task.Delay(200);
        recorder.Gate.SetResult();
        await stopping;

        Assert.Equal(result.Job.Id, Assert.Single(recorder.Calls).Job.Id);
    }
}

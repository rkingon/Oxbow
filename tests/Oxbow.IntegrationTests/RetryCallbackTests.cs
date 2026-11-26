using Microsoft.Extensions.Logging.Abstractions;
using Oxbow.Internal;
using Oxbow.Processing;

namespace Oxbow.IntegrationTests;

public sealed class RetryCallbackTests(PostgresFixture database)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_failed_attempt_schedules_its_retry_before_disposing_the_worker(bool throwInCallback)
    {
        await using var app = await StartAsync();
        var message = throwInCallback ? "callback throws" : "ordinary failure";
        var id = (await app.Client.InsertAsync(new FailArgs(message))).Job.Id;
        var row = await app.WaitForStateAsync(id, JobState.Retryable);
        Assert.Equal($"InvalidOperationException: {message}", Assert.Single(row.Errors).Error);
        Assert.Equal(throwInCallback ? FixedRetryPolicy.RetryAt : DisposableRetryWorker.RetryAt, row.ScheduledAt);
    }

    [Fact]
    public async Task Rescue_uses_the_configured_policy_when_the_worker_retry_callback_throws()
    {
        await using var app = await StartAsync(start: false);
        var id = (await app.Client.InsertAsync(new FailArgs("callback throws"))).Job.Id;
        await using var age = app.DataSource.CreateCommand(
            $"UPDATE public.{app.Prefix}job SET state='running', attempt=1, attempted_at=now()-interval '2 hours' WHERE id=@id");
        age.Parameters.AddWithValue("id", id);
        Assert.Equal(1, await age.ExecuteNonQueryAsync());
        await app.Host.StartAsync();
        var row = await app.WaitForStateAsync(id, JobState.Retryable);
        Assert.Equal(FixedRetryPolicy.RetryAt, row.ScheduledAt);
        Assert.Contains("rescued", Assert.Single(row.Errors).Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Shutdown_during_scope_disposal_refunds_the_final_cancelled_attempt()
    {
        var gate = new DisposalGate();
        await using var app = await TestApp.StartAsync(database, await database.CreateSchemaAsync(), register: b =>
        {
            b.Services.AddSingleton(gate);
            b.AddWorker<FailArgs, CancellingWorker>();
        }, start: false);
        var id = (await app.Client.InsertAsync(new FailArgs("cancel"), new InsertOptions { MaxAttempts = 1 })).Job.Id;
        await using var claim = app.DataSource.CreateCommand(
            $"UPDATE public.{app.Prefix}job SET state='running', attempt=1, attempted_at=now() WHERE id=@id");
        claim.Parameters.AddWithValue("id", id);
        Assert.Equal(1, await claim.ExecuteNonQueryAsync());

        var services = app.Host.Services;
        var runtime = services.GetRequiredService<OxbowRuntime>();
        var scopes = services.GetRequiredService<IServiceScopeFactory>();
        var completer = new Completer(runtime, NullLogger.Instance);
        var executor = new JobExecutor(runtime, services.GetRequiredService<WorkerRegistry>(), scopes,
            services.GetRequiredService<IRetryPolicy>(), completer, new DiscardHooks(runtime, [], scopes, NullLogger.Instance),
            TimeProvider.System, NullLogger.Instance);
        using var running = new RunningJob((await app.Client.GetAsync(id))!);
        using var stop = new CancellationTokenSource();
        var work = executor.ExecuteAsync(running, stop.Token);
        try
        {
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await stop.CancelAsync();
        }
        finally
        {
            gate.Release.TrySetResult();
        }

        await work;
        completer.Complete();
        await completer.RunAsync();
        var row = await app.Client.GetAsync(id);
        Assert.Equal(JobState.Available, row!.State);
        Assert.Equal(0, row.Attempt);
        Assert.Empty(row.Errors);
    }

    private async Task<TestApp> StartAsync(bool start = true) =>
        await TestApp.StartAsync(database, await database.CreateSchemaAsync(),
            o => o.Queues["default"] = new QueueOptions(), b =>
            {
                b.Services.AddSingleton<IRetryPolicy, FixedRetryPolicy>();
                b.AddWorker<FailArgs, DisposableRetryWorker>();
            }, start: start);
}

public sealed class FixedRetryPolicy : IRetryPolicy
{
    public static DateTimeOffset RetryAt { get; } = DateTimeOffset.FromUnixTimeSeconds(DateTimeOffset.UtcNow.ToUnixTimeSeconds()).AddDays(3);
    public DateTimeOffset NextRetry(JobRow job) => RetryAt;
}

public sealed class DisposableRetryWorker : IWorker<FailArgs>, IDisposable
{
    public static DateTimeOffset RetryAt { get; } = FixedRetryPolicy.RetryAt.AddDays(1);
    private bool _disposed;

    public Task WorkAsync(Job<FailArgs> job, CancellationToken cancellationToken) => throw new InvalidOperationException(job.Args.Message);

    public void Dispose() => _disposed = true;

    public DateTimeOffset? GetNextRetry(Job<FailArgs> job)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (job.Args.Message == "callback throws")
        {
            throw new InvalidOperationException("Retry callback failed.");
        }

        return RetryAt;
    }
}

public sealed class DisposalGate
{
    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

public sealed class CancellingWorker(DisposalGate gate) : IWorker<FailArgs>, IAsyncDisposable
{
    public Task WorkAsync(Job<FailArgs> job, CancellationToken cancellationToken) => throw new OperationCanceledException();

    public async ValueTask DisposeAsync()
    {
        gate.Entered.TrySetResult();
        await gate.Release.Task;
    }
}

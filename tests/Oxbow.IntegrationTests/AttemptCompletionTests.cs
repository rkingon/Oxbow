using Microsoft.Extensions.Logging.Abstractions;
using Oxbow.Internal;
using Oxbow.Processing;

namespace Oxbow.IntegrationTests;

public sealed class AttemptCompletionTests(PostgresFixture database)
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_rescued_worker_cannot_complete_or_overwrite_its_replacement(bool replacementStillRunning)
    {
        var gates = new AttemptGates();
        var prefix = await database.CreateSchemaAsync();
        void Configure(OxbowOptions options)
        {
            options.Queues["default"] = new QueueOptions { MaxWorkers = 1 };
            options.RescueStuckJobsAfter = TimeSpan.FromMilliseconds(500);
        }
        void Register(IOxbowBuilder builder)
        {
            builder.Services.AddSingleton(gates);
            builder.AddWorker<RecordArgs, GatedOutputWorker>();
        }

        await using var original = await TestApp.StartAsync(database, prefix, Configure, Register);
        var id = (await original.Client.InsertAsync(new RecordArgs("review"))).Job.Id;
        await gates.FirstStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await using var replacement = await TestApp.StartAsync(database, prefix, Configure, Register);
        try
        {
            await gates.SecondStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var claimed = await original.Client.GetAsync(id);
            Assert.Equal(JobState.Running, claimed!.State);
            Assert.Equal(2, claimed.Attempt);
            Assert.Contains("rescued", Assert.Single(claimed.Errors).Error, StringComparison.Ordinal);
            if (!replacementStillRunning)
            {
                gates.SecondRelease.TrySetResult();
                var completed = await original.WaitForStateAsync(id, JobState.Completed);
                Assert.Equal("second", completed.GetOutput<string>());
            }

            gates.FirstRelease.TrySetResult();
            // Stopping the original host flushes its completion before either assertion.
            await original.Host.StopAsync();
            var row = await replacement.Client.GetAsync(id);
            Assert.Equal(replacementStillRunning ? JobState.Running : JobState.Completed, row!.State);
            Assert.Equal(replacementStillRunning ? null : "second", row.GetOutput<string>());
            Assert.Equal(claimed.AttemptedAt, row.AttemptedAt);
            gates.SecondRelease.TrySetResult();
            var final = await replacement.WaitForStateAsync(id, JobState.Completed);
            Assert.Equal("second", final.GetOutput<string>());
        }
        finally
        {
            gates.FirstRelease.TrySetResult();
            gates.SecondRelease.TrySetResult();
        }
    }

    [Fact]
    public async Task Only_the_matching_attempt_in_a_batch_is_acknowledged()
    {
        await using var app = await TestApp.StartAsync(database, await database.CreateSchemaAsync(),
            register: b => b.AddWorker<RecordArgs, RecordWorker>(), start: false);
        var id = (await app.Client.InsertAsync(new RecordArgs("batch"))).Job.Id;
        await using var claim = app.DataSource.CreateCommand(
            $"UPDATE public.{app.Prefix}job SET state='running', attempt=2, attempted_at=now() WHERE id=@id RETURNING attempted_at");
        claim.Parameters.AddWithValue("id", id);
        var current = new DateTimeOffset(Assert.IsType<DateTime>(await claim.ExecuteScalarAsync()));

        var completer = new Completer(app.Host.Services.GetRequiredService<OxbowRuntime>(), NullLogger.Instance);
        var completed = await completer.EnqueueAsync(new SetStateParams(id, current.AddSeconds(-1), JobState.Completed) { FinalizedAt = DateTimeOffset.UtcNow });
        var cancelled = await completer.EnqueueAsync(new SetStateParams(id, current, JobState.Cancelled) { FinalizedAt = DateTimeOffset.UtcNow });
        completer.Complete();
        await completer.RunAsync();
        var row = await app.Client.GetAsync(id);
        Assert.Equal(JobState.Cancelled, row!.State);
        Assert.Null(await completed);
        Assert.Equal(JobState.Cancelled, await cancelled);
    }
}

public sealed class AttemptGates
{
    public TaskCompletionSource FirstStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource FirstRelease { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource SecondStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource SecondRelease { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

public sealed class GatedOutputWorker(AttemptGates gates) : IWorker<RecordArgs>
{
    public async Task WorkAsync(Job<RecordArgs> job, CancellationToken cancellationToken)
    {
        var first = job.Attempt == 1;
        (first ? gates.FirstStarted : gates.SecondStarted).TrySetResult();
        await (first ? gates.FirstRelease : gates.SecondRelease).Task.WaitAsync(TimeSpan.FromSeconds(20), CancellationToken.None);
        job.SetOutput(first ? "first" : "second");
    }

    public TimeSpan? GetTimeout(Job<RecordArgs> job) =>
        job.Attempt == 1 ? TimeSpan.FromMilliseconds(200) : Timeout.InfiniteTimeSpan;

    public DateTimeOffset? GetNextRetry(Job<RecordArgs> job) => DateTimeOffset.UtcNow;
}

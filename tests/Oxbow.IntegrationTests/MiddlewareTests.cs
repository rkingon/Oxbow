using System.Text.Json.Nodes;

namespace Oxbow.IntegrationTests;

public sealed class MiddlewareTests(PostgresFixture database)
{
    [Fact]
    public async Task Insert_and_work_middleware_wrap_every_job()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(), b => b
            .AddWorker<RecordArgs, RecordWorker>()
            .AddInsertMiddleware<StampTenant>()
            .AddWorkMiddleware<RecordTenant>());

        var result = await app.Client.InsertAsync(new RecordArgs("with middleware"));
        Assert.Equal("acme", result.Job.Metadata.GetProperty("tenant").GetString());

        await app.WaitForStateAsync(result.Job.Id, JobState.Completed);
        Assert.Contains("tenant:acme", app.Recorder.Events);
    }

    [Fact]
    public async Task Work_middleware_can_turn_a_failure_into_a_worker_chosen_retry()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(), b => b
            .AddWorker<FailArgs, FailWorker>()
            .AddWorkMiddleware<RetryLater>());

        var before = DateTimeOffset.UtcNow;
        var result = await app.Client.InsertAsync(new FailArgs("vendor down"));

        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Retryable);
        Assert.Equal("InvalidOperationException: retry in an hour", Assert.Single(job.Errors).Error);
        Assert.InRange(job.ScheduledAt, before + RetryLater.Delay, DateTimeOffset.UtcNow + RetryLater.Delay);
    }

    private sealed class RetryLater : IJobWorkMiddleware
    {
        public static readonly TimeSpan Delay = TimeSpan.FromHours(1);

        public async Task InvokeAsync(JobContext context, JobWorkDelegate next)
        {
            try
            {
                await next(context);
            }
            catch (InvalidOperationException ex)
            {
                throw new JobRetryException(Delay, "retry in an hour", ex);
            }
        }
    }

    private sealed class StampTenant : IJobInsertMiddleware
    {
        public Task<IReadOnlyList<InsertResult>> InvokeAsync(JobInsertContext context, JobInsertDelegate next)
        {
            foreach (var job in context.Jobs)
            {
                job.Metadata["tenant"] = JsonValue.Create("acme");
            }

            return next(context);
        }
    }

    private sealed class RecordTenant(JobRecorder recorder) : IJobWorkMiddleware
    {
        public async Task InvokeAsync(JobContext context, JobWorkDelegate next)
        {
            recorder.Events.Enqueue($"tenant:{context.Job.Metadata.GetProperty("tenant").GetString()}");
            await next(context);
        }
    }
}

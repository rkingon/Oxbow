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

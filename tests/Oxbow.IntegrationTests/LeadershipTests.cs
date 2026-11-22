
using Microsoft.Extensions.Hosting;
using Oxbow.Processing;

namespace Oxbow.IntegrationTests;

public sealed class LeadershipTests(PostgresFixture database)
{
    private static Elector ElectorOf(TestApp app) =>
        app.Host.Services.GetServices<IHostedService>().OfType<OxbowHostedService>().Single().Elector!;

    [Fact]
    public async Task One_node_leads_and_another_takes_over_when_it_stops()
    {
        var prefix = await database.CreateSchemaAsync();
        void Configure(OxbowOptions o) => o.Queues["default"] = new QueueOptions();
        var a = await TestApp.StartAsync(database, prefix, Configure);
        await using var b = await TestApp.StartAsync(database, prefix, Configure);

        await WaitUntilAsync(() => ElectorOf(a).IsLeader || ElectorOf(b).IsLeader);
        Assert.False(ElectorOf(a).IsLeader && ElectorOf(b).IsLeader);

        var leader = ElectorOf(a).IsLeader ? a : b;
        var follower = leader == a ? b : a;
        await leader.DisposeAsync();

        await WaitUntilAsync(() => ElectorOf(follower).IsLeader, TimeSpan.FromSeconds(5));
        if (leader != a)
        {
            await a.DisposeAsync();
        }
    }

    [Fact]
    public async Task The_rescuer_retries_a_job_stuck_running()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o =>
        {
            o.Queues["default"] = new QueueOptions();
            o.RescueStuckJobsAfter = TimeSpan.FromSeconds(1);
            o.JobTimeout = TimeSpan.FromSeconds(1);
        }, b => b.AddWorker<RecordArgs, RecordWorker>());

        // A worker that died mid-job: running, attempted long ago, never reported back.
        var id = await database.ScalarAsOwnerAsync<long>($"""
            INSERT INTO public.{prefix}job (args, kind, queue, state, attempt, attempted_at, max_attempts)
            VALUES ('{"{"}"value": "stuck"{"}"}', 'record', 'default', 'running', 1, now() - interval '1 hour', 5)
            RETURNING id
            """);

        var job = await app.WaitForAsync(id, j => j.State == JobState.Completed || j.Errors.Count > 0);
        Assert.Contains("rescued", job.Errors[0].Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_cleaner_deletes_finalized_jobs_past_retention()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o =>
        {
            o.Queues["default"] = new QueueOptions();
            o.CompletedJobRetention = TimeSpan.FromMinutes(1);
        });

        var id = await database.ScalarAsOwnerAsync<long>($"""
            INSERT INTO public.{prefix}job (args, kind, state, finalized_at)
            VALUES ('{"{}"}', 'record', 'completed', now() - interval '2 minutes')
            RETURNING id
            """);

        await WaitUntilAsync(async () => await app.Client.GetAsync(id) is null);
    }

    [Fact]
    public async Task Periodic_jobs_are_enqueued_by_the_leader()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o =>
        {
            o.Queues["default"] = new QueueOptions();
            o.AddPeriodicJob(PeriodicSchedule.Every(TimeSpan.FromSeconds(1)), () => new RecordArgs("tick"), runOnStart: true);
        }, b => b.AddWorker<RecordArgs, RecordWorker>());

        await WaitUntilAsync(() => app.Recorder.Events.Count(e => e == "tick") >= 3, TimeSpan.FromSeconds(10));
    }

    private static Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null) =>
        WaitUntilAsync(() => Task.FromResult(condition()), timeout);

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(15));
        while (!await condition())
        {
            await Task.Delay(50, cts.Token);
        }
    }
}

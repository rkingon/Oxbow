using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Oxbow.Processing;

namespace Oxbow.IntegrationTests;

[JobKind("long_running")]
public sealed record LongRunningArgs(int Milliseconds, bool Heartbeat) : IJobArgs;

public sealed class LongRunningWorker(JobRecorder recorder) : IWorker<LongRunningArgs>
{
    public async Task WorkAsync(Job<LongRunningArgs> job, CancellationToken cancellationToken)
    {
        recorder.Started(job.Id);
        var until = DateTimeOffset.UtcNow.AddMilliseconds(job.Args.Milliseconds);
        while (DateTimeOffset.UtcNow < until)
        {
            if (job.Args.Heartbeat)
            {
                await job.HeartbeatAsync(cancellationToken);
            }

            await Task.Delay(50, cancellationToken);
        }
    }

    public TimeSpan? GetTimeout(Job<LongRunningArgs> job) => TimeSpan.FromSeconds(30);
}

/// <summary>Holds its job open, exposing it to the test, until released.</summary>
[JobKind("hold")]
public sealed record HoldArgs(Guid Key) : IJobArgs;

public sealed class HoldWorker : IWorker<HoldArgs>
{
    public static ConcurrentDictionary<Guid, (Job<HoldArgs> Job, TaskCompletionSource Release)> Held { get; } = new();

    public async Task WorkAsync(Job<HoldArgs> job, CancellationToken cancellationToken)
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Held[job.Args.Key] = (job, release);
        await release.Task.WaitAsync(cancellationToken);
    }

    public TimeSpan? GetTimeout(Job<HoldArgs> job) => Timeout.InfiniteTimeSpan;

    public static async Task<(Job<HoldArgs> Job, TaskCompletionSource Release)> WaitHeldAsync(Guid key)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!Held.ContainsKey(key))
        {
            await Task.Delay(20, cts.Token);
        }

        return Held[key];
    }
}

public sealed class HeartbeatTests(PostgresFixture database)
{
    private static Elector ElectorOf(TestApp app) =>
        app.Host.Services.GetServices<IHostedService>().OfType<OxbowHostedService>().Single().Elector!;

    private async Task<TestApp> StartHoldAppAsync() =>
        await TestApp.StartAsync(database, await database.CreateSchemaAsync(),
            o => o.Queues["default"] = new QueueOptions(), b => b.AddWorker<HoldArgs, HoldWorker>());

    [Fact]
    public async Task A_job_running_past_the_rescue_horizon_is_rescued_only_without_heartbeats()
    {
        // The leader has no worker for the kind, so before heartbeats it judged these jobs by attempted_at alone.
        var prefix = await database.CreateSchemaAsync();
        void Configure(OxbowOptions o) => o.RescueStuckJobsAfter = TimeSpan.FromSeconds(1);
        await using var leader = await TestApp.StartAsync(database, prefix, o =>
        {
            Configure(o);
            o.Queues["other"] = new QueueOptions();
        });
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
        {
            while (!ElectorOf(leader).IsLeader)
            {
                await Task.Delay(50, cts.Token);
            }
        }

        await using var worker = await TestApp.StartAsync(database, prefix, o =>
        {
            Configure(o);
            o.Queues["default"] = new QueueOptions();
        }, b => b.AddWorker<LongRunningArgs, LongRunningWorker>());

        var options = new InsertOptions { MaxAttempts = 1 };
        var beating = await worker.Client.InsertAsync(new LongRunningArgs(3_000, Heartbeat: true), options);
        var silent = await worker.Client.InsertAsync(new LongRunningArgs(3_000, Heartbeat: false), options);

        var rescued = await worker.WaitForStateAsync(silent.Job.Id, JobState.Discarded);
        Assert.Contains("rescued", rescued.Errors.Single().Error, StringComparison.Ordinal);

        var completed = await worker.WaitForStateAsync(beating.Job.Id, JobState.Completed);
        Assert.Equal(1, completed.Attempt);
        Assert.Empty(completed.Errors);
        Assert.True(completed.HeartbeatAt > completed.AttemptedAt!.Value.AddSeconds(2));
    }

    [Fact]
    public async Task The_rescuer_uses_the_later_of_attempted_at_and_the_attempts_heartbeat()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o =>
        {
            o.Queues["default"] = new QueueOptions();
            o.RescueStuckJobsAfter = TimeSpan.FromMinutes(10);
            o.JobTimeout = TimeSpan.FromSeconds(1);
        }, b => b.AddWorker<RecordArgs, RecordWorker>().AddWorker<BlockArgs, BlockWorker>());

        // Running rows of dead workers, all started an hour ago. 'block' has no timeout.
        async Task<long> InsertAsync(string kind, string heartbeat) => await database.ScalarAsOwnerAsync<long>($"""
            INSERT INTO public.{prefix}job (args, kind, queue, state, attempt, attempted_at, max_attempts, metadata)
            VALUES ('{"{"}"value": "x"{"}"}', '{kind}', 'default', 'running', 1, now() - interval '1 hour', 1,
                    {heartbeat})
            RETURNING id
            """);
        string Beat(string ago) => $"jsonb_build_object('oxbow:heartbeat_at', now() - interval '{ago}')";

        var recent = await InsertAsync("record", Beat("1 second"));
        var quiet = await InsertAsync("record", Beat("30 minutes"));
        var garbage = await InsertAsync("record", """'{"oxbow:heartbeat_at": "not a time"}'""");
        var quietNoTimeout = await InsertAsync("block", Beat("30 minutes"));
        var neverBeatNoTimeout = await InsertAsync("block", "'{}'");
        var earlierAttemptNoTimeout = await InsertAsync("block", Beat("2 hours"));
        var unknownKind = await InsertAsync("nobody_works_this", Beat("1 second"));

        foreach (var id in new[] { quiet, garbage, quietNoTimeout })
        {
            var job = await app.WaitForStateAsync(id, JobState.Discarded);
            Assert.Contains("rescued", job.Errors.Single().Error, StringComparison.Ordinal);
        }

        await Task.Delay(1_000);
        foreach (var id in new[] { recent, neverBeatNoTimeout, earlierAttemptNoTimeout, unknownKind })
        {
            var job = await app.Client.GetAsync(id);
            Assert.Equal(JobState.Running, job!.State);
            Assert.Empty(job.Errors);
        }
    }

    [Fact]
    public async Task Heartbeats_are_throttled_and_progress_is_visible_while_running()
    {
        await using var app = await StartHoldAppAsync();
        var key = Guid.NewGuid();
        var inserted = await app.Client.InsertAsync(new HoldArgs(key));
        var (job, release) = await HoldWorker.WaitHeldAsync(key);

        await job.HeartbeatAsync();
        var first = (await app.Client.GetAsync(job.Id))!.HeartbeatAt;
        Assert.NotNull(first);
        Assert.True(first >= job.Row.AttemptedAt);

        await job.HeartbeatAsync();
        Assert.Equal(first, (await app.Client.GetAsync(job.Id))!.HeartbeatAt);

        await job.ReportProgressAsync(40, "step 2 of 5");
        var running = (await app.Client.GetAsync(job.Id))!;
        Assert.Equal(JobState.Running, running.State);
        Assert.Equal(40, running.Progress!.Percent);
        Assert.Equal("step 2 of 5", running.Progress.Message);
        Assert.Equal(1, running.Progress.Attempt);
        Assert.True(running.HeartbeatAt > first);
        Assert.Equal(running.Progress, (await app.Client.ListAsync(new JobListQuery())).Jobs.Single(j => j.Id == inserted.Job.Id).Progress);
        Assert.Equal(40, job.Progress!.Percent);

        release.SetResult();
        var completed = await app.WaitForStateAsync(job.Id, JobState.Completed);
        Assert.Equal(running.Progress, completed.Progress);
    }

    [Fact]
    public async Task A_finished_or_superseded_attempt_cannot_write()
    {
        await using var app = await StartHoldAppAsync();

        var superseded = Guid.NewGuid();
        await app.Client.InsertAsync(new HoldArgs(superseded));
        var (stale, staleRelease) = await HoldWorker.WaitHeldAsync(superseded);
        await database.ExecuteAsOwnerAsync(
            $"UPDATE public.{app.Prefix}job SET attempted_at = attempted_at + interval '1 second' WHERE id = {stale.Id}");
        await stale.HeartbeatAsync();
        await stale.ReportProgressAsync(50);
        var row = (await app.Client.GetAsync(stale.Id))!;
        Assert.Null(row.HeartbeatAt);
        Assert.Null(row.Progress);
        staleRelease.SetResult();

        var finished = Guid.NewGuid();
        await app.Client.InsertAsync(new HoldArgs(finished));
        var (job, release) = await HoldWorker.WaitHeldAsync(finished);
        release.SetResult();
        await app.WaitForStateAsync(job.Id, JobState.Completed);

        await job.HeartbeatAsync();
        await job.ReportProgressAsync(100);
        row = (await app.Client.GetAsync(job.Id))!;
        Assert.Equal(JobState.Completed, row.State);
        Assert.Null(row.HeartbeatAt);
        Assert.Null(row.Progress);
    }
}

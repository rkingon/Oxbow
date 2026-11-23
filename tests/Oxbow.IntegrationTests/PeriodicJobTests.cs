using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Oxbow.Internal;
using Oxbow.Processing;

namespace Oxbow.IntegrationTests;

public sealed class PeriodicJobTests(PostgresFixture database)
{
    private static void Queues(OxbowOptions o) => o.Queues["default"] = new QueueOptions();

    private static void Workers(IOxbowBuilder b) => b.AddWorker<RecordArgs, RecordWorker>();

    [Fact]
    public async Task Stored_jobs_round_trip_through_upsert_get_list_and_delete()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, register: Workers, start: false);
        var client = app.Client;

        var created = await client.UpsertPeriodicJobAsync(
            "report", PeriodicSchedule.Every(TimeSpan.FromHours(1)), new RecordArgs("a"),
            new InsertOptions { Queue = "reports", Priority = 2, Tags = ["nightly"], Metadata = new JsonObject { ["team"] = "ops" } });
        Assert.Equal("record", created.Kind);
        Assert.Equal("a", created.Args!.Value.GetProperty("value").GetString());
        Assert.Equal("reports", created.Options!.Queue);
        Assert.Equal(2, created.Options.Priority);
        Assert.Equal(["nightly"], created.Options.Tags!);
        Assert.Equal("ops", created.Options.Metadata!["team"]!.GetValue<string>());
        Assert.Equal(TimeSpan.FromHours(1), created.Interval);
        Assert.Null(created.CronExpression);
        Assert.False(created.RegisteredInCode);
        Assert.Null(created.PausedAt);
        Assert.Null(created.LastRunAt);
        Assert.InRange(created.NextRunAt!.Value, DateTimeOffset.UtcNow.AddMinutes(59), DateTimeOffset.UtcNow.AddMinutes(61));

        // New args, same schedule: the next run stays.
        var replaced = await client.UpsertPeriodicJobAsync("report", PeriodicSchedule.Every(TimeSpan.FromHours(1)), new RecordArgs("b"));
        Assert.Equal("b", replaced.Args!.Value.GetProperty("value").GetString());
        Assert.Null(replaced.Options);
        Assert.Equal(created.NextRunAt, replaced.NextRunAt);
        Assert.Equal(created.CreatedAt, replaced.CreatedAt);

        // A new schedule starts again from its next occurrence.
        var rescheduled = await client.UpsertPeriodicJobAsync(
            "report", PeriodicSchedule.Every(TimeSpan.FromMinutes(5)), "record", JsonDocument.Parse("""{"value":"c"}""").RootElement);
        Assert.InRange(rescheduled.NextRunAt!.Value, DateTimeOffset.UtcNow.AddMinutes(4), DateTimeOffset.UtcNow.AddMinutes(6));

        await client.UpsertPeriodicJobAsync("another", PeriodicSchedule.Cron("0 3 * * *"), new RecordArgs("d"));
        Assert.Equal(["another", "report"], (await client.ListPeriodicJobsAsync(new())).Jobs.Select(j => j.Name));
        Assert.Equal("c", (await client.GetPeriodicJobAsync("report"))!.Args!.Value.GetProperty("value").GetString());

        Assert.NotNull(await client.DeletePeriodicJobAsync("report"));
        Assert.Null(await client.GetPeriodicJobAsync("report"));
        Assert.Null(await client.DeletePeriodicJobAsync("report"));
        Assert.Null(await client.PausePeriodicJobAsync("report"));
        Assert.Null(await client.ResumePeriodicJobAsync("report"));
    }

    [Fact]
    public async Task Create_fails_when_the_name_is_taken()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o =>
        {
            Queues(o);
            o.AddPeriodicJob("in-code", PeriodicSchedule.Every(TimeSpan.FromHours(1)), () => new RecordArgs("code"));
        }, Workers);
        var hourly = PeriodicSchedule.Every(TimeSpan.FromHours(1));

        var created = await app.Client.CreatePeriodicJobAsync("report", hourly, new RecordArgs("a"));
        var ex = await Assert.ThrowsAsync<PeriodicJobExistsException>(() =>
            app.Client.CreatePeriodicJobAsync("report", PeriodicSchedule.Every(TimeSpan.FromMinutes(1)), "record", JsonDocument.Parse("""{"value":"b"}""").RootElement));
        Assert.Equal("report", ex.Name);
        var unchanged = (await app.Client.GetPeriodicJobAsync("report"))!;
        Assert.Equal("a", unchanged.Args!.Value.GetProperty("value").GetString());
        Assert.Equal(created.NextRunAt, unchanged.NextRunAt);

        await WaitUntilAsync(async () => await app.Client.GetPeriodicJobAsync("in-code") is not null);
        await Assert.ThrowsAsync<PeriodicJobExistsException>(() => app.Client.CreatePeriodicJobAsync("in-code", hourly, new RecordArgs("a")));
    }

    [Fact]
    public async Task Listing_pages_by_name()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, register: Workers, start: false);
        var names = Enumerable.Range(0, 7).Select(i => $"tenant-{i:D2}").ToList();
        foreach (var name in names.AsEnumerable().Reverse())
        {
            await app.Client.CreatePeriodicJobAsync(name, PeriodicSchedule.Every(TimeSpan.FromHours(1)), new RecordArgs(name));
        }

        var seen = new List<string>();
        string? cursor = null;
        var pages = 0;
        do
        {
            var page = await app.Client.ListPeriodicJobsAsync(new PeriodicJobListQuery { Limit = 3, Cursor = cursor });
            seen.AddRange(page.Jobs.Select(j => j.Name));
            cursor = page.NextCursor;
            pages++;
        }
        while (cursor is not null);

        Assert.Equal(names, seen);
        Assert.Equal(3, pages);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => app.Client.ListPeriodicJobsAsync(new PeriodicJobListQuery { Limit = 0 }));
    }

    [Fact]
    public async Task Upsert_rejects_what_would_fail_every_run()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, Queues, Workers, start: false);
        var client = app.Client;
        var hourly = PeriodicSchedule.Every(TimeSpan.FromHours(1));

        Assert.Throws<Cronos.CronFormatException>(() => PeriodicSchedule.Cron("61 * * * *"));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.UpsertPeriodicJobAsync("x", hourly, "unknown_kind", JsonDocument.Parse("{}").RootElement));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.UpsertPeriodicJobAsync("x", hourly, "record", JsonDocument.Parse("""{"value": 42}""").RootElement));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.UpsertPeriodicJobAsync("x", hourly, new RecordArgs("a"), new InsertOptions { ScheduledAt = DateTimeOffset.UtcNow }));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.UpsertPeriodicJobAsync("x", hourly, new RecordArgs("a"), new InsertOptions { Priority = 9 }));
        await Assert.ThrowsAsync<ArgumentException>(() => client.UpsertPeriodicJobAsync("", hourly, new RecordArgs("a")));

        var custom = TimeZoneInfo.CreateCustomTimeZone("Not/A_Zone", TimeSpan.FromHours(3), "custom", "custom");
        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.UpsertPeriodicJobAsync("x", PeriodicSchedule.Cron("0 3 * * *", custom), new RecordArgs("a")));

        Assert.Empty((await client.ListPeriodicJobsAsync(new())).Jobs);
    }

    [Fact]
    public async Task A_cron_schedule_keeps_its_time_zone()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, register: Workers, start: false);
        var london = TimeZoneInfo.FindSystemTimeZoneById("Europe/London");

        var row = await app.Client.UpsertPeriodicJobAsync("nightly", PeriodicSchedule.Cron("0 3 * * *", london), new RecordArgs("a"));

        Assert.Equal("0 3 * * *", row.CronExpression);
        Assert.Equal("Europe/London", row.TimeZoneId);
        Assert.Null(row.Interval);
        var local = TimeZoneInfo.ConvertTime(row.NextRunAt!.Value, london);
        Assert.Equal((3, 0, 0), (local.Hour, local.Minute, local.Second));
        Assert.InRange(row.NextRunAt.Value, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));
    }

    [Fact]
    public async Task A_stored_job_runs_without_a_restart_and_through_insert_middleware()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, Queues, b => Workers(b.AddInsertMiddleware<StampSource>()));
        await WaitUntilAsync(() => ElectorOf(app).IsLeader);

        var tokyo = TimeZoneInfo.FindSystemTimeZoneById("Asia/Tokyo");
        await app.Client.UpsertPeriodicJobAsync("tick", PeriodicSchedule.Cron("* * * * * *", tokyo), new RecordArgs("tick"));

        await WaitUntilAsync(() => app.Recorder.Events.Count(e => e == "tick") >= 2);
        var job = (await RunsAsync(app, "tick"))[0];
        Assert.Equal("tick", job.Metadata.GetProperty("oxbow:periodic_job").GetString());
        Assert.Equal("middleware", job.Metadata.GetProperty("source").GetString());
        Assert.NotNull((await app.Client.GetPeriodicJobAsync("tick"))!.LastRunAt);
    }

    [Fact]
    public async Task A_paused_job_doesnt_run_until_resumed()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, Queues, Workers);
        await app.Client.UpsertPeriodicJobAsync("tick", PeriodicSchedule.Every(TimeSpan.FromSeconds(1)), new RecordArgs("tick"));
        await WaitUntilAsync(async () => (await RunsAsync(app, "tick")).Count >= 1);

        var paused = await app.Client.PausePeriodicJobAsync("tick");
        Assert.NotNull(paused!.PausedAt);
        var runs = (await RunsAsync(app, "tick")).Count;
        await Task.Delay(TimeSpan.FromSeconds(2.5));
        Assert.Equal(runs, (await RunsAsync(app, "tick")).Count);

        var resumed = await app.Client.ResumePeriodicJobAsync("tick");
        Assert.Null(resumed!.PausedAt);
        Assert.True(resumed.NextRunAt > DateTimeOffset.UtcNow.AddMilliseconds(-100));
        await WaitUntilAsync(async () => (await RunsAsync(app, "tick")).Count > runs);
    }

    [Fact]
    public async Task Code_registered_jobs_keep_their_run_times_across_restarts()
    {
        var prefix = await database.CreateSchemaAsync();
        void Configure(OxbowOptions o)
        {
            Queues(o);
            o.AddPeriodicJob("hourly", PeriodicSchedule.Every(TimeSpan.FromHours(1)), () => new RecordArgs("hourly"), runOnStart: true);
        }

        PeriodicJobRow first;
        await using (var app = await TestApp.StartAsync(database, prefix, Configure, Workers))
        {
            await WaitUntilAsync(async () => (await RunsAsync(app, "hourly")).Count == 1);
            first = (await app.Client.GetPeriodicJobAsync("hourly"))!;
            Assert.True(first.RegisteredInCode);
            Assert.Equal("record", first.Kind);
            Assert.NotNull(first.LastRunAt);
            Assert.InRange(first.NextRunAt!.Value, DateTimeOffset.UtcNow.AddMinutes(59), DateTimeOffset.UtcNow.AddMinutes(61));
        }

        // Run on start applies to the first run only, not to every new leader.
        await using (var app = await TestApp.StartAsync(database, prefix, Configure, Workers))
        {
            await WaitUntilAsync(() => ElectorOf(app).IsLeader);
            await Task.Delay(TimeSpan.FromSeconds(1));
            Assert.Single(await RunsAsync(app, "hourly"));
            var second = (await app.Client.GetPeriodicJobAsync("hourly"))!;
            Assert.Equal(first.LastRunAt, second.LastRunAt);
            Assert.Equal(first.NextRunAt, second.NextRunAt);
        }

        // Down past three runs: one catch-up run, then the schedule resumes from now.
        await database.ExecuteAsOwnerAsync($"UPDATE public.{prefix}periodic_job SET next_run_at = now() - interval '3 hours'");
        await using (var app = await TestApp.StartAsync(database, prefix, Configure, Workers))
        {
            await WaitUntilAsync(async () => (await RunsAsync(app, "hourly")).Count == 2);
            await Task.Delay(TimeSpan.FromSeconds(1));
            Assert.Equal(2, (await RunsAsync(app, "hourly")).Count);
            Assert.InRange((await app.Client.GetPeriodicJobAsync("hourly"))!.NextRunAt!.Value, DateTimeOffset.UtcNow.AddMinutes(59), DateTimeOffset.UtcNow.AddMinutes(61));
        }
    }

    [Fact]
    public async Task Code_registered_jobs_can_be_paused_but_not_replaced()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o =>
        {
            Queues(o);
            o.AddPeriodicJob(PeriodicSchedule.Every(TimeSpan.FromHours(1)), () => new RecordArgs("unnamed"));
        }, Workers);

        const string derived = "record every 01:00:00";
        await WaitUntilAsync(async () => await app.Client.GetPeriodicJobAsync(derived) is not null);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            app.Client.UpsertPeriodicJobAsync(derived, PeriodicSchedule.Every(TimeSpan.FromMinutes(1)), new RecordArgs("x")));
        Assert.NotNull((await app.Client.PausePeriodicJobAsync(derived))!.PausedAt);

        // Deleting the row of a job still in code resets it; the leader recreates it.
        await app.Client.DeletePeriodicJobAsync(derived);
        await WaitUntilAsync(async () => await app.Client.GetPeriodicJobAsync(derived) is { PausedAt: null });
    }

    [Fact]
    public async Task A_failover_neither_skips_nor_repeats_a_run()
    {
        var prefix = await database.CreateSchemaAsync();
        var interval = TimeSpan.FromSeconds(2);
        void Configure(OxbowOptions o)
        {
            Queues(o);
            o.AddPeriodicJob("tick", PeriodicSchedule.Every(interval), () => new RecordArgs("tick"));
        }

        var a = await TestApp.StartAsync(database, prefix, Configure, Workers);
        var b = await TestApp.StartAsync(database, prefix, Configure, Workers);
        try
        {
            await WaitUntilAsync(async () => (await RunsAsync(a, "tick")).Count >= 2, TimeSpan.FromSeconds(20));
            var leader = ElectorOf(a).IsLeader ? a : b;
            var follower = leader == a ? b : a;
            await leader.DisposeAsync();
            await WaitUntilAsync(() => ElectorOf(follower).IsLeader);

            var before = (await RunsAsync(follower, "tick")).Count;
            await WaitUntilAsync(async () => (await RunsAsync(follower, "tick")).Count >= before + 2, TimeSpan.FromSeconds(20));

            var created = (await RunsAsync(follower, "tick")).Select(j => j.CreatedAt).Order().ToList();
            var gaps = created.Zip(created.Skip(1), (x, y) => y - x).ToList();
            Assert.All(gaps, gap => Assert.InRange(gap, interval * 0.5, interval * 1.5));
        }
        finally
        {
            await a.DisposeAsync();
            await b.DisposeAsync();
        }
    }

    [Fact]
    public async Task Two_leaders_enqueue_a_due_run_once()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, register: Workers, start: false);
        await app.Client.UpsertPeriodicJobAsync("tick", PeriodicSchedule.Every(TimeSpan.FromHours(1)), new RecordArgs("tick"));
        await database.ExecuteAsOwnerAsync($"UPDATE public.{prefix}periodic_job SET next_run_at = now() - interval '1 second'");
        var row = (await app.Client.GetPeriodicJobAsync("tick"))!;

        // Both read the row before either advances it, as across a failover.
        var due = new PeriodicEnqueuer.DueRun(
            row.Name, false, row.Kind, row.Args!.Value.GetRawText(), null, null, null, 3600, row.NextRunAt!.Value, row.UpdatedAt, DateTimeOffset.UtcNow);
        var runtime = app.Host.Services.GetRequiredService<OxbowRuntime>();
        var client = app.Host.Services.GetRequiredService<OxbowClient>();
        var enqueuers = Enumerable.Range(0, 4)
            .Select(_ => new PeriodicEnqueuer(runtime, client, TimeProvider.System, NullLogger.Instance))
            .ToList();
        await Task.WhenAll(enqueuers.Select(e => e.EnqueueAsync(due, new Dictionary<string, PeriodicJob>(), CancellationToken.None)));

        Assert.Single(await RunsAsync(app, "tick"));
        Assert.True((await app.Client.GetPeriodicJobAsync("tick"))!.NextRunAt > DateTimeOffset.UtcNow.AddMinutes(59));
    }

    [Fact]
    public async Task A_stored_schedule_the_leader_cant_read_is_parked()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, Queues, Workers);
        await database.ExecuteAsOwnerAsync($"""
            INSERT INTO public.{prefix}periodic_job (name, kind, args, cron, time_zone, next_run_at)
            VALUES ('broken', 'record', '{"{"}"value": "x"{"}"}', 'not a cron', 'UTC', now() - interval '1 second')
            """);

        await WaitUntilAsync(async () =>
            await database.ScalarAsOwnerAsync<bool>($"SELECT next_run_at IS NULL FROM public.{prefix}periodic_job WHERE name = 'broken'"));
        Assert.Empty(await RunsAsync(app, "broken"));

        // Reads don't parse the schedule, so the row still lists.
        var listed = Assert.Single((await app.Client.ListPeriodicJobsAsync(new())).Jobs);
        Assert.Equal("not a cron", listed.CronExpression);
        Assert.Equal("UTC", listed.TimeZoneId);
    }

    private static Elector ElectorOf(TestApp app) =>
        app.Host.Services.GetServices<IHostedService>().OfType<OxbowHostedService>().Single().Elector!;

    private static async Task<List<JobRow>> RunsAsync(TestApp app, string name)
    {
        var page = await app.Client.ListAsync(new JobListQuery { Limit = 10_000 });
        return page.Jobs
            .Where(j => j.Metadata.TryGetProperty("oxbow:periodic_job", out var value) && value.GetString() == name)
            .ToList();
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

    private sealed class StampSource : IJobInsertMiddleware
    {
        public Task<IReadOnlyList<InsertResult>> InvokeAsync(JobInsertContext context, JobInsertDelegate next)
        {
            foreach (var job in context.Jobs)
            {
                job.Metadata["source"] = JsonValue.Create("middleware");
            }

            return next(context);
        }
    }
}

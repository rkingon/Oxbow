using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Oxbow.Internal;
using Oxbow.Processing;

namespace Oxbow.IntegrationTests;

// Timings are read from attempted_at, which is the now() the limiter decided with, so lower bounds are exact. Upper
// bounds leave room for timer and round-trip latency per gap: with Burst = 1 a late start isn't made up, so lateness
// adds up over a run.
public sealed class RateLimitTests(PostgresFixture database)
{
    private static readonly TimeSpan Slack = TimeSpan.FromMilliseconds(300);

    [Fact]
    public async Task One_per_second_holds_across_two_nodes()
    {
        var prefix = await database.CreateSchemaAsync();
        void Configure(OxbowOptions o) =>
            o.Queues["default"] = new QueueOptions { MaxWorkers = 10, RateLimit = new(1, TimeSpan.FromSeconds(1)) };
        await using var a = await StartAsync(prefix, Configure);
        await using var b = await StartAsync(prefix, Configure);

        await a.Client.InsertManyAsync(Enumerable.Range(0, 5).Select(i => new InsertManyItem(new RecordArgs($"{i}"))));
        await WaitCompletedAsync(prefix, 5);

        AssertSpacing(await StartsAsync(prefix), TimeSpan.FromSeconds(1), Slack);
        Assert.Equal(5L, await database.ScalarAsOwnerAsync<long>($"SELECT count(*) FROM {prefix}job WHERE attempt = 1"));
    }

    [Fact]
    public async Task A_large_backlog_drains_at_the_rate_across_two_nodes()
    {
        var prefix = await database.CreateSchemaAsync();
        void Configure(OxbowOptions o) =>
            o.Queues["default"] = new QueueOptions { MaxWorkers = 50, RateLimit = new(50, TimeSpan.FromSeconds(1)) };
        await using var a = await StartAsync(prefix, Configure);
        await using var b = await StartAsync(prefix, Configure);

        await a.Client.InsertManyAsync(Enumerable.Range(0, 2_000).Select(i => new InsertManyItem(new RecordArgs($"{i}"))));
        await Task.Delay(TimeSpan.FromSeconds(3));

        var starts = await StartsAsync(prefix);
        AssertSpacing(starts, TimeSpan.FromMilliseconds(20));
        var span = starts[^1] - starts[0];
        Assert.True(span > TimeSpan.FromSeconds(2));
        Assert.InRange(starts.Count, (int)(span.TotalSeconds * 50 / 2), (int)(span.TotalSeconds * 50) + 1);
        Assert.Equal(0L, await database.ScalarAsOwnerAsync<long>(
            $"SELECT count(*) FROM {prefix}job WHERE state = 'available' AND attempt > 0"));
    }

    [Fact]
    public async Task A_burst_starts_at_once_then_the_rate_takes_over()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o =>
            o.Queues["default"] = new QueueOptions { RateLimit = new(2, TimeSpan.FromSeconds(1)) { Burst = 3 } });

        await app.Client.InsertManyAsync(Enumerable.Range(0, 7).Select(i => new InsertManyItem(new RecordArgs($"{i}"))));
        await WaitCompletedAsync(prefix, 7);

        // Measured from the burst: with Burst > 1 the limiter makes up a late start, so consecutive starts can be closer
        // than the interval.
        var starts = await StartsAsync(prefix);
        Assert.Equal(starts[0], starts[2]);
        for (var i = 3; i < starts.Count; i++)
        {
            var due = TimeSpan.FromMilliseconds(500 * (i - 2));
            Assert.InRange(starts[i] - starts[0], due, due + Slack);
        }
    }

    [Fact]
    public async Task Waiting_jobs_stay_available_without_spending_an_attempt()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o =>
            o.Queues["default"] = new QueueOptions { RateLimit = new(1, TimeSpan.FromMinutes(1)) });

        var jobs = await app.Client.InsertManyAsync(Enumerable.Range(0, 3).Select(i => new InsertManyItem(new RecordArgs($"{i}"))));
        await app.Recorder.WaitStartedAsync(jobs[0].Job.Id);
        await Task.Delay(TimeSpan.FromSeconds(1.5));

        foreach (var inserted in jobs.Skip(1))
        {
            var job = (await app.Client.GetAsync(inserted.Job.Id))!;
            Assert.Equal((JobState.Available, 0, inserted.Job.ScheduledAt), (job.State, job.Attempt, job.ScheduledAt));
            Assert.Empty(job.Errors);
        }

        Assert.Single(app.Recorder.RunCounts);
    }

    [Fact]
    public async Task The_producer_wakes_when_the_next_start_is_due_not_at_the_next_poll()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o =>
        {
            o.FetchPollInterval = TimeSpan.FromSeconds(1);
            o.Queues["default"] = new QueueOptions { RateLimit = new(20, TimeSpan.FromSeconds(1)) };
        });

        await app.Client.InsertManyAsync(Enumerable.Range(0, 41).Select(i => new InsertManyItem(new RecordArgs($"{i}"))));
        await WaitCompletedAsync(prefix, 41);

        // 20/s is twice what the 100 ms fetch cooldown would allow and twenty times the poll. The median gap shrugs off
        // the odd late wake on a loaded machine.
        var starts = await StartsAsync(prefix);
        AssertSpacing(starts, TimeSpan.FromMilliseconds(50));
        var gaps = starts.Skip(1).Zip(starts, (later, earlier) => later - earlier).Order().ToList();
        Assert.InRange(gaps[gaps.Count / 2], TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(80));
    }

    [Fact]
    public async Task Rate_and_concurrency_limits_combine()
    {
        var prefix = await database.CreateSchemaAsync();
        var tracker = new LimitTracker();
        await using var app = await StartAsync(prefix, o => o.Queues["default"] = new QueueOptions
        {
            RateLimit = new(10, TimeSpan.FromSeconds(1)),
            ConcurrencyLimit = new() { Global = 1 },
        }, tracker);

        await app.Client.InsertManyAsync(new[] { "a", "b" }.SelectMany(key => Enumerable.Range(0, 4)
            .Select(_ => new InsertManyItem(new LimitedArgs(key, DelayMilliseconds: 300)))));
        await WaitCompletedAsync(prefix, 8);

        Assert.All(tracker.MaxConcurrent.Values, max => Assert.Equal(1, max));
        AssertSpacing(await StartsAsync(prefix), TimeSpan.FromMilliseconds(100));
        foreach (var key in new[] { "a", "b" })
        {
            AssertSpacing(await StartsAsync(prefix, key), TimeSpan.FromMilliseconds(300));
        }
    }

    [Fact]
    public async Task Partition_rate_limits_are_independent_across_keys()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o =>
            o.Queues["default"] = new QueueOptions { PartitionRateLimit = new(2, TimeSpan.FromSeconds(1)) });

        await app.Client.InsertManyAsync(new[] { "a", "b", "c" }.SelectMany(key => Enumerable.Range(0, 4)
            .Select(_ => new InsertManyItem(new LimitedArgs(key)))));
        var unkeyed = await app.Client.InsertManyAsync(Enumerable.Range(0, 10).Select(i => new InsertManyItem(new RecordArgs($"{i}"))));
        await WaitCompletedAsync(prefix, 22);

        var firsts = new List<DateTime>();
        foreach (var key in new[] { "a", "b", "c" })
        {
            var starts = await StartsAsync(prefix, key);
            AssertSpacing(starts, TimeSpan.FromMilliseconds(500), Slack);
            firsts.Add(starts[0]);
        }

        Assert.InRange(firsts.Max() - firsts.Min(), TimeSpan.Zero, Slack);
        var unkeyedStarts = await StartsAsync(prefix, unkeyedOnly: true);
        Assert.InRange(unkeyedStarts[^1] - unkeyedStarts[0], TimeSpan.Zero, Slack);
    }

    [Fact]
    public async Task Partition_and_queue_rate_limits_combine()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o => o.Queues["default"] = new QueueOptions
        {
            RateLimit = new(4, TimeSpan.FromSeconds(1)),
            PartitionRateLimit = new(1, TimeSpan.FromSeconds(1)),
        });

        await app.Client.InsertManyAsync(new[] { "a", "b" }.SelectMany(key => Enumerable.Range(0, 3)
            .Select(_ => new InsertManyItem(new LimitedArgs(key)))));
        await WaitCompletedAsync(prefix, 6);

        AssertSpacing(await StartsAsync(prefix), TimeSpan.FromMilliseconds(250));
        foreach (var key in new[] { "a", "b" })
        {
            AssertSpacing(await StartsAsync(prefix, key), TimeSpan.FromSeconds(1), Slack);
        }
    }

    [Fact]
    public async Task The_limiter_survives_a_node_restart()
    {
        var prefix = await database.CreateSchemaAsync();
        void Configure(OxbowOptions o) =>
            o.Queues["default"] = new QueueOptions { RateLimit = new(1, TimeSpan.FromSeconds(3)) };

        await using (var first = await StartAsync(prefix, Configure))
        {
            var job = await first.Client.InsertAsync(new RecordArgs("first"));
            await first.WaitForStateAsync(job.Job.Id, JobState.Completed);
        }

        await using var second = await StartAsync(prefix, Configure);
        await second.Client.InsertAsync(new RecordArgs("second"));
        await WaitCompletedAsync(prefix, 2);

        var starts = await StartsAsync(prefix);
        Assert.InRange(starts[1] - starts[0], TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(3) + Slack);
    }

    [Fact]
    public async Task Unlimited_queues_are_unaffected()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o =>
        {
            o.Queues["default"] = new QueueOptions();
            o.Queues["limited"] = new QueueOptions { RateLimit = new(1, TimeSpan.FromMinutes(1)) };
        });

        await app.Client.InsertManyAsync(Enumerable.Range(0, 2).Select(i =>
            new InsertManyItem(new RecordArgs($"limited-{i}"), new InsertOptions { Queue = "limited" })));
        await app.Client.InsertManyAsync(Enumerable.Range(0, 20).Select(i => new InsertManyItem(new RecordArgs($"{i}"))));
        await WaitCompletedAsync(prefix, 21);

        var starts = await StartsAsync(prefix, queue: "default");
        Assert.Equal(20, starts.Count);
        Assert.InRange(starts[^1] - starts[0], TimeSpan.Zero, Slack);
        Assert.Null(await database.ScalarAsOwnerAsync<DateTime?>($"SELECT rate_limit_tat FROM {prefix}queue WHERE name = 'default'"));
        Assert.NotNull(await database.ScalarAsOwnerAsync<DateTime?>($"SELECT rate_limit_tat FROM {prefix}queue WHERE name = 'limited'"));
    }

    [Fact]
    public async Task A_fetch_takes_what_the_limiter_allows_and_reports_when_the_next_start_is_due()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        var sql = new OxbowSql("public", prefix);
        var limits = new FetchLimits(null, new RateLimit(1, TimeSpan.FromHours(1)) { Burst = 3 }, new RateLimit(1, TimeSpan.FromHours(2)) { Burst = 2 });
        await app.Client.InsertManyAsync(
        [
            .. Enumerable.Range(0, 3).Select(_ => new InsertManyItem(new LimitedArgs("a"))),
            new InsertManyItem(new LimitedArgs("b")),
            new InsertManyItem(new LimitedArgs("b")),
        ]);

        var first = await FetchAsync(app, sql, limits);
        Assert.Equal(["a", "a", "b"], first.Jobs.Select(j => j.PartitionKey![13..^2]));
        Assert.InRange(first.RateWait!.Value, TimeSpan.FromMinutes(59), TimeSpan.FromHours(1));

        // Key a used its burst of two; b has a start left, so isn't throttled.
        Assert.InRange(first.PartitionRateWait!.Value, TimeSpan.FromMinutes(119), TimeSpan.FromHours(2));

        // The queue has no allowance left, however much each key has.
        var second = await FetchAsync(app, sql, limits);
        Assert.Empty(second.Jobs);
        Assert.InRange(second.RateWait!.Value, TimeSpan.FromMinutes(59), TimeSpan.FromHours(1));

        // With the queue limiter caught up, key a is out of burst and b has one start left.
        await database.ExecuteAsOwnerAsync($"UPDATE {prefix}queue SET rate_limit_tat = NULL");
        var third = await FetchAsync(app, sql, limits);
        Assert.Equal("b", Assert.Single(third.Jobs).PartitionKey![13..^2]);
        Assert.InRange(third.PartitionRateWait!.Value, TimeSpan.FromMinutes(119), TimeSpan.FromHours(2));
        Assert.Equal(1L, await database.ScalarAsOwnerAsync<long>($"SELECT count(*) FROM {prefix}job WHERE state = 'available'"));
    }

    [Fact]
    public async Task Maintenance_deletes_partition_limiters_once_they_have_caught_up()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o =>
            o.Queues["default"] = new QueueOptions { PartitionRateLimit = new(10, TimeSpan.FromSeconds(1)) });

        await app.Client.InsertManyAsync(new[] { "a", "b", "c" }.Select(key => new InsertManyItem(new LimitedArgs(key))));
        await WaitCompletedAsync(prefix, 3);
        Assert.Equal(3L, await database.ScalarAsOwnerAsync<long>($"SELECT count(*) FROM {prefix}rate_limit"));

        // Rows are kept a minute past catching up; age two of them past that.
        await database.ExecuteAsOwnerAsync(
            $"UPDATE {prefix}rate_limit SET tat = now() - interval '2 minutes' WHERE partition_key <> '{{\"customer\":\"c\"}}'");
        await database.ExecuteAsOwnerAsync(
            $"UPDATE {prefix}rate_limit SET tat = now() + interval '1 hour' WHERE partition_key = '{{\"customer\":\"c\"}}'");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (await database.ScalarAsOwnerAsync<long>($"SELECT count(*) FROM {prefix}rate_limit") > 1)
        {
            await Task.Delay(100, cts.Token);
        }

        Assert.Equal("""{"customer":"c"}""", await database.ScalarAsOwnerAsync<string>($"SELECT partition_key FROM {prefix}rate_limit"));
    }

    [Fact]
    public async Task A_fetch_that_waited_for_the_queue_lock_judges_by_when_it_got_it()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        var sql = new OxbowSql("public", prefix);
        var limits = new FetchLimits(null, new RateLimit(1, TimeSpan.FromSeconds(1)));
        await app.Client.InsertManyAsync(Enumerable.Range(0, 2).Select(i => new InsertManyItem(new RecordArgs($"{i}"))));

        await using var holder = await app.DataSource.OpenConnectionAsync();
        await using var holding = await holder.BeginTransactionAsync();
        await using (var queueLock = new NpgsqlCommand(sql.QueueLock, holder, holding))
        {
            queueLock.Parameters.AddWithValue("queue", "default");
            await queueLock.ExecuteNonQueryAsync();
        }

        // Both fetches begin before the lock is free, the second more than an interval after the first: by their
        // transactions' start times both would be allowed, though they'd start together.
        var first = FetchAsync(app, sql, limits);
        await Task.Delay(TimeSpan.FromSeconds(1.2));
        var second = FetchAsync(app, sql, limits);
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        Assert.False(first.IsCompleted || second.IsCompleted);
        await holding.CommitAsync();

        Assert.Equal(1, (await first).Jobs.Count + (await second).Jobs.Count);
    }

    [Fact]
    public async Task A_wait_longer_than_a_timer_allows_still_starts_what_was_fetched()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o =>
            o.Queues["default"] = new QueueOptions { RateLimit = new(1, TimeSpan.FromDays(60)) });

        var jobs = await app.Client.InsertManyAsync(Enumerable.Range(0, 2).Select(i => new InsertManyItem(new RecordArgs($"{i}"))));
        await app.WaitForStateAsync(jobs[0].Job.Id, JobState.Completed);
        await Task.Delay(TimeSpan.FromSeconds(1));

        var waiting = (await app.Client.GetAsync(jobs[1].Job.Id))!;
        Assert.Equal((JobState.Available, 0), (waiting.State, waiting.Attempt));
    }

    [Fact]
    public async Task The_queue_cleaner_keeps_a_queue_whose_limiter_is_ahead()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        await database.ExecuteAsOwnerAsync($"""
            INSERT INTO {prefix}queue (name, updated_at, rate_limit_tat) VALUES
                ('ahead', now() - interval '2 days', now() + interval '1 day'),
                ('caught_up', now() - interval '2 days', now() - interval '1 day'),
                ('unlimited', now() - interval '2 days', NULL)
            """);

        await using (var clean = app.DataSource.CreateCommand(new OxbowSql("public", prefix).QueueDeleteExpired))
        {
            clean.Parameters.AddWithValue("horizon", DateTimeOffset.UtcNow.AddDays(-1));
            await clean.ExecuteNonQueryAsync();
        }

        Assert.Equal("ahead", await database.ScalarAsOwnerAsync<string>(
            $"SELECT string_agg(name, ',') FROM {prefix}queue WHERE name IN ('ahead', 'caught_up', 'unlimited')"));
    }

    private static async Task<LimitedFetch> FetchAsync(TestApp app, OxbowSql sql, FetchLimits limits)
    {
        await using var connection = await app.DataSource.OpenConnectionAsync();
        await using var batch = new NpgsqlBatch(connection);
        return await Producer.FetchLimitedAsync(batch, sql, "default", limits, 10, "test", [], [], default);
    }

    /// <summary>Asserts consecutive starts are at least <paramref name="interval"/> apart, and at most <paramref name="late"/> more.</summary>
    private static void AssertSpacing(List<DateTime> starts, TimeSpan interval, TimeSpan? late = null)
    {
        for (var i = 1; i < starts.Count; i++)
        {
            var gap = starts[i] - starts[i - 1];
            Assert.True(gap >= interval && (late is null || gap <= interval + late), $"Starts {i - 1} and {i} are {gap.TotalMilliseconds} ms apart.");
        }
    }

    private Task<TestApp> StartAsync(string prefix, Action<OxbowOptions> configure, LimitTracker? tracker = null) =>
        TestApp.StartAsync(database, prefix, configure, b =>
        {
            b.Services.AddSingleton(tracker ?? new LimitTracker());
            b.AddWorker<LimitedArgs, LimitedWorker>().AddWorker<RecordArgs, RecordWorker>();
        });

    private async Task WaitCompletedAsync(string prefix, long count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (await database.ScalarAsOwnerAsync<long>($"SELECT count(*) FROM {prefix}job WHERE state = 'completed'") < count)
        {
            await Task.Delay(50, cts.Token);
        }
    }

    private async Task<List<DateTime>> StartsAsync(string prefix, string? key = null, bool unkeyedOnly = false, string? queue = null)
    {
        var where = key is not null ? $"partition_key = '{{\"customer\":\"{key}\"}}'"
            : unkeyedOnly ? "partition_key IS NULL"
            : queue is not null ? $"queue = '{queue}'"
            : "true";
        await using var connection = new NpgsqlConnection(database.OwnerConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"SELECT attempted_at FROM {prefix}job WHERE attempted_at IS NOT NULL AND {where} ORDER BY attempted_at", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var starts = new List<DateTime>();
        while (await reader.ReadAsync())
        {
            starts.Add(reader.GetDateTime(0));
        }

        return starts;
    }
}

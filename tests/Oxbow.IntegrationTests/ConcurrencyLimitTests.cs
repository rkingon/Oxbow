using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Oxbow.Internal;
using Oxbow.Processing;

namespace Oxbow.IntegrationTests;

public sealed class ConcurrencyLimitTests(PostgresFixture database)
{
    [Fact]
    public async Task Global_limit_holds_across_nodes_under_load()
    {
        var prefix = await database.CreateSchemaAsync();
        var tracker = new LimitTracker();
        void Configure(OxbowOptions o) =>
            o.Queues["default"] = new QueueOptions { MaxWorkers = 20, ConcurrencyLimit = new() { Global = 2 } };
        await using var a = await StartAsync(prefix, tracker, Configure);
        await using var b = await StartAsync(prefix, tracker, Configure);
        await using var c = await StartAsync(prefix, tracker, Configure);

        // Each insert's NOTIFY wakes all three nodes at once, so their fetches race for the same keys.
        var sampling = SampleWhileAsync(prefix, "partition_key", () => tracker.Completed < 180);
        for (var round = 0; round < 3; round++)
        {
            await a.Client.InsertManyAsync(Enumerable.Range(0, 10).SelectMany(key => Enumerable.Range(0, 6)
                .Select(_ => new InsertManyItem(new LimitedArgs($"{round}-{key}", DelayMilliseconds: 50)))));
            await Task.Delay(500);
        }

        var maxRunning = await sampling;

        Assert.Equal(180, tracker.Completed);
        Assert.Equal(2, maxRunning);
        Assert.All(tracker.MaxConcurrent.Values, max => Assert.InRange(max, 1, 2));
        Assert.Contains(2, tracker.MaxConcurrent.Values);
        Assert.True(tracker.Nodes.Count >= 2);
    }

    [Fact]
    public async Task A_global_fetch_waits_for_the_queue_lock_and_counts_what_the_holder_started()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        var sql = new OxbowSql("public", prefix);
        var limit = new ConcurrencyLimit { Global = 1 };
        var low = await app.Client.InsertAsync(new LimitedArgs("k"), new InsertOptions { Priority = 2 });

        await using var first = await app.DataSource.OpenConnectionAsync();
        await using var holder = await first.BeginTransactionAsync();
        await using (var batch = new NpgsqlBatch(first, holder))
        {
            var taken = await Producer.FetchLimitedAsync(batch, sql, "default", limit, 10, "first", [], [], default);
            Assert.Equal(low.Job.Id, Assert.Single(taken).Id);
        }

        // Sorts ahead of the job just taken, so a fetch that didn't wait for the holder's commit would see room for it.
        var high = await app.Client.InsertAsync(new LimitedArgs("k"), new InsertOptions { Priority = 1 });

        await using var second = await app.DataSource.OpenConnectionAsync();
        await using var waitingBatch = new NpgsqlBatch(second);
        var waiting = Producer.FetchLimitedAsync(waitingBatch, sql, "default", limit, 10, "second", [], [], default);
        await Task.Delay(500);
        Assert.False(waiting.IsCompleted);

        await holder.CommitAsync();
        Assert.Empty(await waiting);
        Assert.Equal(JobState.Available, (await app.Client.GetAsync(high.Job.Id))!.State);
    }

    [Fact]
    public async Task A_batch_holds_the_queue_lock_until_its_last_statement()
    {
        // The limited fetch relies on Npgsql running a batch without a transaction as one implicit transaction.
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        var sql = new OxbowSql("public", prefix);
        await database.ExecuteAsOwnerAsync($"INSERT INTO {prefix}queue (name) VALUES ('default')");

        await using var holder = await app.DataSource.OpenConnectionAsync();
        await using var batch = new NpgsqlBatch(holder);
        var queueLock = new NpgsqlBatchCommand(sql.QueueLock);
        queueLock.Parameters.AddWithValue("queue", "default");
        batch.BatchCommands.Add(queueLock);
        batch.BatchCommands.Add(new NpgsqlBatchCommand("SELECT pg_sleep(1)"));
        var running = batch.ExecuteNonQueryAsync();
        await Task.Delay(300);

        await using var probe = await app.DataSource.OpenConnectionAsync();
        await using var nowait = new NpgsqlCommand($"SELECT 1 FROM {prefix}queue WHERE name = 'default' FOR UPDATE NOWAIT", probe);
        var probed = await Record.ExceptionAsync(() => nowait.ExecuteScalarAsync());
        await running;
        Assert.Equal(PostgresErrorCodes.LockNotAvailable, Assert.IsType<PostgresException>(probed).SqlState);
    }

    [Fact]
    public async Task Local_limit_holds_per_node_until_the_outcome_is_written()
    {
        var prefix = await database.CreateSchemaAsync();
        var tracker = new LimitTracker();
        void Configure(OxbowOptions o) =>
            o.Queues["default"] = new QueueOptions { MaxWorkers = 10, ConcurrencyLimit = new() { Local = 1 } };
        await using var a = await StartAsync(prefix, tracker, Configure);
        await using var b = await StartAsync(prefix, tracker, Configure);

        // Instant jobs: each finishes long before the completer writes it, which is the window a freed slot could
        // double-book the key in.
        await a.Client.InsertManyAsync(Enumerable.Range(0, 60).Select(_ => new InsertManyItem(new LimitedArgs("a"))));

        var maxRunningPerNode = await SampleWhileAsync(
            prefix, "partition_key, attempted_by[array_length(attempted_by, 1)]", () => tracker.Completed < 60);

        Assert.Equal(60, tracker.Completed);
        Assert.Equal(1, maxRunningPerNode);
        Assert.Equal(2, tracker.Nodes.Count);
    }

    [Fact]
    public async Task Over_limit_jobs_stay_available_without_spending_an_attempt()
    {
        var prefix = await database.CreateSchemaAsync();
        var tracker = new LimitTracker();
        await using var app = await StartAsync(prefix, tracker, o =>
            o.Queues["default"] = new QueueOptions { MaxWorkers = 5, ConcurrencyLimit = new() { Global = 1 } });

        var jobs = await app.Client.InsertManyAsync(Enumerable.Range(0, 3)
            .Select(_ => new InsertManyItem(new LimitedArgs("k", Block: true))));
        await app.Recorder.WaitStartedAsync(jobs[0].Job.Id);
        await Task.Delay(TimeSpan.FromSeconds(1));

        var waiting = await Task.WhenAll(jobs.Skip(1).Select(j => app.Client.GetAsync(j.Job.Id)));
        Assert.All(waiting, j => Assert.Equal((JobState.Available, 0), (j!.State, j.Attempt)));
        Assert.Single(app.Recorder.RunCounts);

        await app.Client.CancelAsync(jobs[0].Job.Id);
        await app.Recorder.WaitStartedAsync(jobs[1].Job.Id);
        Assert.Equal(JobState.Available, (await app.Client.GetAsync(jobs[2].Job.Id))!.State);
    }

    [Fact]
    public async Task A_saturated_key_does_not_starve_the_others()
    {
        var prefix = await database.CreateSchemaAsync();
        var tracker = new LimitTracker();
        await using var app = await StartAsync(prefix, tracker, o =>
            o.Queues["default"] = new QueueOptions { MaxWorkers = 4, ConcurrencyLimit = new() { Global = 1 } });

        // The hot key's backlog sorts ahead of every other job and is far larger than one fetch's scan window.
        var blocker = await app.Client.InsertAsync(new LimitedArgs("hot", Block: true));
        await app.Recorder.WaitStartedAsync(blocker.Job.Id);
        await app.Client.InsertManyAsync(Enumerable.Range(0, 300).Select(_ => new InsertManyItem(new LimitedArgs("hot"))));
        await app.Client.InsertManyAsync(Enumerable.Range(0, 5).Select(i => new InsertManyItem(new LimitedArgs($"cold-{i}"))));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (tracker.Completed < 5)
        {
            await Task.Delay(50, cts.Token);
        }

        await Task.Delay(300);
        Assert.Equal(5, tracker.Completed);
        Assert.Equal(300L, await database.ScalarAsOwnerAsync<long>(
            $"SELECT count(*) FROM {prefix}job WHERE partition_key = '{{\"customer\":\"hot\"}}' AND state = 'available' AND attempt = 0"));
    }

    [Fact]
    public async Task Unlimited_queues_and_jobs_without_a_key_are_not_limited()
    {
        var prefix = await database.CreateSchemaAsync();
        var tracker = new LimitTracker();
        await using var app = await StartAsync(prefix, tracker, o =>
        {
            o.Queues["default"] = new QueueOptions { MaxWorkers = 5 };
            o.Queues["limited"] = new QueueOptions { MaxWorkers = 5, ConcurrencyLimit = new() { Global = 1, Local = 1 } };
        });

        var keyed = await app.Client.InsertManyAsync(Enumerable.Range(0, 5)
            .Select(_ => new InsertManyItem(new LimitedArgs("same", Block: true))));
        var unkeyed = await app.Client.InsertManyAsync(Enumerable.Range(0, 5)
            .Select(_ => new InsertManyItem(new BlockArgs(), new InsertOptions { Queue = "limited" })));

        await Task.WhenAll(keyed.Concat(unkeyed).Select(r => app.Recorder.WaitStartedAsync(r.Job.Id)));
    }

    [Fact]
    public async Task The_insert_option_wins_over_args_defaults_which_win_over_the_attribute()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        var derived = await app.Client.InsertAsync(new LimitedArgs("acme"));
        var explicitKey = await app.Client.InsertAsync(new LimitedArgs("acme"), new InsertOptions { PartitionKey = "explicit" });
        var argsDefault = await app.Client.InsertAsync(new DefaultKeyArgs("acme"));
        var argsDefaultOverridden = await app.Client.InsertAsync(new DefaultKeyArgs("acme"), new InsertOptions { PartitionKey = "explicit" });
        var none = await app.Client.InsertAsync(new RecordArgs("no key"));
        await app.Client.InsertManyFastAsync([new InsertManyItem(new LimitedArgs("copied"))]);

        Assert.Equal("""{"customer":"acme"}""", await KeyAsync(prefix, derived.Job.Id));
        Assert.Equal("""{"customer":"acme"}""", derived.Job.PartitionKey);
        Assert.Equal("explicit", (await app.Client.GetAsync(explicitKey.Job.Id))!.PartitionKey);
        Assert.Equal("explicit", await KeyAsync(prefix, explicitKey.Job.Id));
        Assert.Equal("from-args", await KeyAsync(prefix, argsDefault.Job.Id));
        Assert.Equal("explicit", await KeyAsync(prefix, argsDefaultOverridden.Job.Id));
        Assert.Null(await KeyAsync(prefix, none.Job.Id));
        Assert.Equal(1L, await database.ScalarAsOwnerAsync<long>(
            $"SELECT count(*) FROM {prefix}job WHERE partition_key = '{{\"customer\":\"copied\"}}'"));

        await Assert.ThrowsAsync<ArgumentException>(() =>
            app.Client.InsertAsync(new RecordArgs("too long"), new InsertOptions { PartitionKey = new string('k', 256) }));
    }

    [Fact]
    public async Task Insert_middleware_can_set_the_key()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, register: b => b.AddInsertMiddleware<KeyByTenant>());

        var derived = await app.Client.InsertAsync(new LimitedArgs("acme"));
        var none = await app.Client.InsertAsync(new RecordArgs("no key"));

        Assert.Equal("""tenant-1:{"customer":"acme"}""", await KeyAsync(prefix, derived.Job.Id));
        Assert.Equal("tenant-1:", await KeyAsync(prefix, none.Job.Id));
    }

    [Fact]
    public async Task Jobs_can_be_listed_and_managed_by_partition_key()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        await app.Client.InsertManyAsync(new[] { "acme", "acme", "globex" }.Select(c => new InsertManyItem(new LimitedArgs(c))));
        var acme = """{"customer":"acme"}""";

        var listed = await app.Client.ListAsync(new JobListQuery { PartitionKey = acme });
        Assert.Equal(2, listed.Jobs.Count);
        Assert.All(listed.Jobs, j => Assert.Equal(acme, j.PartitionKey));

        Assert.Equal(2L, await app.Client.DeleteManyAsync(new JobFilter { PartitionKey = acme }));
        var rest = Assert.Single((await app.Client.ListAsync(new JobListQuery())).Jobs);
        Assert.Equal("""{"customer":"globex"}""", rest.PartitionKey);
    }

    [Fact]
    public async Task A_locked_row_does_not_use_up_the_only_free_slot()
    {
        var prefix = await database.CreateSchemaAsync();
        var tracker = new LimitTracker();
        await using var app = await StartAsync(prefix, tracker, o =>
            o.Queues["default"] = new QueueOptions { MaxWorkers = 1, ConcurrencyLimit = new() { Global = 5 } });
        await app.Client.PauseQueueAsync("default");
        var held = await app.Client.InsertAsync(new LimitedArgs("a"));
        var next = await app.Client.InsertAsync(new LimitedArgs("b"));

        await using var owner = new NpgsqlConnection(database.OwnerConnectionString);
        await owner.OpenAsync();
        await using var holder = await owner.BeginTransactionAsync();
        await using (var lockRow = new NpgsqlCommand($"SELECT 1 FROM {prefix}job WHERE id = {held.Job.Id} FOR UPDATE", owner, holder))
        {
            await lockRow.ExecuteScalarAsync();
        }

        await app.Client.ResumeQueueAsync("default");
        await app.Recorder.WaitStartedAsync(next.Job.Id);
        await holder.RollbackAsync();
        await app.Recorder.WaitStartedAsync(held.Job.Id);
    }

    [Fact]
    public async Task A_failed_completion_write_keeps_the_local_slot_until_the_row_stops_running()
    {
        var prefix = await database.CreateSchemaAsync();
        var tracker = new LimitTracker();
        await using var app = await StartAsync(prefix, tracker, o =>
        {
            o.Queues["default"] = new QueueOptions { MaxWorkers = 5, ConcurrencyLimit = new() { Local = 1 } };
            o.RescueStuckJobsAfter = TimeSpan.FromSeconds(6);
            o.JobTimeout = TimeSpan.FromSeconds(2);
        });

        var first = await app.Client.InsertAsync(new LimitedArgs("k", DelayMilliseconds: 500));
        var second = await app.Client.InsertAsync(new LimitedArgs("k", DelayMilliseconds: 1000));
        await app.Recorder.WaitStartedAsync(first.Job.Id);

        // The completer can't write the first job's outcome and gives up, leaving its row running.
        var revoke = $"REVOKE UPDATE ON public.{prefix}job FROM {PostgresFixture.AppRole}";
        await database.ExecuteSerializedAsOwnerAsync(revoke);
        await Task.Delay(TimeSpan.FromSeconds(2.5));
        await database.ExecuteSerializedAsOwnerAsync(revoke.Replace("REVOKE", "GRANT").Replace(" FROM ", " TO "));

        var maxRunning = await SampleWhileAsync(
            prefix, "partition_key", () => app.Client.GetAsync(second.Job.Id).Result!.State != JobState.Completed);
        Assert.Equal(JobState.Completed, (await app.Client.GetAsync(second.Job.Id))!.State);
        Assert.Equal(1, maxRunning);
    }

    [Fact]
    public async Task Batch_callbacks_keep_their_partition_key()
    {
        var prefix = await database.CreateSchemaAsync();
        var tracker = new LimitTracker();
        await using var app = await TestApp.StartAsync(database, prefix, o =>
            o.Queues["default"] = new QueueOptions { ConcurrencyLimit = new() { Global = 1 } }, b =>
        {
            b.Services.AddSingleton(tracker);
            b.AddWorker<LimitedArgs, LimitedWorker>().AddWorker<RecordArgs, RecordWorker>().AddInsertMiddleware<KeyByTenant>();
        });

        var inserted = await app.Client.InsertBatchAsync([new InsertManyItem(new RecordArgs("member"))], new BatchOptions
        {
            OnComplete = new BatchCallback(new LimitedArgs("acme")),
            OnSuccess = new BatchCallback(new RecordArgs("done"), new InsertOptions { PartitionKey = "explicit" }),
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        Batch? batch;
        while ((batch = await app.Client.GetBatchAsync(inserted.BatchId))?.OnSuccessJobId is null)
        {
            await Task.Delay(50, cts.Token);
        }

        Assert.Equal("""tenant-1:{"customer":"acme"}""", (await app.Client.GetAsync(batch.OnCompleteJobId!.Value))!.PartitionKey);
        Assert.Equal("tenant-1:explicit", (await app.Client.GetAsync(batch.OnSuccessJobId.Value))!.PartitionKey);
    }

    private Task<TestApp> StartAsync(string prefix, LimitTracker tracker, Action<OxbowOptions> configure) =>
        TestApp.StartAsync(database, prefix, configure, b =>
        {
            b.Services.AddSingleton(tracker);
            b.AddWorker<LimitedArgs, LimitedWorker>().AddWorker<BlockArgs, BlockWorker>();
        });

    private Task<string?> KeyAsync(string prefix, long id) =>
        database.ScalarAsOwnerAsync<string>($"SELECT partition_key FROM {prefix}job WHERE id = {id}");

    /// <summary>Samples the most running jobs in any group until <paramref name="whileTrue"/> turns false.</summary>
    private async Task<long> SampleWhileAsync(string prefix, string groupBy, Func<bool> whileTrue)
    {
        var max = 0L;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (whileTrue() && !cts.IsCancellationRequested)
        {
            max = Math.Max(max, await database.ScalarAsOwnerAsync<long>($"""
                SELECT coalesce(max(n), 0) FROM (
                    SELECT count(*) AS n FROM {prefix}job
                    WHERE state = 'running' AND partition_key IS NOT NULL
                    GROUP BY {groupBy}
                ) AS s
                """));
        }

        return max;
    }

    private sealed class KeyByTenant : IJobInsertMiddleware
    {
        public Task<IReadOnlyList<InsertResult>> InvokeAsync(JobInsertContext context, JobInsertDelegate next)
        {
            foreach (var job in context.Jobs)
            {
                job.PartitionKey = $"tenant-1:{job.PartitionKey}";
            }

            return next(context);
        }
    }
}

[JobKind("limited")]
public sealed record LimitedArgs([property: PartitionKey] string Customer, int DelayMilliseconds = 0, bool Block = false) : IJobArgs;

[JobKind("default_key")]
public sealed record DefaultKeyArgs([property: PartitionKey] string Customer) : IJobArgs
{
    public InsertOptions GetInsertOptions() => new() { PartitionKey = "from-args" };
}

/// <summary>Concurrency per key as workers see it, shared across nodes.</summary>
public sealed class LimitTracker
{
    private readonly ConcurrentDictionary<string, int> _current = new();
    private int _completed;

    public ConcurrentDictionary<string, int> MaxConcurrent { get; } = new();

    public ConcurrentDictionary<string, bool> Nodes { get; } = new();

    public int Completed => Volatile.Read(ref _completed);

    public void Enter(string key, string node)
    {
        var now = _current.AddOrUpdate(key, 1, (_, n) => n + 1);
        MaxConcurrent.AddOrUpdate(key, now, (_, max) => Math.Max(max, now));
        Nodes[node] = true;
    }

    public void Exit(string key, bool completed)
    {
        _current.AddOrUpdate(key, 0, (_, n) => n - 1);
        if (completed)
        {
            Interlocked.Increment(ref _completed);
        }
    }
}

public sealed class LimitedWorker(LimitTracker tracker, JobRecorder recorder) : IWorker<LimitedArgs>
{
    public async Task WorkAsync(Job<LimitedArgs> job, CancellationToken cancellationToken)
    {
        recorder.Started(job.Id);
        tracker.Enter(job.Args.Customer, job.Row.AttemptedBy[^1]);
        var completed = false;
        try
        {
            await Task.Delay(job.Args.Block ? Timeout.InfiniteTimeSpan : TimeSpan.FromMilliseconds(job.Args.DelayMilliseconds), cancellationToken);
            completed = true;
        }
        finally
        {
            tracker.Exit(job.Args.Customer, completed);
        }
    }

    public TimeSpan? GetTimeout(Job<LimitedArgs> job) => job.Args.Block ? Timeout.InfiniteTimeSpan : null;
}

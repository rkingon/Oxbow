using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Oxbow.Processing;

namespace Oxbow.IntegrationTests;

[JobKind("seq")]
public sealed record SeqArgs(
    [property: SequenceKey] string Key,
    int N,
    int FailTimes = 0,
    int DelayMs = 0,
    string? Signal = null,
    string? Await = null) : IJobArgs;

public sealed class SeqWorker(SequenceLog log) : IWorker<SeqArgs>
{
    public async Task WorkAsync(Job<SeqArgs> job, CancellationToken cancellationToken)
    {
        log.Enter(job.Args.Key, job.Args.N);
        try
        {
            if (job.Args.Signal is { } signal)
            {
                log.Signal(signal);
            }

            if (job.Args.Await is { } other)
            {
                await log.WaitForSignalAsync(other).WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
            }

            if (job.Args.DelayMs > 0)
            {
                await Task.Delay(job.Args.DelayMs, cancellationToken);
            }

            if (job.Attempt <= job.Args.FailTimes)
            {
                throw new InvalidOperationException("Planned failure.");
            }
        }
        finally
        {
            log.Exit(job.Args.Key);
        }
    }

    public TimeSpan? GetTimeout(Job<SeqArgs> job) => TimeSpan.FromSeconds(30);

    public DateTimeOffset? GetNextRetry(Job<SeqArgs> job) => DateTimeOffset.UtcNow.AddMilliseconds(50);
}

/// <summary>What sequence workers saw, shared by every node in a test.</summary>
public sealed class SequenceLog
{
    private readonly ConcurrentDictionary<string, int> _active = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _signals = new(StringComparer.Ordinal);
    private readonly ConcurrentQueue<(string Key, int N)> _started = new();
    private int _overlaps;

    public int Overlaps => _overlaps;

    public void Enter(string key, int n)
    {
        if (_active.AddOrUpdate(key, 1, (_, c) => c + 1) > 1)
        {
            Interlocked.Increment(ref _overlaps);
        }

        _started.Enqueue((key, n));
    }

    public void Exit(string key) => _active.AddOrUpdate(key, 0, (_, c) => c - 1);

    public int[] Order(string key) => _started.Where(s => s.Key == key).Select(s => s.N).ToArray();

    public void Signal(string name) => SignalSource(name).TrySetResult();

    public Task WaitForSignalAsync(string name) => SignalSource(name).Task;

    private TaskCompletionSource SignalSource(string name) =>
        _signals.GetOrAdd(name, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
}

public sealed class SequenceTests(PostgresFixture database)
{
    private static string NewKey() => Guid.NewGuid().ToString("N");

    private Task<TestApp> StartWorkerAsync(string prefix, SequenceLog log, Action<OxbowOptions>? configure = null) =>
        TestApp.StartAsync(database, prefix, o =>
        {
            o.Queues["default"] = new QueueOptions { MaxWorkers = 10 };
            configure?.Invoke(o);
        }, b =>
        {
            b.AddWorker<SeqArgs, SeqWorker>();
            b.Services.AddSingleton(log);
        });

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(20));
        while (!await condition())
        {
            await Task.Delay(50, cts.Token);
        }
    }

    private static async Task WaitAllAsync(TestApp app, IEnumerable<long> ids, JobState state = JobState.Completed)
    {
        foreach (var id in ids)
        {
            await app.WaitForStateAsync(id, state, TimeSpan.FromSeconds(30));
        }
    }

    private static Elector ElectorOf(TestApp app) =>
        app.Host.Services.GetServices<IHostedService>().OfType<OxbowHostedService>().Single().Elector!;

    [Fact]
    public async Task Jobs_run_one_at_a_time_in_insert_order_across_two_nodes()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var a = await StartWorkerAsync(prefix, log);
        await using var b = await StartWorkerAsync(prefix, log);

        string[] keys = [NewKey(), NewKey(), NewKey()];
        var items = Enumerable.Range(0, 30)
            .SelectMany(n => keys.Select(k => new InsertManyItem(new SeqArgs(k, n, DelayMs: Random.Shared.Next(0, 10)))))
            .ToList();
        var results = await a.Client.InsertManyAsync(items);
        Assert.All(results, r => Assert.Equal(JobState.Pending, r.Job.State));

        await WaitAllAsync(a, results.Select(r => r.Job.Id));
        foreach (var key in keys)
        {
            Assert.Equal(Enumerable.Range(0, 30), log.Order(key));
        }

        Assert.Equal(0, log.Overlaps);
    }

    [Fact]
    public async Task Different_sequences_run_in_parallel()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log);
        var (x, y) = (NewKey(), NewKey());

        // Each waits until the other has started, so they only finish if they run at the same time.
        var results = await app.Client.InsertManyAsync([
            new InsertManyItem(new SeqArgs(x, 0, Signal: x, Await: y)),
            new InsertManyItem(new SeqArgs(y, 0, Signal: y, Await: x)),
        ]);

        await WaitAllAsync(app, results.Select(r => r.Job.Id));
    }

    [Fact]
    public async Task A_failing_head_is_retried_before_anything_behind_it_runs()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log);
        var key = NewKey();

        var results = await app.Client.InsertManyAsync(Enumerable.Range(0, 5)
            .Select(n => new InsertManyItem(new SeqArgs(key, n, FailTimes: n == 0 ? 2 : 0))));

        await WaitAllAsync(app, results.Select(r => r.Job.Id));
        Assert.Equal([0, 0, 0, 1, 2, 3, 4], log.Order(key));
        Assert.Equal(3, (await app.Client.GetAsync(results[0].Job.Id))!.Attempt);
    }

    [Fact]
    public async Task A_discarded_job_halts_its_sequence_until_retried()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log);
        var key = NewKey();

        var head = await app.Client.InsertAsync(new SeqArgs(key, 0, FailTimes: 1), new InsertOptions { MaxAttempts = 1 });
        var next = await app.Client.InsertAsync(new SeqArgs(key, 1));
        await app.WaitForStateAsync(head.Job.Id, JobState.Discarded);

        await Task.Delay(1000);
        Assert.Equal(JobState.Pending, (await app.Client.GetAsync(next.Job.Id))!.State);

        await app.Client.RetryAsync(head.Job.Id);
        await WaitAllAsync(app, [head.Job.Id, next.Job.Id]);
        Assert.Equal([0, 0, 1], log.Order(key));
    }

    [Fact]
    public async Task Deleting_the_halting_job_lets_the_sequence_continue()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log);
        var key = NewKey();

        var head = await app.Client.InsertAsync(new SeqArgs(key, 0, FailTimes: 1), new InsertOptions { MaxAttempts = 1 });
        var next = await app.Client.InsertAsync(new SeqArgs(key, 1));
        await app.WaitForStateAsync(head.Job.Id, JobState.Discarded);

        await app.Client.DeleteAsync(head.Job.Id);
        await app.WaitForStateAsync(next.Job.Id, JobState.Completed);
    }

    [Fact]
    public async Task A_sequence_set_to_continue_moves_past_a_discarded_job()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log);
        var key = NewKey();
        var options = new InsertOptions { MaxAttempts = 1, SequenceContinueOn = SequenceContinueOn.Discarded };

        var results = await app.Client.InsertManyAsync([
            new InsertManyItem(new SeqArgs(key, 0, FailTimes: 1), options),
            new InsertManyItem(new SeqArgs(key, 1), options),
        ]);

        await app.WaitForStateAsync(results[0].Job.Id, JobState.Discarded);
        await app.WaitForStateAsync(results[1].Job.Id, JobState.Completed);
    }

    [Fact]
    public async Task A_cancelled_running_head_halts_and_survives_the_cleaner()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log, o => o.CancelledJobRetention = TimeSpan.FromMilliseconds(1));
        var key = NewKey();

        var head = await app.Client.InsertAsync(new SeqArgs(key, 0, DelayMs: 60_000));
        var next = await app.Client.InsertAsync(new SeqArgs(key, 1));
        await WaitUntilAsync(() => Task.FromResult(log.Order(key).Length == 1));
        await app.Client.CancelAsync(head.Job.Id);
        await app.WaitForStateAsync(head.Job.Id, JobState.Cancelled);

        // Several cleaner and sweep passes.
        await Task.Delay(1500);
        Assert.Equal(JobState.Cancelled, (await app.Client.GetAsync(head.Job.Id))!.State);
        Assert.Equal(JobState.Pending, (await app.Client.GetAsync(next.Job.Id))!.State);
    }

    [Fact]
    public async Task Cancelling_a_job_that_never_started_takes_it_out_of_the_line()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log);
        var key = NewKey();

        var results = await app.Client.InsertManyAsync(Enumerable.Range(0, 3)
            .Select(n => new InsertManyItem(new SeqArgs(key, n, DelayMs: n == 0 ? 300 : 0))));
        var cancelled = await app.Client.CancelAsync(results[1].Job.Id);
        Assert.Equal(JobState.Cancelled, cancelled!.State);

        await WaitAllAsync(app, [results[0].Job.Id, results[2].Job.Id]);
        Assert.Equal([0, 2], log.Order(key));
    }

    [Fact]
    public async Task A_head_scheduled_in_the_future_holds_the_jobs_behind_it()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log);
        var key = NewKey();
        var at = DateTimeOffset.UtcNow.AddSeconds(1.5);

        var head = await app.Client.InsertAsync(new SeqArgs(key, 0), new InsertOptions { ScheduledAt = at });
        var next = await app.Client.InsertAsync(new SeqArgs(key, 1));

        // Without the sequence, the job due now would run first.
        await WaitAllAsync(app, [head.Job.Id, next.Job.Id]);
        Assert.Equal([0, 1], log.Order(key));

        var laterKey = NewKey();
        var later = await app.Client.InsertAsync(new SeqArgs(laterKey, 0), new InsertOptions { ScheduledAt = DateTimeOffset.UtcNow.AddHours(1) });
        var behind = await app.Client.InsertAsync(new SeqArgs(laterKey, 1));
        await app.WaitForStateAsync(later.Job.Id, JobState.Scheduled);
        await Task.Delay(1000);
        Assert.Equal(JobState.Pending, (await app.Client.GetAsync(behind.Job.Id))!.State);
    }

    [Fact]
    public async Task Promotion_follows_inserts_and_completions_without_waiting_for_the_sweep()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log, o => o.SchedulerInterval = TimeSpan.FromMinutes(1));
        await WaitUntilAsync(() => Task.FromResult(ElectorOf(app).IsLeader));
        var key = NewKey();

        // The leader's first sweep runs on election; from here only notifications and the completer promote.
        await Task.Delay(300);
        var stopwatch = Stopwatch.StartNew();
        await using (var connection = await app.DataSource.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await app.Client.InsertManyAsync(connection, transaction, Enumerable.Range(0, 20)
                .Select(n => new InsertManyItem(new SeqArgs(key, n))));
            await transaction.CommitAsync();
        }

        var last = (await app.Client.ListAsync(new JobListQuery { Descending = true, Limit = 1 })).Jobs[0];
        await app.WaitForStateAsync(last.Id, JobState.Completed, TimeSpan.FromSeconds(10));
        Assert.Equal(Enumerable.Range(0, 20), log.Order(key));
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5), $"20 jobs took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task A_later_transaction_that_commits_first_runs_first_and_they_never_overlap()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log);
        var key = NewKey();

        await using var first = await app.DataSource.OpenConnectionAsync();
        await using var firstTransaction = await first.BeginTransactionAsync();
        var a = await app.Client.InsertAsync(first, firstTransaction, new SeqArgs(key, 0));

        await using (var second = await app.DataSource.OpenConnectionAsync())
        await using (var secondTransaction = await second.BeginTransactionAsync())
        {
            var b = await app.Client.InsertAsync(second, secondTransaction, new SeqArgs(key, 1));
            await secondTransaction.CommitAsync();
            Assert.True(b.Job.Id > a.Job.Id);
            await app.WaitForStateAsync(b.Job.Id, JobState.Completed);
        }

        await firstTransaction.CommitAsync();
        await app.WaitForStateAsync(a.Job.Id, JobState.Completed);
        Assert.Equal([1, 0], log.Order(key));
    }

    [Fact]
    public async Task Concurrent_transactions_keep_their_own_order_and_never_overlap()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var a = await StartWorkerAsync(prefix, log);
        await using var b = await StartWorkerAsync(prefix, log);
        var key = NewKey();

        var ids = await Task.WhenAll(Enumerable.Range(0, 8).Select(async t =>
        {
            await using var connection = await a.DataSource.OpenConnectionAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            var results = await a.Client.InsertManyAsync(connection, transaction, Enumerable.Range(0, 5)
                .Select(n => new InsertManyItem(new SeqArgs(key, (t * 100) + n, DelayMs: 5))));
            await Task.Delay(Random.Shared.Next(0, 200));
            await transaction.CommitAsync();
            return results.Select(r => r.Job.Id).ToArray();
        }));

        await WaitAllAsync(a, ids.SelectMany(i => i));
        var order = log.Order(key);
        Assert.Equal(40, order.Length);
        Assert.Equal(0, log.Overlaps);
        foreach (var t in Enumerable.Range(0, 8))
        {
            Assert.Equal(Enumerable.Range(t * 100, 5), order.Where(n => n / 100 == t));
        }
    }

    [Fact]
    public async Task Bulk_copied_sequence_jobs_keep_their_order()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log);
        var key = NewKey();

        Assert.Equal(25, await app.Client.InsertManyFastAsync(Enumerable.Range(0, 25)
            .Select(n => new InsertManyItem(new SeqArgs(key, n)))));

        await WaitUntilAsync(() => Task.FromResult(log.Order(key).Length == 25));
        Assert.Equal(Enumerable.Range(0, 25), log.Order(key));
        Assert.Equal(0, log.Overlaps);
    }

    [Fact]
    public async Task Rows_carry_their_sequence_key_and_list_filters_by_it()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        var key = NewKey();

        var inserted = await app.Client.InsertAsync(new RecordArgs("a"), new InsertOptions { SequenceKey = key });
        await app.Client.InsertAsync(new RecordArgs("b"));

        Assert.Equal(key, inserted.Job.SequenceKey);
        var page = await app.Client.ListAsync(new JobListQuery { SequenceKey = key });
        Assert.Equal([inserted.Job.Id], page.Jobs.Select(j => j.Id));
        Assert.Equal("seq:{\"key\":\"k\"}", (await app.Client.InsertAsync(new SeqArgs("k", 0))).Job.SequenceKey);
    }

    [Fact]
    public async Task Bulk_retry_returns_a_halted_sequence_to_the_line_and_resumes_it()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log);
        var key = NewKey();

        var head = await app.Client.InsertAsync(new SeqArgs(key, 0, FailTimes: 1), new InsertOptions { MaxAttempts = 1 });
        var next = await app.Client.InsertAsync(new SeqArgs(key, 1));
        await app.WaitForStateAsync(head.Job.Id, JobState.Discarded);

        // Both match: the halted head goes back to pending and runs first, the pending one is left in line.
        var sequenceKey = head.Job.SequenceKey!;
        var result = await app.Client.RetryManyAsync(new JobFilter { SequenceKey = sequenceKey });
        Assert.Equal(2, result.Retried);

        await WaitAllAsync(app, [head.Job.Id, next.Job.Id]);
        Assert.Equal([0, 0, 1], log.Order(key));
        Assert.Equal(0, log.Overlaps);
    }

    [Fact]
    public async Task Bulk_delete_of_a_halting_job_and_bulk_cancel_of_a_head_promote_the_next_job()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        await using var app = await StartWorkerAsync(prefix, log, o => o.SchedulerInterval = TimeSpan.FromMinutes(1));
        var (halted, waiting) = (NewKey(), NewKey());

        var head = await app.Client.InsertAsync(new SeqArgs(halted, 0, FailTimes: 1), new InsertOptions { MaxAttempts = 1 });
        var afterHalt = await app.Client.InsertAsync(new SeqArgs(halted, 1));
        await app.WaitForStateAsync(head.Job.Id, JobState.Discarded);
        Assert.Equal(1, await app.Client.DeleteManyAsync(new JobFilter { SequenceKey = head.Job.SequenceKey, States = [JobState.Discarded] }));
        await app.WaitForStateAsync(afterHalt.Job.Id, JobState.Completed, TimeSpan.FromSeconds(5));

        // A head scheduled far ahead holds its sequence; cancelling it in bulk (it never ran) frees the next job.
        var later = await app.Client.InsertAsync(new SeqArgs(waiting, 0), new InsertOptions { ScheduledAt = DateTimeOffset.UtcNow.AddHours(1) });
        var behind = await app.Client.InsertAsync(new SeqArgs(waiting, 1));
        await app.WaitForStateAsync(later.Job.Id, JobState.Scheduled, TimeSpan.FromSeconds(5));
        var cancelled = await app.Client.CancelManyAsync(new JobFilter { SequenceKey = later.Job.SequenceKey, States = [JobState.Scheduled] });
        Assert.Equal(1, cancelled.Cancelled);
        await app.WaitForStateAsync(behind.Job.Id, JobState.Completed, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task A_unique_duplicate_of_a_pending_job_is_skipped()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        var unique = new InsertOptions { SequenceKey = NewKey(), Unique = new UniqueOptions { ByArgs = true } };

        var first = await app.Client.InsertAsync(new RecordArgs("same"), unique);
        var second = await app.Client.InsertAsync(new RecordArgs("same"), unique);

        Assert.Equal(JobState.Pending, first.Job.State);
        Assert.True(second.UniqueSkippedAsDuplicate);
        Assert.Equal(first.Job.Id, second.Job.Id);
    }

    [Fact]
    public async Task An_explicit_key_overrides_the_attribute_and_can_span_kinds()
    {
        var prefix = await database.CreateSchemaAsync();
        var log = new SequenceLog();
        var recorder = new JobRecorder();
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(), b =>
        {
            b.AddWorker<SeqArgs, SeqWorker>();
            b.AddWorker<RecordArgs, RecordWorker>();
            b.Services.AddSingleton(log);
        }, recorder);
        var shared = new InsertOptions { SequenceKey = NewKey() };

        var slowKey = NewKey();
        var slow = await app.Client.InsertAsync(new SeqArgs(slowKey, 0, DelayMs: 1000), shared);
        var record = await app.Client.InsertAsync(new RecordArgs("after"), shared);

        await WaitUntilAsync(() => Task.FromResult(log.Order(slowKey).Length == 1));
        Assert.Equal(JobState.Pending, (await app.Client.GetAsync(record.Job.Id))!.State);
        await app.WaitForStateAsync(record.Job.Id, JobState.Completed);
        Assert.Equal(JobState.Completed, (await app.Client.GetAsync(slow.Job.Id))!.State);
    }

    [Fact]
    public async Task A_stuck_head_the_rescuer_discards_halts_the_sequence_unless_set_to_continue()
    {
        var prefix = await database.CreateSchemaAsync();
        var (halting, continuing) = (NewKey(), NewKey());

        // Heads stuck running on a node that died, out of attempts, each with one job behind it.
        await database.ExecuteAsOwnerAsync($"""
            INSERT INTO public.{prefix}job (args, kind, state, attempt, max_attempts, attempted_at, sequence_key, sequence_continue_on)
            VALUES
                ('{"{}"}', 'record', 'running', 1, 1, now() - interval '1 hour', '{halting}', NULL),
                ('{"{"}"value": "behind"{"}"}', 'record', 'pending', 0, 5, NULL, '{halting}', NULL),
                ('{"{}"}', 'record', 'running', 1, 1, now() - interval '1 hour', '{continuing}', 2),
                ('{"{"}"value": "behind"{"}"}', 'record', 'pending', 0, 5, NULL, '{continuing}', 2)
            """);

        await using var app = await TestApp.StartAsync(database, prefix, o =>
        {
            o.Queues["default"] = new QueueOptions();
            o.RescueStuckJobsAfter = TimeSpan.FromSeconds(1);
        }, b => b.AddWorker<RecordArgs, RecordWorker>());

        var jobs = (await app.Client.ListAsync(new JobListQuery())).Jobs;
        await app.WaitForStateAsync(jobs[3].Id, JobState.Completed);
        Assert.Equal(JobState.Discarded, (await app.Client.GetAsync(jobs[0].Id))!.State);
        Assert.Equal(JobState.Pending, (await app.Client.GetAsync(jobs[1].Id))!.State);
    }
}

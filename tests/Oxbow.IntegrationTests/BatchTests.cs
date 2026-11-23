using Microsoft.Extensions.Logging.Abstractions;
using Oxbow.Internal;
using Oxbow.Processing;

namespace Oxbow.IntegrationTests;

[JobKind("batch_done")]
public sealed record BatchDoneArgs(string Name) : IJobArgs;

public sealed class BatchDoneWorker(JobRecorder recorder, IOxbowClient client) : IWorker<BatchDoneArgs>
{
    public async Task WorkAsync(Job<BatchDoneArgs> job, CancellationToken cancellationToken)
    {
        var batch = await client.GetBatchAsync(job.BatchId!.Value, cancellationToken);
        recorder.Events.Enqueue($"{job.Args.Name}:{job.BatchId}:{batch?.Completed}/{batch?.Total}");
        recorder.Started(job.Id);
    }
}

public sealed class BatchTests(PostgresFixture database)
{
    private static readonly BatchOptions BothCallbacks = new()
    {
        OnComplete = new BatchCallback(new BatchDoneArgs("complete")),
        OnSuccess = new BatchCallback(new BatchDoneArgs("success")),
    };

    private static void Register(IOxbowBuilder b) => b
        .AddWorker<RecordArgs, RecordWorker>()
        .AddWorker<FailArgs, FailWorker>()
        .AddWorker<UniqueArgs, UniqueWorker>()
        .AddWorker<BatchDoneArgs, BatchDoneWorker>();

    private static void WorkDefault(OxbowOptions o) => o.Queues["default"] = new QueueOptions();

    [Fact]
    public async Task On_complete_and_on_success_fire_once_when_every_job_succeeds()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, WorkDefault, Register);

        var inserted = await app.Client.InsertBatchAsync(
            Enumerable.Range(0, 10).Select(i => new InsertManyItem(new RecordArgs($"{i}"))), BothCallbacks);
        Assert.All(inserted.Jobs, r => Assert.Equal(inserted.BatchId, r.Job.BatchId));

        var batch = await WaitForBatchAsync(app, inserted.BatchId, b => b.FinalizedAt is not null);
        Assert.Equal((10, 10, 0, 0, 0), (batch.Total, batch.Completed, batch.Discarded, batch.Cancelled, batch.Remaining));

        var complete = await app.WaitForStateAsync(batch.OnCompleteJobId!.Value, JobState.Completed);
        var success = await app.WaitForStateAsync(batch.OnSuccessJobId!.Value, JobState.Completed);
        Assert.Equal(inserted.BatchId, complete.BatchId);
        Assert.Equal("success", success.Metadata.GetProperty("oxbow:batch_callback").GetString());

        // Several more finisher passes change nothing.
        await Task.Delay(1000);
        Assert.Equal(2, (await CallbacksAsync(app)).Count);
        Assert.Equal(
            [$"complete:{inserted.BatchId}:10/10", $"success:{inserted.BatchId}:10/10"],
            app.Recorder.Events.Where(e => e.StartsWith("complete", StringComparison.Ordinal) || e.StartsWith("success", StringComparison.Ordinal)).Order());
    }

    [Fact]
    public async Task On_complete_fires_with_failures_and_on_success_does_not()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, WorkDefault, Register);

        var inserted = await app.Client.InsertBatchAsync(
        [
            new InsertManyItem(new RecordArgs("a")),
            new InsertManyItem(new RecordArgs("b")),
            new InsertManyItem(new FailArgs("boom"), new InsertOptions { MaxAttempts = 1 }),
        ], BothCallbacks);

        var batch = await WaitForBatchAsync(app, inserted.BatchId, b => b.FinalizedAt is not null);
        Assert.Equal((3, 2, 1, 0), (batch.Total, batch.Completed, batch.Discarded, batch.Cancelled));
        Assert.Null(batch.OnSuccessJobId);
        await app.WaitForStateAsync(batch.OnCompleteJobId!.Value, JobState.Completed);

        await Task.Delay(1000);
        var callbacks = await CallbacksAsync(app);
        Assert.Equal(batch.OnCompleteJobId, Assert.Single(callbacks).Id);
    }

    [Fact]
    public async Task Overlapping_finishers_enqueue_each_callback_exactly_once()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var a = await TestApp.StartAsync(database, prefix, register: Register, start: false);
        await using var b = await TestApp.StartAsync(database, prefix, register: Register, start: false);

        var ids = new List<long>();
        for (var i = 0; i < 30; i++)
        {
            ids.Add((await a.Client.InsertBatchAsync(
                Enumerable.Range(0, 3).Select(j => new InsertManyItem(new RecordArgs($"{i}.{j}"))), BothCallbacks)).BatchId);
        }

        await database.ExecuteAsOwnerAsync(
            $"UPDATE public.{prefix}job SET state = 'completed', finalized_at = now() WHERE batch_id IS NOT NULL");

        // Leaders never overlap by design; this is the guard for when they do anyway (a paused process, clock trouble).
        var finishers = new[] { MaintenanceOf(a), MaintenanceOf(b) };
        await Task.WhenAll(Enumerable.Range(0, 8).Select(i => Task.Run(() => finishers[i % 2].FinishBatchesAsync(CancellationToken.None))));

        var callbacks = await CallbacksAsync(a);
        Assert.Equal(60, callbacks.Count);
        Assert.All(
            callbacks.GroupBy(j => (j.BatchId, j.Metadata.GetProperty("oxbow:batch_callback").GetString())),
            g => Assert.Single(g));
        foreach (var id in ids)
        {
            var batch = await a.Client.GetBatchAsync(id);
            Assert.NotNull(batch!.FinalizedAt);
            Assert.Equal(3, batch.Completed);
        }
    }

    [Fact]
    public async Task Callbacks_are_enqueued_exactly_once_across_a_leader_failover()
    {
        var prefix = await database.CreateSchemaAsync();
        var a = await TestApp.StartAsync(database, prefix, WorkDefault, Register);
        var b = await TestApp.StartAsync(database, prefix, WorkDefault, Register);
        TestApp? stopped = null;
        try
        {
            await WaitUntilAsync(() => ElectorOf(a).IsLeader || ElectorOf(b).IsLeader);
            var leader = ElectorOf(a).IsLeader ? a : b;
            var survivor = leader == a ? b : a;

            // Members come due over a few seconds, so batches finish before, during and after the handover.
            var now = DateTimeOffset.UtcNow;
            var ids = new List<long>();
            for (var i = 0; i < 20; i++)
            {
                var due = now.AddMilliseconds(150 * i);
                ids.Add((await survivor.Client.InsertBatchAsync(
                    Enumerable.Range(0, 2).Select(j => new InsertManyItem(new RecordArgs($"{i}.{j}"), new InsertOptions { ScheduledAt = due })),
                    BothCallbacks)).BatchId);
            }

            await Task.Delay(1200);
            stopped = leader;
            await leader.DisposeAsync();
            await WaitUntilAsync(() => ElectorOf(survivor).IsLeader);

            foreach (var id in ids)
            {
                await WaitForBatchAsync(survivor, id, x => x.FinalizedAt is not null, TimeSpan.FromSeconds(20));
            }

            await Task.Delay(1000);
            var callbacks = await CallbacksAsync(survivor);
            Assert.Equal(40, callbacks.Count);
            Assert.All(
                callbacks.GroupBy(j => (j.BatchId, j.Metadata.GetProperty("oxbow:batch_callback").GetString())),
                g => Assert.Single(g));
        }
        finally
        {
            foreach (var app in new[] { a, b }.Where(x => x != stopped))
            {
                await app.DisposeAsync();
            }
        }
    }

    [Fact]
    public async Task A_rolled_back_transaction_leaves_no_batch()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, register: Register, start: false);

        await using var connection = await app.DataSource.OpenConnectionAsync();
        BatchInsertResult inserted;
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            inserted = await app.Client.InsertBatchAsync(connection, transaction, [new InsertManyItem(new RecordArgs("x"))], BothCallbacks);
            await transaction.RollbackAsync();
        }

        Assert.Null(await app.Client.GetBatchAsync(inserted.BatchId));
        Assert.Null(await app.Client.GetAsync(inserted.Jobs[0].Job.Id));
    }

    [Fact]
    public async Task Progress_counts_members_past_their_retention_until_the_batch_is_cleaned()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o =>
        {
            WorkDefault(o);
            o.CompletedJobRetention = TimeSpan.FromMinutes(1);
            o.BatchRetention = TimeSpan.FromMinutes(1);
        }, Register);

        var inserted = await app.Client.InsertBatchAsync(
        [
            new InsertManyItem(new RecordArgs("a")),
            new InsertManyItem(new RecordArgs("b")),
            new InsertManyItem(new RecordArgs("later"), new InsertOptions { ScheduledAt = DateTimeOffset.UtcNow.AddHours(1) }),
        ], new BatchOptions { OnComplete = new BatchCallback(new BatchDoneArgs("complete")) });
        var (first, second, later) = (inserted.Jobs[0].Job.Id, inserted.Jobs[1].Job.Id, inserted.Jobs[2].Job.Id);
        await app.WaitForStateAsync(first, JobState.Completed);
        await app.WaitForStateAsync(second, JobState.Completed);

        // Age the finished members past retention, next to a job outside any batch to show the cleaner ran.
        var control = (await app.Client.InsertAsync(new RecordArgs("control"))).Job.Id;
        await app.WaitForStateAsync(control, JobState.Completed);
        await database.ExecuteAsOwnerAsync(
            $"UPDATE public.{prefix}job SET finalized_at = now() - interval '2 minutes' WHERE id IN ({first}, {second}, {control})");
        await WaitUntilAsync(async () => await app.Client.GetAsync(control) is null);

        Assert.NotNull(await app.Client.GetAsync(first));
        var batch = await app.Client.GetBatchAsync(inserted.BatchId);
        Assert.Equal((3, 2, 0, 0, 1), (batch!.Total, batch.Completed, batch.Discarded, batch.Cancelled, batch.Remaining));
        Assert.Null(batch.FinalizedAt);
        Assert.Equal([first, second, later], (await app.Client.ListAsync(new JobListQuery { BatchId = inserted.BatchId })).Jobs.Select(j => j.Id));

        await app.Client.RetryAsync(later);
        batch = await WaitForBatchAsync(app, inserted.BatchId, x => x.FinalizedAt is not null);
        Assert.Equal((3, 3, 0), (batch.Total, batch.Completed, batch.Remaining));
        await app.WaitForStateAsync(batch.OnCompleteJobId!.Value, JobState.Completed);

        // Past the batch's retention, with its callback done, the batch goes and then its members.
        await database.ExecuteAsOwnerAsync($"UPDATE public.{prefix}batch SET finalized_at = now() - interval '2 minutes'");
        await database.ExecuteAsOwnerAsync($"UPDATE public.{prefix}job SET finalized_at = now() - interval '2 minutes' WHERE id = {later}");
        await WaitUntilAsync(async () => await app.Client.GetBatchAsync(inserted.BatchId) is null);
        await WaitUntilAsync(async () => await app.Client.GetAsync(first) is null && await app.Client.GetAsync(later) is null);
    }

    [Fact]
    public async Task A_batch_is_kept_past_retention_while_its_callback_is_unfinished()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o =>
        {
            WorkDefault(o);
            o.BatchRetention = TimeSpan.FromMinutes(1);
        }, Register);

        var parked = await app.Client.InsertBatchAsync([], new BatchOptions
        {
            OnComplete = new BatchCallback(new BatchDoneArgs("parked"), new InsertOptions { Queue = "nobody" }),
        });
        var done = await app.Client.InsertBatchAsync([], new BatchOptions
        {
            OnComplete = new BatchCallback(new BatchDoneArgs("done")),
        });
        var finished = await WaitForBatchAsync(app, done.BatchId, b => b.FinalizedAt is not null);
        await app.WaitForStateAsync(finished.OnCompleteJobId!.Value, JobState.Completed);
        await WaitForBatchAsync(app, parked.BatchId, b => b.FinalizedAt is not null);

        await database.ExecuteAsOwnerAsync($"UPDATE public.{prefix}batch SET finalized_at = now() - interval '2 minutes'");
        await WaitUntilAsync(async () => await app.Client.GetBatchAsync(done.BatchId) is null);
        Assert.NotNull(await app.Client.GetBatchAsync(parked.BatchId));
    }

    [Fact]
    public async Task An_empty_batch_finishes_with_both_callbacks()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, WorkDefault, Register);

        var inserted = await app.Client.InsertBatchAsync([], BothCallbacks);
        Assert.Empty(inserted.Jobs);

        var batch = await WaitForBatchAsync(app, inserted.BatchId, b => b.FinalizedAt is not null);
        Assert.Equal(0, batch.Total);
        await app.WaitForStateAsync(batch.OnCompleteJobId!.Value, JobState.Completed);
        await app.WaitForStateAsync(batch.OnSuccessJobId!.Value, JobState.Completed);
    }

    [Fact]
    public async Task A_unique_duplicate_is_not_a_member()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, WorkDefault, Register);

        var existing = await app.Client.InsertAsync(new UniqueArgs("c1", "first"), new InsertOptions { Queue = "nobody" });
        var inserted = await app.Client.InsertBatchAsync(
            [new InsertManyItem(new UniqueArgs("c1", "second")), new InsertManyItem(new RecordArgs("x"))], BothCallbacks);

        Assert.True(inserted.Jobs[0].UniqueSkippedAsDuplicate);
        Assert.Equal(existing.Job.Id, inserted.Jobs[0].Job.Id);
        Assert.Null(inserted.Jobs[0].Job.BatchId);

        var batch = await WaitForBatchAsync(app, inserted.BatchId, b => b.FinalizedAt is not null);
        Assert.Equal((1, 1), (batch.Total, batch.Completed));
        Assert.NotNull(batch.OnSuccessJobId);
        Assert.Equal(JobState.Available, (await app.Client.GetAsync(existing.Job.Id))!.State);
    }

    [Fact]
    public async Task Cancelled_and_deleted_members_finish_the_batch_without_success()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, WorkDefault, Register);

        var parkedInQueue = new InsertOptions { Queue = "nobody" };
        var inserted = await app.Client.InsertBatchAsync(
        [
            new InsertManyItem(new RecordArgs("done")),
            new InsertManyItem(new RecordArgs("cancel"), parkedInQueue),
            new InsertManyItem(new RecordArgs("delete"), parkedInQueue),
        ], BothCallbacks);
        await app.WaitForStateAsync(inserted.Jobs[0].Job.Id, JobState.Completed);

        await app.Client.CancelAsync(inserted.Jobs[1].Job.Id);
        var progress = await app.Client.GetBatchAsync(inserted.BatchId);
        Assert.Equal((1, 1, 1), (progress!.Completed, progress.Cancelled, progress.Remaining));

        await app.Client.DeleteAsync(inserted.Jobs[2].Job.Id);
        var batch = await WaitForBatchAsync(app, inserted.BatchId, b => b.FinalizedAt is not null);
        Assert.Equal((3, 1, 0, 2, 0), (batch.Total, batch.Completed, batch.Discarded, batch.Cancelled, batch.Remaining));
        Assert.NotNull(batch.OnCompleteJobId);
        Assert.Null(batch.OnSuccessJobId);
    }

    [Fact]
    public async Task Callbacks_with_the_same_unique_key_are_rejected()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, register: Register, start: false);

        var callback = new BatchCallback(new UniqueArgs("c1", "x"));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            app.Client.InsertBatchAsync([], new BatchOptions { OnComplete = callback, OnSuccess = callback }));
    }

    [Fact]
    public async Task Insert_middleware_can_drop_a_callback_but_not_detach_members()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, WorkDefault, b =>
        {
            Register(b);
            b.AddInsertMiddleware<DropSuccessAndClearMetadata>();
        });

        var inserted = await app.Client.InsertBatchAsync([new InsertManyItem(new RecordArgs("x"))], BothCallbacks);
        Assert.Equal(inserted.BatchId, inserted.Jobs[0].Job.BatchId);

        var batch = await WaitForBatchAsync(app, inserted.BatchId, b => b.FinalizedAt is not null);
        Assert.Equal(1, batch.Completed);
        Assert.NotNull(batch.OnCompleteJobId);
        Assert.Null(batch.OnSuccessJobId);
    }

    [Fact]
    public async Task A_callback_middleware_made_invalid_is_rejected_at_insert()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, register: b =>
        {
            Register(b);
            b.AddInsertMiddleware<BreakCallbackPriority>();
        }, start: false);

        await Assert.ThrowsAsync<InvalidOperationException>(() => app.Client.InsertBatchAsync([], BothCallbacks));
    }

    private static bool IsCallback(JobInsertParams job) => job.Metadata.ContainsKey("oxbow:batch_callback");

    private sealed class DropSuccessAndClearMetadata : IJobInsertMiddleware
    {
        public Task<IReadOnlyList<InsertResult>> InvokeAsync(JobInsertContext context, JobInsertDelegate next)
        {
            if (context.Jobs.Any(j => IsCallback(j) && j.Metadata["oxbow:batch_callback"]!.GetValue<string>() == "success"))
            {
                return Task.FromResult<IReadOnlyList<InsertResult>>([]);
            }

            foreach (var job in context.Jobs.Where(j => !IsCallback(j)))
            {
                job.Metadata.Remove("oxbow:batch_id");
            }

            return next(context);
        }
    }

    private sealed class BreakCallbackPriority : IJobInsertMiddleware
    {
        public Task<IReadOnlyList<InsertResult>> InvokeAsync(JobInsertContext context, JobInsertDelegate next)
        {
            foreach (var job in context.Jobs.Where(IsCallback))
            {
                job.Priority = 5;
            }

            return next(context);
        }
    }

    private static Elector ElectorOf(TestApp app) =>
        app.Host.Services.GetServices<Microsoft.Extensions.Hosting.IHostedService>().OfType<OxbowHostedService>().Single().Elector!;

    private static Maintenance MaintenanceOf(TestApp app)
    {
        var services = app.Host.Services;
        return new Maintenance(
            services.GetRequiredService<OxbowRuntime>(),
            services.GetRequiredService<WorkerRegistry>(),
            services.GetRequiredService<OxbowClient>(),
            services.GetRequiredService<IServiceScopeFactory>(),
            services.GetRequiredService<IRetryPolicy>(),
            TimeProvider.System,
            _ => { },
            NullLogger.Instance);
    }

    private static async Task<List<JobRow>> CallbacksAsync(TestApp app) =>
        [.. (await app.Client.ListAsync(new JobListQuery { Kinds = ["batch_done"], Limit = 1000 })).Jobs];

    private static async Task<Batch> WaitForBatchAsync(TestApp app, long id, Func<Batch, bool> predicate, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(15));
        Batch? last = null;
        while (!cts.IsCancellationRequested)
        {
            last = await app.Client.GetBatchAsync(id);
            if (last is not null && predicate(last))
            {
                return last;
            }

            await Task.Delay(50);
        }

        throw new TimeoutException($"Batch {id} never matched; last finalized {last?.FinalizedAt}, remaining {last?.Remaining}.");
    }

    private static Task WaitUntilAsync(Func<bool> condition) => WaitUntilAsync(() => Task.FromResult(condition()));

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!await condition())
        {
            await Task.Delay(50, cts.Token);
        }
    }
}

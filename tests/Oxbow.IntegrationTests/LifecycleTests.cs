using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Oxbow.Internal;
using Oxbow.Processing;

namespace Oxbow.IntegrationTests;

public sealed class LifecycleTests(PostgresFixture database)
{
    [Fact]
    public async Task Shutdown_returns_in_flight_jobs_to_available_without_spending_the_attempt()
    {
        var prefix = await database.CreateSchemaAsync();
        var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(),
            b => b.AddWorker<BlockArgs, BlockWorker>(), shutdownTimeout: TimeSpan.FromMilliseconds(500));

        var result = await app.Client.InsertAsync(new BlockArgs());
        await app.Recorder.WaitStartedAsync(result.Job.Id);
        await app.DisposeAsync();

        await using var reader = await TestApp.StartAsync(database, prefix);
        var job = await reader.Client.GetAsync(result.Job.Id);
        Assert.Equal(JobState.Available, job!.State);
        Assert.Equal(0, job.Attempt);
        Assert.Empty(job.Errors);
    }

    [Fact]
    public async Task A_paused_queue_starts_nothing_until_resumed()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(),
            b => b.AddWorker<RecordArgs, RecordWorker>());

        await app.Client.PauseQueueAsync("default");
        await Task.Delay(300);
        var result = await app.Client.InsertAsync(new RecordArgs("paused"));
        await Task.Delay(1000);
        Assert.Equal(JobState.Available, (await app.Client.GetAsync(result.Job.Id))!.State);

        await app.Client.ResumeQueueAsync("default");
        await app.WaitForStateAsync(result.Job.Id, JobState.Completed);
    }

    [Fact]
    public async Task A_pause_sent_while_the_listener_is_down_holds_once_it_reconnects()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(),
            b => b.AddWorker<RecordArgs, RecordWorker>());

        // A job running proves the producer is past its startup read, so only a reconnect can catch the pause.
        var ready = await app.Client.InsertAsync(new RecordArgs("ready"));
        await app.WaitForStateAsync(ready.Job.Id, JobState.Completed);

        var listener = await WaitForListenerAsync(app);
        await using (var kill = app.DataSource.CreateCommand("SELECT pg_terminate_backend(@pid)"))
        {
            kill.Parameters.AddWithValue("pid", listener);
            await kill.ExecuteNonQueryAsync();
        }

        // The listener reconnects after a second's backoff. No listener right after the pause commits means none
        // heard it.
        await WaitAsync(async () => await FindListenerAsync(app) is null);
        await app.Client.PauseQueueAsync("default");
        Assert.Null(await FindListenerAsync(app));

        await WaitForListenerAsync(app);
        await Task.Delay(300);

        var result = await app.Client.InsertAsync(new RecordArgs("paused"));
        await Task.Delay(1000);
        Assert.Equal(JobState.Available, (await app.Client.GetAsync(result.Job.Id))!.State);

        await app.Client.ResumeQueueAsync("default");
        await app.WaitForStateAsync(result.Job.Id, JobState.Completed);
    }

    private static async Task<int> WaitForListenerAsync(TestApp app)
    {
        int? pid = null;
        await WaitAsync(async () => (pid = await FindListenerAsync(app)) is not null);
        return pid!.Value;
    }

    private static async Task WaitAsync(Func<Task<bool>> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!await condition())
        {
            await Task.Delay(20, cts.Token);
        }
    }

    // The notifier's connection, once its LISTENs have all run: an idle backend whose last query is one of them.
    private static async Task<int?> FindListenerAsync(TestApp app)
    {
        var sql = app.Host.Services.GetRequiredService<OxbowRuntime>().Sql;
        string[] listens = [.. new[] { sql.InsertChannel, sql.ControlChannel, sql.LeadershipChannel, sql.SequenceChannel }
            .Select(channel => $"LISTEN {Identifiers.Quote(channel)}")];
        await using var command = app.DataSource.CreateCommand(
            "SELECT pid FROM pg_stat_activity WHERE state = 'idle' AND query = ANY(@listens)");
        command.Parameters.AddWithValue("listens", listens);
        return await command.ExecuteScalarAsync() as int?;
    }

    [Fact]
    public async Task Retry_delete_and_list_follow_river_semantics()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        var results = await app.Client.InsertManyAsync(Enumerable.Range(0, 5).Select(i => new InsertManyItem(new RecordArgs($"{i}"))));
        var cancelled = await app.Client.CancelAsync(results[0].Job.Id);
        Assert.Equal(JobState.Cancelled, cancelled!.State);

        var retried = await app.Client.RetryAsync(results[0].Job.Id);
        Assert.Equal(JobState.Available, retried!.State);
        Assert.Null(retried.FinalizedAt);

        var deleted = await app.Client.DeleteAsync(results[1].Job.Id);
        Assert.NotNull(deleted);
        Assert.Null(await app.Client.GetAsync(results[1].Job.Id));

        var page1 = await app.Client.ListAsync(new JobListQuery { Limit = 2 });
        var page2 = await app.Client.ListAsync(new JobListQuery { Limit = 2, Cursor = page1.NextCursor });
        var page3 = await app.Client.ListAsync(new JobListQuery { Limit = 2, Cursor = page2.NextCursor });
        Assert.Equal(
            results.Where((_, i) => i != 1).Select(r => r.Job.Id),
            page1.Jobs.Concat(page2.Jobs).Concat(page3.Jobs).Select(j => j.Id));
        Assert.Null(page3.NextCursor);
    }

    [Fact]
    public async Task Insert_many_returns_rows_in_order_inside_the_callers_transaction()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        await using var connection = await app.DataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        var results = await app.Client.InsertManyAsync(connection, transaction, Enumerable.Range(0, 100)
            .Select(i => new InsertManyItem(new RecordArgs($"{i}"), new InsertOptions { Priority = (i % 4) + 1, Tags = ["bulk-insert"] })));
        await transaction.RollbackAsync();

        Assert.Equal(Enumerable.Range(0, 100).Select(i => $"{i}"), results.Select(r => r.Job.Args.GetProperty("value").GetString()));
        Assert.All(results, r => Assert.Equal(["bulk-insert"], r.Job.Tags));
        Assert.Null(await app.Client.GetAsync(results[0].Job.Id));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Stopping_after_dispose_does_not_throw(bool stopHostFirst)
    {
        var prefix = await database.CreateSchemaAsync();
        var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions());
        var service = app.Host.Services.GetServices<IHostedService>().OfType<OxbowHostedService>().Single();

        if (stopHostFirst)
        {
            await app.Host.StopAsync();
        }

        app.Host.Dispose();
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Overlapping_stops_share_one_shutdown()
    {
        var prefix = await database.CreateSchemaAsync();
        var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions());
        var service = app.Host.Services.GetServices<IHostedService>().OfType<OxbowHostedService>().Single();

        var first = service.StopAsync(CancellationToken.None);
        Assert.Same(first, service.StopAsync(CancellationToken.None));
        await first;
        await app.DisposeAsync();
    }
}

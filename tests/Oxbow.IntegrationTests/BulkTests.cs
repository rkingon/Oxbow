using System.Text.Json.Nodes;

namespace Oxbow.IntegrationTests;

public sealed class BulkTests(PostgresFixture database)
{
    private static InsertOptions Idle(string tenant, params string[] tags) =>
        new() { Queue = "idle", Metadata = new JsonObject { ["tenant"] = tenant }, Tags = tags };

    [Fact]
    public async Task Cancel_many_by_metadata_cancels_waiting_jobs_and_stops_running_ones_on_every_node()
    {
        var prefix = await database.CreateSchemaAsync();
        var recorder = new JobRecorder();
        await using var node1 = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions { MaxWorkers = 3 },
            b => b.AddWorker<BlockArgs, BlockWorker>().AddWorker<RecordArgs, RecordWorker>(), recorder);
        await using var node2 = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions { MaxWorkers = 3 },
            b => b.AddWorker<BlockArgs, BlockWorker>().AddWorker<RecordArgs, RecordWorker>(), recorder);
        var client = node1.Client;

        var running = new List<long>();
        for (var i = 0; i < 5; i++)
        {
            var tenant = i < 4 ? "acme" : "other";
            var result = await client.InsertAsync(new BlockArgs(), new InsertOptions { Metadata = new JsonObject { ["tenant"] = tenant } });
            await recorder.WaitStartedAsync(result.Job.Id);
            running.Add(result.Job.Id);
        }

        var available = (await client.InsertAsync(new RecordArgs("a"), Idle("acme"))).Job.Id;
        var scheduled = (await client.InsertAsync(new RecordArgs("s"), Idle("acme") with { ScheduledAt = DateTimeOffset.UtcNow.AddHours(1) })).Job.Id;
        var retryable = (await client.InsertAsync(new RecordArgs("r"), Idle("acme"))).Job.Id;
        var completed = (await client.InsertAsync(new RecordArgs("c"), Idle("acme"))).Job.Id;
        var otherTenant = (await client.InsertAsync(new RecordArgs("o"), Idle("other"))).Job.Id;
        await SetStateAsync(prefix, retryable, "retryable", "scheduled_at = now() + interval '1 hour'");
        await SetStateAsync(prefix, completed, "completed", "finalized_at = now()");

        var cancel = await client.CancelManyAsync(new JobFilter { Metadata = new JsonObject { ["tenant"] = "acme" } });

        Assert.Equal(new CancelManyResult(Cancelled: 3, CancelRequested: 4), cancel);
        foreach (var id in new[] { available, scheduled, retryable })
        {
            var job = await client.GetAsync(id);
            Assert.Equal(JobState.Cancelled, job!.State);
            Assert.NotNull(job.FinalizedAt);
        }

        // The cancel poll runs every minute, so stopping within seconds means the NOTIFY reached both nodes.
        var stopped = new List<JobRow>();
        foreach (var id in running.Take(4))
        {
            stopped.Add(await node1.WaitForStateAsync(id, JobState.Cancelled, TimeSpan.FromSeconds(5)));
        }

        Assert.Equal(2, stopped.Select(j => j.AttemptedBy[^1]).Distinct().Count());
        Assert.Equal(JobState.Completed, (await client.GetAsync(completed))!.State);
        Assert.Equal(JobState.Available, (await client.GetAsync(otherTenant))!.State);
        await Task.Delay(500);
        Assert.Equal(JobState.Running, (await client.GetAsync(running[4]))!.State);
    }

    [Fact]
    public async Task Cancel_many_stops_more_running_jobs_than_fit_in_one_notification()
    {
        var prefix = await database.CreateSchemaAsync();
        var queue = new string('q', 127);
        await database.ExecuteAsOwnerAsync($"ALTER SEQUENCE {prefix}job_id_seq RESTART WITH 1000000000000000000");
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues[queue] = new QueueOptions { MaxWorkers = 700 },
            b => b.AddWorker<BlockArgs, BlockWorker>().AddWorker<RecordArgs, RecordWorker>());

        var results = await app.Client.InsertManyAsync(Enumerable.Range(0, 650)
            .Select(_ => new InsertManyItem(new BlockArgs(), new InsertOptions { Queue = queue })));
        foreach (var result in results)
        {
            await app.Recorder.WaitStartedAsync(result.Job.Id);
        }

        var cancel = await app.Client.CancelManyAsync(new JobFilter { Queues = [queue] });
        Assert.Equal(650, cancel.CancelRequested);

        foreach (var result in results)
        {
            await app.WaitForStateAsync(result.Job.Id, JobState.Cancelled, TimeSpan.FromSeconds(5));
        }
    }

    [Fact]
    public async Task Retry_many_matches_retry_and_leaves_running_jobs_alone()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(),
            b => b.AddWorker<BlockArgs, BlockWorker>().AddWorker<RecordArgs, RecordWorker>());

        var running = (await app.Client.InsertAsync(new BlockArgs(), new InsertOptions { Tags = ["batch"] })).Job.Id;
        await app.Recorder.WaitStartedAsync(running);
        await database.ExecuteAsOwnerAsync(
            $"UPDATE {prefix}job SET metadata = metadata || '{{\"cancel_attempted_at\": \"2026-01-01T00:00:00Z\"}}' WHERE id = {running}");

        var discarded = (await app.Client.InsertAsync(new RecordArgs("d"), Idle("acme", "batch") with { MaxAttempts = 3 })).Job.Id;
        var cancelled = (await app.Client.InsertAsync(new RecordArgs("c"), Idle("acme", "batch"))).Job.Id;
        var completed = (await app.Client.InsertAsync(new RecordArgs("k"), Idle("acme", "batch"))).Job.Id;
        var untagged = (await app.Client.InsertAsync(new RecordArgs("u"), Idle("acme"))).Job.Id;
        await SetStateAsync(prefix, discarded, "discarded", "attempt = 3, finalized_at = now()");
        await SetStateAsync(prefix, untagged, "discarded", "finalized_at = now()");
        await app.Client.CancelAsync(cancelled);
        await SetStateAsync(prefix, completed, "completed", "finalized_at = now()");

        var retried = await app.Client.RetryManyAsync(new JobFilter { Tags = ["batch"] });

        Assert.Equal(new RetryManyResult(Retried: 3, SkippedUniqueConflicts: 0), retried);
        var job = await app.Client.GetAsync(discarded);
        Assert.Equal(JobState.Available, job!.State);
        Assert.Equal(4, job.MaxAttempts);
        Assert.Null(job.FinalizedAt);
        job = await app.Client.GetAsync(cancelled);
        Assert.Equal(JobState.Available, job!.State);
        Assert.False(job.Metadata.TryGetProperty("cancel_attempted_at", out _));
        Assert.Equal(JobState.Available, (await app.Client.GetAsync(completed))!.State);
        Assert.Equal(JobState.Discarded, (await app.Client.GetAsync(untagged))!.State);

        job = await app.Client.GetAsync(running);
        Assert.Equal(JobState.Running, job!.State);
        Assert.True(job.Metadata.TryGetProperty("cancel_attempted_at", out _));
    }

    [Fact]
    public async Task Retry_many_skips_jobs_whose_unique_key_is_taken()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        var first = (await app.Client.InsertAsync(new UniqueArgs("c1", "t"), new InsertOptions { Queue = "idle" })).Job.Id;
        await SetStateAsync(prefix, first, "discarded", "finalized_at = now()");
        var second = (await app.Client.InsertAsync(new UniqueArgs("c1", "t"), new InsertOptions { Queue = "idle" })).Job.Id;
        await SetStateAsync(prefix, second, "discarded", "finalized_at = now()");
        var third = (await app.Client.InsertAsync(new UniqueArgs("c2", "t"), new InsertOptions { Queue = "idle" })).Job.Id;
        await SetStateAsync(prefix, third, "discarded", "finalized_at = now()");
        var live = (await app.Client.InsertAsync(new UniqueArgs("c2", "t"), new InsertOptions { Queue = "idle" })).Job.Id;

        Assert.Equal(new RetryManyResult(1, 2), await app.Client.RetryManyAsync(new JobFilter { Kinds = ["unique"], States = [JobState.Discarded] }));

        Assert.Equal(JobState.Available, (await app.Client.GetAsync(first))!.State);
        Assert.Equal(JobState.Discarded, (await app.Client.GetAsync(second))!.State);
        Assert.Equal(JobState.Discarded, (await app.Client.GetAsync(third))!.State);
        Assert.Equal(JobState.Available, (await app.Client.GetAsync(live))!.State);
        Assert.Equal(new RetryManyResult(0, 2), await app.Client.RetryManyAsync(new JobFilter { Kinds = ["unique"], States = [JobState.Discarded] }));
    }

    [Fact]
    public async Task Delete_many_skips_running_jobs()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(),
            b => b.AddWorker<BlockArgs, BlockWorker>().AddWorker<RecordArgs, RecordWorker>());

        var running = (await app.Client.InsertAsync(new BlockArgs(), new InsertOptions { Tags = ["purge"] })).Job.Id;
        await app.Recorder.WaitStartedAsync(running);
        var available = (await app.Client.InsertAsync(new RecordArgs("a"), Idle("acme", "purge"))).Job.Id;
        var completed = (await app.Client.InsertAsync(new RecordArgs("c"), Idle("acme", "purge"))).Job.Id;
        var kept = (await app.Client.InsertAsync(new RecordArgs("k"), Idle("acme"))).Job.Id;
        await SetStateAsync(prefix, completed, "completed", "finalized_at = now()");

        Assert.Equal(2, await app.Client.DeleteManyAsync(new JobFilter { Tags = ["purge"] }));

        Assert.Null(await app.Client.GetAsync(available));
        Assert.Null(await app.Client.GetAsync(completed));
        Assert.Equal(JobState.Running, (await app.Client.GetAsync(running))!.State);
        Assert.NotNull(await app.Client.GetAsync(kept));
    }

    [Fact]
    public async Task Tag_filters_match_jobs_carrying_all_the_tags()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        var both = (await app.Client.InsertAsync(new RecordArgs("ab"), Idle("acme", "red", "blue"))).Job.Id;
        var red = (await app.Client.InsertAsync(new RecordArgs("a"), Idle("acme", "red"))).Job.Id;
        await app.Client.InsertAsync(new RecordArgs("b"), Idle("acme", "blue"));

        Assert.Equal(new[] { both }, Ids(await app.Client.ListAsync(new JobListQuery { Tags = ["red", "blue"] })));
        Assert.Equal(new[] { both, red }, Ids(await app.Client.ListAsync(new JobListQuery { Tags = ["red"] })));
        Assert.Empty((await app.Client.ListAsync(new JobListQuery { Tags = ["red", new string('x', 300)] })).Jobs);
    }

    [Fact]
    public async Task List_filters_by_metadata_and_tags_across_pages()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        var expected = new List<long>();
        for (var i = 0; i < 35; i++)
        {
            var options = new InsertOptions
            {
                Queue = "idle",
                Metadata = new JsonObject { ["tenant"] = i % 3 == 0 ? "other" : "acme", ["customer"] = new JsonObject { ["id"] = i % 2 } },
                Tags = i % 5 == 0 ? ["skip"] : ["keep"],
            };
            var id = (await app.Client.InsertAsync(new RecordArgs($"{i}"), options)).Job.Id;
            if (i % 3 != 0 && i % 2 == 1 && i % 5 != 0)
            {
                expected.Add(id);
            }
        }

        var query = new JobListQuery
        {
            Metadata = new JsonObject { ["tenant"] = "acme", ["customer"] = new JsonObject { ["id"] = 1 } },
            Tags = ["keep"],
            States = [JobState.Available],
            Limit = 4,
        };
        var ascending = await ReadAllAsync(app.Client, query);
        Assert.Equal(expected, ascending);
        Assert.Equal(Enumerable.Reverse(expected), await ReadAllAsync(app.Client, query with { Descending = true }));
    }

    [Fact]
    public async Task Bulk_operations_cover_sets_larger_than_a_batch()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        await app.Client.InsertManyFastAsync(Enumerable.Range(0, 3_500)
            .Select(i => new InsertManyItem(new RecordArgs($"{i}"), Idle(i % 7 == 0 ? "other" : "acme"))));
        var acme = new JobFilter { Metadata = new JsonObject { ["tenant"] = "acme" } };

        Assert.Equal(new CancelManyResult(3_000, 0), await app.Client.CancelManyAsync(acme));
        Assert.Equal(3_000, await CountAsync(prefix, "state = 'cancelled' AND metadata @> '{\"tenant\": \"acme\"}'"));
        Assert.Equal(500, await CountAsync(prefix, "state = 'available' AND metadata @> '{\"tenant\": \"other\"}'"));

        Assert.Equal(new RetryManyResult(3_000, 0), await app.Client.RetryManyAsync(acme));
        Assert.Equal(3_500, await CountAsync(prefix, "state = 'available'"));

        Assert.Equal(3_000, await app.Client.DeleteManyAsync(acme));
        Assert.Equal(500, await CountAsync(prefix, "TRUE"));
    }

    [Fact]
    public async Task Bulk_operations_reject_a_filter_matching_everything_unless_all_is_set()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        await app.Client.InsertAsync(new RecordArgs("a"), Idle("acme"));

        JobFilter[] everything = [new JobFilter(), new JobFilter { Metadata = new JsonObject(), Tags = [] }];
        foreach (var filter in everything)
        {
            await Assert.ThrowsAsync<ArgumentException>(() => app.Client.CancelManyAsync(filter));
            await Assert.ThrowsAsync<ArgumentException>(() => app.Client.RetryManyAsync(filter));
            await Assert.ThrowsAsync<ArgumentException>(() => app.Client.DeleteManyAsync(filter));
        }

        await Assert.ThrowsAsync<ArgumentNullException>(() => app.Client.DeleteManyAsync(null!));
        Assert.Equal(1, await CountAsync(prefix, "TRUE"));

        Assert.Equal(1, await app.Client.DeleteManyAsync(new JobFilter { All = true }));
        Assert.Equal(0, await CountAsync(prefix, "TRUE"));
    }

    [Fact]
    public async Task Bulk_operations_reject_a_list_query_because_they_ignore_its_page()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);
        await app.Client.InsertAsync(new RecordArgs("a"), Idle("acme"));
        await app.Client.InsertAsync(new RecordArgs("b"), Idle("acme"));

        var page = new JobListQuery { Metadata = new JsonObject { ["tenant"] = "acme" }, Limit = 1 };
        Assert.Single((await app.Client.ListAsync(page)).Jobs);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => app.Client.DeleteManyAsync(page));
        Assert.Contains("JobFilter", ex.Message, StringComparison.Ordinal);
        await Assert.ThrowsAsync<ArgumentException>(() => app.Client.CancelManyAsync(page));
        await Assert.ThrowsAsync<ArgumentException>(() => app.Client.RetryManyAsync(page with { All = true }));
        Assert.Equal(2, await CountAsync(prefix, "TRUE"));
    }

    private static long[] Ids(JobListResult page) => page.Jobs.Select(j => j.Id).ToArray();

    private static async Task<List<long>> ReadAllAsync(IOxbowClient client, JobListQuery query)
    {
        var ids = new List<long>();
        for (string? cursor = null; ;)
        {
            var page = await client.ListAsync(query with { Cursor = cursor });
            ids.AddRange(Ids(page));
            if ((cursor = page.NextCursor) is null)
            {
                return ids;
            }
        }
    }

    private Task SetStateAsync(string prefix, long id, string state, string set) =>
        database.ExecuteAsOwnerAsync($"UPDATE {prefix}job SET state = '{state}', {set} WHERE id = {id}");

    private async Task<long> CountAsync(string prefix, string where) =>
        await database.ScalarAsOwnerAsync<long>($"SELECT count(*) FROM {prefix}job WHERE {where}");
}

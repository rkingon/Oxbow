using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Oxbow.Abstractions.Sample;
using Oxbow.Testing;

namespace Oxbow.Tests;

public sealed class AbstractionsTests
{
    [Fact]
    public void Abstractions_reference_only_the_BCL()
    {
        var references = typeof(IJobArgs).Assembly.GetReferencedAssemblies().Select(a => a.Name!).ToList();

        Assert.All(references, name => Assert.True(
            name.StartsWith("System.", StringComparison.Ordinal),
            $"Oxbow.Abstractions references '{name}'."));
    }

    [Fact]
    public void The_sample_application_layer_references_only_the_abstractions()
    {
        var references = typeof(ShipOrder).Assembly.GetReferencedAssemblies().Select(a => a.Name!).ToList();

        Assert.Contains("Oxbow.Abstractions", references);
        Assert.DoesNotContain(references, name => name is "Oxbow" or "Npgsql" || name.StartsWith("Microsoft.Extensions", StringComparison.Ordinal));
    }

    [Fact]
    public void Moved_types_are_forwarded_from_Oxbow()
    {
        var oxbow = typeof(OxbowOptions).Assembly;

        Assert.Same(typeof(IJobArgs), oxbow.GetType("Oxbow.IJobArgs"));
        Assert.Same(typeof(Job<>), oxbow.GetType("Oxbow.Job`1"));
        Assert.Same(typeof(IOxbowClient), oxbow.GetType("Oxbow.IOxbowClient"));
    }

    [Fact]
    public async Task A_job_records_heartbeats_and_progress_on_its_runtime()
    {
        var runtime = new RecordingJobRuntime();
        var job = new Job<ShipOrder>(Row("sample_ship_order"), new ShipOrder(7), runtime);

        await new ShipOrderWorker(new ShippedOrders()).WorkAsync(job, CancellationToken.None);

        Assert.Equal(1, runtime.Heartbeats);
        var progress = Assert.Single(runtime.Progress);
        Assert.Equal((100, "shipped"), (progress.Percent, progress.Message));
        Assert.Equal(100, job.Progress!.Percent);
        Assert.NotNull(job.Output);
    }

    [Fact]
    public async Task The_harness_runs_a_worker_written_against_the_abstractions()
    {
        var services = new ServiceCollection().AddSingleton<ShippedOrders>().BuildServiceProvider();
        var harness = new WorkerHarness<ShipOrder, ShipOrderWorker>(services);

        var result = await harness.WorkAsync(new ShipOrder(3));

        Assert.Equal(WorkOutcome.Completed, result.Outcome);
        Assert.Equal([3L], services.GetRequiredService<ShippedOrders>().Ids);
        Assert.Equal(3, result.Output!.Value.GetProperty("orderId").GetInt64());
    }

    private sealed class StampTenant(string tenant) : IJobInsertMiddleware
    {
        public Task<IReadOnlyList<InsertResult>> InvokeAsync(JobInsertContext context, JobInsertDelegate next)
        {
            foreach (var job in context.Jobs)
            {
                job.Metadata["tenant"] = tenant;
                job.Tags.Add("stamped");
            }

            return next(context);
        }
    }

    [JobKind("defaults")]
    private sealed record WithDefaults(string Value) : IJobArgs
    {
        public InsertOptions GetInsertOptions() => new() { Queue = "slow", Priority = 3 };
    }

    [Fact]
    public async Task The_recording_client_records_kind_args_options_and_middleware_metadata()
    {
        var oxbow = new RecordingOxbowClient([new StampTenant("acme")]);

        var id = await new PlaceOrder(oxbow).ExecuteAsync(42);
        await oxbow.InsertAsync(new WithDefaults("x"), new InsertOptions { Priority = 2 });

        var shipped = Assert.Single(oxbow.Inserted<ShipOrder>());
        Assert.Equal(id, shipped.Id);
        Assert.Equal(42, shipped.Args.OrderId);
        Assert.Equal("sample_ship_order", shipped.Kind);
        Assert.Equal(["orders", "stamped"], shipped.Row.Tags);
        Assert.Equal("acme", shipped.Metadata.GetProperty("tenant").GetString());
        Assert.Equal(42, shipped.Row.Args.GetProperty("orderId").GetInt64());
        Assert.Equal(JobState.Available, shipped.Row.State);

        var withDefaults = oxbow.Jobs[1];
        Assert.Equal(("slow", 2), (withDefaults.Row.Queue, withDefaults.Row.Priority));
        Assert.Equal(("slow", 2), (withDefaults.Options!.Queue, withDefaults.Options.Priority));
        Assert.Equal(25, withDefaults.Row.MaxAttempts);
    }

    [Fact]
    public async Task The_recording_client_reads_and_manages_what_it_stored()
    {
        var oxbow = new RecordingOxbowClient();
        var later = await oxbow.InsertAsync(new ShipOrder(1), new InsertOptions { ScheduledAt = DateTimeOffset.UtcNow.AddHours(1) });
        var tagged = await oxbow.InsertAsync(new ShipOrder(2), new InsertOptions { Metadata = new JsonObject { ["tenant"] = "acme" } });
        var batch = await oxbow.InsertBatchAsync([new InsertManyItem(new ShipOrder(3)), new InsertManyItem(new ShipOrder(4))]);

        Assert.Equal(JobState.Scheduled, later.Job.State);
        Assert.Equal(tagged.Job.Id, Assert.Single((await oxbow.ListAsync(new JobListQuery { Metadata = new JsonObject { ["tenant"] = "acme" } })).Jobs).Id);
        Assert.Equal(batch.BatchId, (await oxbow.GetAsync(batch.Jobs[0].Job.Id))!.BatchId);

        Assert.Equal(JobState.Cancelled, (await oxbow.CancelAsync(later.Job.Id))!.State);
        Assert.Equal(new CancelManyResult(2, 0), await oxbow.CancelManyAsync(new JobFilter { BatchId = batch.BatchId }));
        Assert.Equal(2, (await oxbow.GetBatchAsync(batch.BatchId))!.Cancelled);
        await Assert.ThrowsAsync<ArgumentException>(() => oxbow.DeleteManyAsync(new JobFilter()));
        Assert.Equal(4, await oxbow.DeleteManyAsync(new JobFilter { All = true }));
        Assert.Empty(oxbow.Jobs);

        var page = await oxbow.ListAsync(new JobListQuery { Limit = 1 });
        Assert.Empty(page.Jobs);
        Assert.Null(page.NextCursor);
    }

    private sealed class CountResults(List<int> counts) : IJobInsertMiddleware
    {
        public async Task<IReadOnlyList<InsertResult>> InvokeAsync(JobInsertContext context, JobInsertDelegate next)
        {
            var results = await next(context);
            counts.Add(results.Count);
            return results;
        }
    }

    [Fact]
    public async Task The_recording_client_matches_Oxbow_at_the_edges()
    {
        var counts = new List<int>();
        var oxbow = new RecordingOxbowClient([new CountResults(counts)]);

        Assert.Equal(2, await oxbow.InsertManyFastAsync([new InsertManyItem(new ShipOrder(1)), new InsertManyItem(new ShipOrder(2))]));
        Assert.Equal([0], counts);

        await Assert.ThrowsAsync<ArgumentException>(() => oxbow.DeleteManyAsync(new JobFilter { Tags = [] }));
        await Assert.ThrowsAsync<ArgumentException>(() => oxbow.CancelManyAsync(new JobFilter { Metadata = [] }));

        var callbacks = new BatchOptions { OnSuccess = new BatchCallback(new ShipOrder(9)) };
        var batch = await oxbow.InsertBatchAsync([new InsertManyItem(new ShipOrder(3))], callbacks);
        var recorded = Assert.Single(oxbow.Batches);
        Assert.Equal((batch.BatchId, callbacks), (recorded.Id, recorded.Options));
        Assert.Equal([batch.Jobs[0].Job.Id], recorded.JobIds);
    }

    [Fact]
    public async Task The_recording_client_pages_like_Oxbow()
    {
        var oxbow = new RecordingOxbowClient();
        await oxbow.InsertManyAsync(Enumerable.Range(1, 3).Select(i => new InsertManyItem(new ShipOrder(i))));

        var first = await oxbow.ListAsync(new JobListQuery { Limit = 2 });
        var second = await oxbow.ListAsync(new JobListQuery { Limit = 2, Cursor = first.NextCursor });

        Assert.Equal([1L, 2L], first.Jobs.Select(j => j.Id));
        Assert.Equal([3L], second.Jobs.Select(j => j.Id));
        Assert.Null(second.NextCursor);
    }

    private static JobRow Row(string kind) => new()
    {
        Id = 1,
        State = JobState.Running,
        Attempt = 1,
        MaxAttempts = 25,
        AttemptedBy = [],
        CreatedAt = DateTimeOffset.UtcNow,
        ScheduledAt = DateTimeOffset.UtcNow,
        Priority = 1,
        Args = JsonDocument.Parse("{}").RootElement,
        Errors = [],
        Kind = kind,
        Metadata = JsonDocument.Parse("{}").RootElement,
        Queue = "default",
        Tags = [],
    };
}

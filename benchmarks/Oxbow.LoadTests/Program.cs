using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Oxbow;

var role = args[0];
var node = role == "worker" ? args[1] : role;
var builder = Host.CreateApplicationBuilder();
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.Services.AddNpgsqlDataSource(Environment.GetEnvironmentVariable("OXBOW_LOAD_DB") ?? throw new InvalidOperationException("Missing OXBOW_LOAD_DB"));
builder.Services.AddOxbow(o =>
{
    o.ClientId = node;
    if (role == "worker")
    {
        o.Queues["default"] = new();
        o.Queues["hot"] = new() { ConcurrencyLimit = new() { Global = 64 } };
        o.Queues["rate"] = new() { PartitionRateLimit = new(500, TimeSpan.FromSeconds(1)) { Burst = 64 } };
        if (Environment.GetEnvironmentVariable("OXBOW_LOAD_SHORT_RETENTION") == "1")
        {
            o.CompletedJobRetention = TimeSpan.FromMinutes(5);
        }
        o.JobTimeout = TimeSpan.FromSeconds(10);
        o.RescueStuckJobsAfter = TimeSpan.FromSeconds(12);
    }
}).AddWorker<LoadArgs, LoadWorker>();
using var host = builder.Build();
var db = host.Services.GetRequiredService<NpgsqlDataSource>();
var client = host.Services.GetRequiredService<IOxbowNpgsqlClient>();

switch (role)
{
    case "init":
        foreach (var migration in OxbowSchema.GetMigrations(0))
        {
            await using var command = db.CreateCommand(migration.Sql);
            await command.ExecuteNonQueryAsync();
        }
        await using (var command = db.CreateCommand("CREATE TABLE effects (job bigint, attempt int, node text, PRIMARY KEY(job, attempt))"))
        {
            await command.ExecuteNonQueryAsync();
        }
        break;
    case "worker":
        await host.RunAsync();
        break;
    case "probe":
        // The controller starts the follower after the first worker records its effect.
        var probe = new LoadArgs(-1, 0, "crash");
        await client.InsertBatchAsync([new(probe)],
            new() { OnComplete = new(new LoadArgs(-2, 0, "callback")) });
        await client.InsertAsync(new LoadArgs(-3, 0, "retry"),
            new InsertOptions { ScheduledAt = DateTimeOffset.UtcNow.AddSeconds(2) });
        await using (var connection = await db.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await client.InsertAsync(connection, transaction, new LoadArgs(-4, 0, "rollback"));
            await transaction.RollbackAsync();
        }
        break;
    case "produce":
        var clock = Stopwatch.StartNew();
        var scheduled = TimeSpan.Zero;
        foreach (var segment in args.Skip(1).Chunk(8))
        {
            await Traffic.RunAsync(client, segment, clock, scheduled);
            scheduled += TimeSpan.FromSeconds(double.Parse(segment[1], CultureInfo.InvariantCulture)
                / int.Parse(segment[2], CultureInfo.InvariantCulture));
        }
        break;
    default:
        throw new ArgumentException("Expected init, worker, probe, or produce.");
}

[JobKind("load")]
internal sealed record LoadArgs(int Seq, int DelayMs, string Mode = "normal") : IJobArgs;

internal sealed class LoadWorker(NpgsqlDataSource db) : IWorker<LoadArgs>
{
    public async Task WorkAsync(Job<LoadArgs> job, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        if (job.Args.Mode != "normal")
        {
            await using var cmd = db.CreateCommand("INSERT INTO effects VALUES ($1, $2, $3)");
            cmd.Parameters.AddWithValue(job.Id);
            cmd.Parameters.AddWithValue(job.Attempt);
            cmd.Parameters.AddWithValue(job.Row.AttemptedBy[^1]);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
            if (job.Attempt == 1 && job.Args.Mode == "crash")
            {
                // The controller kills this process after observing its durable effect.
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            if (job.Attempt == 1 && job.Args.Mode == "retry")
            {
                throw new JobRetryException(TimeSpan.FromMilliseconds(500), "injected retry");
            }
        }
        if (job.Args.DelayMs > 0)
        {
            await Task.Delay(job.Args.DelayMs, cancellationToken);
        }
        job.SetOutput(job.Queue == "hot"
            ? new long[] { job.Args.Seq, started, Stopwatch.GetTimestamp() }
            : job.Args.Seq);
    }
}

internal static class Traffic
{
    // A missed dispatch deadline remains visible in the offered-to-ack latency.
    // Bound in-flight transactions and record every offer the producer cannot dispatch.
    public static async Task RunAsync(IOxbowClient client, string[] args, Stopwatch timer, TimeSpan scheduled)
    {
        var culture = CultureInfo.InvariantCulture;
        var first = int.Parse(args[0], culture);
        var count = int.Parse(args[1], culture);
        var rate = int.Parse(args[2], culture);
        var batch = int.Parse(args[3], culture);
        var delay = int.Parse(args[4], culture);
        var api = args[5];
        var options = new InsertOptions { Queue = args[6], PartitionKey = args[7] };
        var pending = new List<Task>();
        for (var offset = 0; offset < count; offset += batch)
        {
            var due = scheduled + TimeSpan.FromSeconds((double)offset / rate);
            var remaining = due - timer.Elapsed;
            if (remaining.TotalMilliseconds >= 1)
            {
                await Task.Delay(remaining);
            }
            for (var i = pending.Count - 1; i >= 0; i--)
            {
                if (pending[i].IsCompleted)
                {
                    await pending[i];
                    pending.RemoveAt(i);
                }
            }
            var start = first + offset;
            var size = Math.Min(batch, count - offset);
            if (pending.Count >= 512)
            {
                Console.WriteLine(JsonSerializer.Serialize(new { start, size, acknowledged = false, skipped = true }));
                continue;
            }
            pending.Add(InsertAsync(start, size, due));
        }
        await Task.WhenAll(pending);

        async Task InsertAsync(int start, int size, TimeSpan due)
        {
            var begin = timer.Elapsed;
            var items = Enumerable.Range(start, size)
                .Select(n => new InsertManyItem(new LoadArgs(n, delay), options)).ToArray();
            try
            {
                var inserted = api switch
                {
                    "single" when size == 1 => (await client.InsertAsync((LoadArgs)items[0].Args, options)).UniqueSkippedAsDuplicate ? 0 : 1,
                    "many" => (await client.InsertManyAsync(items)).Count,
                    "copy" => await client.InsertManyFastAsync(items),
                    _ => throw new ArgumentException("Invalid insert API or batch size.")
                };
                if (inserted != size)
                {
                    throw new InvalidOperationException("Acknowledged count did not match the batch.");
                }
                Receipt(true);
            }
            catch (NpgsqlException)
            {
                // The verifier resolves each ambiguous range from durable job rows.
                Receipt(false);
            }

            void Receipt(bool acknowledged)
            {
                var end = timer.Elapsed;
                Console.WriteLine(JsonSerializer.Serialize(new { start, size, acknowledged,
                    insertMs = (end - begin).TotalMilliseconds, offeredToAckMs = (end - due).TotalMilliseconds }));
            }
        }
    }
}

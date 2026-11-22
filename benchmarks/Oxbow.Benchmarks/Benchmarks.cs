using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using IHost = Microsoft.Extensions.Hosting.IHost;

namespace Oxbow.Benchmarks;

[MemoryDiagnoser]
public class InsertBenchmarks
{
    private Database _database = null!;
    private IHost _host = null!;
    private IOxbowClient _client = null!;
    private NpgsqlDataSource _dataSource = null!;
    private InsertManyItem[] _batch = null!;

    [Params(1_000)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _database = await Database.StartAsync();
        _host = _database.BuildHost(maxWorkers: 1, withQueue: false);
        _client = _host.Services.GetRequiredService<IOxbowClient>();
        _dataSource = _host.Services.GetRequiredService<NpgsqlDataSource>();
        _batch = Enumerable.Range(0, BatchSize).Select(i => new InsertManyItem(new NoopArgs(i))).ToArray();
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        _host.Dispose();
        await _database.DisposeAsync();
    }

    [Benchmark(Description = "InsertAsync (own transaction)")]
    public Task<InsertResult> Insert() => _client.InsertAsync(new NoopArgs(1));

    [Benchmark(Description = "InsertAsync (caller's transaction)")]
    public async Task InsertInCallersTransaction()
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await _client.InsertAsync(connection, transaction, new NoopArgs(1));
        await transaction.CommitAsync();
    }

    [Benchmark(Description = "InsertManyAsync")]
    public Task<IReadOnlyList<InsertResult>> InsertMany() => _client.InsertManyAsync(_batch);
}

/// <summary>Jobs worked per second: inserts a backlog, then times one node draining it with a no-op worker.</summary>
[SimpleJob(RunStrategy.Monitoring, launchCount: 1, warmupCount: 1, iterationCount: 5)]
public class ThroughputBenchmarks
{
    private Database _database = null!;
    private IHost _host = null!;
    private Counter _counter = null!;

    [Params(10_000)]
    public int Jobs { get; set; }

    [Params(100, 1_000)]
    public int MaxWorkers { get; set; }

    [GlobalSetup]
    public async Task SetupAsync() => _database = await Database.StartAsync();

    [GlobalCleanup]
    public async Task CleanupAsync() => await _database.DisposeAsync();

    [IterationSetup]
    public void Setup()
    {
        _database.TruncateAsync().GetAwaiter().GetResult();
        _host = _database.BuildHost(MaxWorkers, withQueue: true);
        _counter = _host.Services.GetRequiredService<Counter>();
        var client = _host.Services.GetRequiredService<IOxbowClient>();
        for (var offset = 0; offset < Jobs; offset += 5_000)
        {
            client.InsertManyAsync(Enumerable.Range(offset, Math.Min(5_000, Jobs - offset)).Select(i => new InsertManyItem(new NoopArgs(i))))
                .GetAwaiter().GetResult();
        }
    }

    [IterationCleanup]
    public void Cleanup()
    {
        _host.StopAsync().GetAwaiter().GetResult();
        _host.Dispose();
    }

    [Benchmark(Description = "Work a backlog")]
    public async Task Drain()
    {
        var done = _counter.Reset(Jobs);
        await _host.StartAsync();
        await done;
    }
}

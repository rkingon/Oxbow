using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Oxbow.Benchmarks;

/// <summary>A Postgres container with Oxbow's schema, shared by a benchmark class.</summary>
public sealed class Database : IAsyncDisposable
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine")
        .WithCommand("-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "max_connections=300")
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public static async Task<Database> StartAsync()
    {
        var database = new Database();
        await database._container.StartAsync();
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        foreach (var migration in OxbowSchema.GetMigrations(0))
        {
            await using var command = new NpgsqlCommand(migration.Sql, connection);
            await command.ExecuteNonQueryAsync();
        }

        return database;
    }

    public async Task TruncateAsync()
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("TRUNCATE public.oxbow_job", connection);
        await command.ExecuteNonQueryAsync();
    }

    public IHost BuildHost(int maxWorkers, bool withQueue)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddNpgsqlDataSource(ConnectionString, b => b.ConnectionStringBuilder.MaxPoolSize = 200);
        builder.Services.AddSingleton<Counter>();
        builder.Services
            .AddOxbow(o =>
            {
                if (withQueue)
                {
                    o.Queues["default"] = new QueueOptions { MaxWorkers = maxWorkers };
                }
            })
            .AddWorker<NoopArgs, NoopWorker>();
        return builder.Build();
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}

[JobKind("noop")]
public sealed record NoopArgs(int N) : IJobArgs;

public sealed class Counter
{
    private int _count;
    private TaskCompletionSource _done = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _target;

    public Task Reset(int target)
    {
        _count = 0;
        _target = target;
        _done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        return _done.Task;
    }

    public void Increment()
    {
        if (Interlocked.Increment(ref _count) == _target)
        {
            _done.TrySetResult();
        }
    }
}

public sealed class NoopWorker(Counter counter) : IWorker<NoopArgs>
{
    public Task WorkAsync(Job<NoopArgs> job, CancellationToken cancellationToken)
    {
        counter.Increment();
        return Task.CompletedTask;
    }
}

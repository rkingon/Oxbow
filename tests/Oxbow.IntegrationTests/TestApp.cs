using System.Collections.Concurrent;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Oxbow.IntegrationTests;

/// <summary>A host running Oxbow as the least-privilege app role, with fast intervals for tests.</summary>
public sealed class TestApp : IAsyncDisposable
{
    private TestApp(IHost host, string prefix)
    {
        Host = host;
        Prefix = prefix;
    }

    public IHost Host { get; }

    public string Prefix { get; }

    public IOxbowClient Client => Host.Services.GetRequiredService<IOxbowClient>();

    public JobRecorder Recorder => Host.Services.GetRequiredService<JobRecorder>();

    public NpgsqlDataSource DataSource => Host.Services.GetRequiredService<NpgsqlDataSource>();

    public static async Task<TestApp> StartAsync(
        PostgresFixture database,
        string prefix,
        Action<OxbowOptions>? configure = null,
        Action<IOxbowBuilder>? register = null,
        JobRecorder? recorder = null,
        TimeSpan? shutdownTimeout = null,
        bool start = true)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddFilter("Oxbow", LogLevel.Warning);
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = shutdownTimeout ?? TimeSpan.FromSeconds(5));
        builder.Services.AddSingleton(recorder ?? new JobRecorder());
        builder.Services.AddNpgsqlDataSource(database.AppConnectionString);
        var oxbow = builder.Services.AddOxbow(o =>
        {
            o.TablePrefix = prefix;
            o.FetchPollInterval = TimeSpan.FromMilliseconds(200);
            o.SchedulerInterval = TimeSpan.FromMilliseconds(300);
            o.RescuerInterval = TimeSpan.FromMilliseconds(300);
            o.CleanerInterval = TimeSpan.FromMilliseconds(300);
            o.ElectInterval = TimeSpan.FromMilliseconds(500);
            configure?.Invoke(o);
        });
        register?.Invoke(oxbow);
        var host = builder.Build();
        if (start)
        {
            await host.StartAsync();
        }

        return new TestApp(host, prefix);
    }

    public async Task<JobRow> WaitForAsync(long jobId, Func<JobRow, bool> predicate, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(15));
        JobRow? last = null;
        while (!cts.IsCancellationRequested)
        {
            last = await Client.GetAsync(jobId);
            if (last is not null && predicate(last))
            {
                return last;
            }

            await Task.Delay(50);
        }

        throw new TimeoutException($"Job {jobId} never matched; last state {last?.State}, attempt {last?.Attempt}.");
    }

    public Task<JobRow> WaitForStateAsync(long jobId, JobState state, TimeSpan? timeout = null) =>
        WaitForAsync(jobId, j => j.State == state, timeout);

    public async ValueTask DisposeAsync()
    {
        try
        {
            await Host.StopAsync();
        }
        finally
        {
            Host.Dispose();
        }
    }
}

/// <summary>Records what workers saw, shared across hosts in a test.</summary>
public sealed class JobRecorder
{
    private readonly ConcurrentDictionary<long, TaskCompletionSource> _started = new();

    public ConcurrentDictionary<long, int> RunCounts { get; } = new();

    public ConcurrentQueue<string> Events { get; } = new();

    public void Started(long jobId)
    {
        RunCounts.AddOrUpdate(jobId, 1, (_, n) => n + 1);
        StartedSignal(jobId).TrySetResult();
    }

    public Task WaitStartedAsync(long jobId, TimeSpan? timeout = null) =>
        StartedSignal(jobId).Task.WaitAsync(timeout ?? TimeSpan.FromSeconds(15));

    private TaskCompletionSource StartedSignal(long jobId) =>
        _started.GetOrAdd(jobId, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
}

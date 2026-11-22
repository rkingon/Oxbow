namespace Oxbow.IntegrationTests;

[JobKind("record")]
public sealed record RecordArgs(string Value) : IJobArgs;

public sealed class RecordWorker(JobRecorder recorder) : IWorker<RecordArgs>
{
    public Task WorkAsync(Job<RecordArgs> job, CancellationToken cancellationToken)
    {
        recorder.Started(job.Id);
        recorder.Events.Enqueue(job.Args.Value);
        return Task.CompletedTask;
    }
}

[JobKind("fail")]
public sealed record FailArgs(string Message) : IJobArgs;

public sealed class FailWorker(JobRecorder recorder) : IWorker<FailArgs>
{
    public Task WorkAsync(Job<FailArgs> job, CancellationToken cancellationToken)
    {
        recorder.Started(job.Id);
        throw new InvalidOperationException(job.Args.Message);
    }

    public DateTimeOffset? GetNextRetry(Job<FailArgs> job) => DateTimeOffset.UtcNow.AddMilliseconds(100);
}

[JobKind("block")]
public sealed record BlockArgs(bool HonorCancellation = true) : IJobArgs;

public sealed class BlockWorker(JobRecorder recorder) : IWorker<BlockArgs>
{
    public async Task WorkAsync(Job<BlockArgs> job, CancellationToken cancellationToken)
    {
        recorder.Started(job.Id);
        await Task.Delay(Timeout.InfiniteTimeSpan, job.Args.HonorCancellation ? cancellationToken : CancellationToken.None);
    }

    public TimeSpan? GetTimeout(Job<BlockArgs> job) => Timeout.InfiniteTimeSpan;
}

[JobKind("slow")]
public sealed record SlowArgs(int TimeoutMilliseconds) : IJobArgs;

public sealed class SlowWorker(JobRecorder recorder) : IWorker<SlowArgs>
{
    public async Task WorkAsync(Job<SlowArgs> job, CancellationToken cancellationToken)
    {
        recorder.Started(job.Id);
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
    }

    public TimeSpan? GetTimeout(Job<SlowArgs> job) => TimeSpan.FromMilliseconds(job.Args.TimeoutMilliseconds);

    public DateTimeOffset? GetNextRetry(Job<SlowArgs> job) => DateTimeOffset.UtcNow.AddHours(1);
}

[JobKind("snooze")]
public sealed record SnoozeArgs(int Times) : IJobArgs;

public sealed class SnoozeWorker(JobRecorder recorder) : IWorker<SnoozeArgs>
{
    public Task WorkAsync(Job<SnoozeArgs> job, CancellationToken cancellationToken)
    {
        recorder.Started(job.Id);
        var snoozes = job.Metadata.TryGetProperty("snoozes", out var s) ? s.GetInt32() : 0;
        if (snoozes < job.Args.Times)
        {
            throw new JobSnoozeException(TimeSpan.FromMilliseconds(100));
        }

        return Task.CompletedTask;
    }
}

[JobKind("self_cancel")]
public sealed record SelfCancelArgs : IJobArgs;

public sealed class SelfCancelWorker : IWorker<SelfCancelArgs>
{
    public Task WorkAsync(Job<SelfCancelArgs> job, CancellationToken cancellationToken) =>
        throw new JobCancelException("Not worth retrying.");
}

[JobKind("unique")]
public sealed record UniqueArgs([property: UniqueKey] string CustomerId, string TraceId) : IJobArgs
{
    public InsertOptions GetInsertOptions() => new() { Unique = new() { ByArgs = true } };
}

public sealed class UniqueWorker : IWorker<UniqueArgs>
{
    public Task WorkAsync(Job<UniqueArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
}

[JobKind("nobody_works_this")]
public sealed record OrphanArgs : IJobArgs;

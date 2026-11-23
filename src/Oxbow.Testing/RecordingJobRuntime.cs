namespace Oxbow.Testing;

/// <summary>
/// An <see cref="IJobRuntime"/> that records heartbeats and progress, for unit tests that create a
/// <see cref="Job{TArgs}"/> directly.
/// </summary>
/// <param name="timeProvider">The clock stamped on progress. Default: the system clock.</param>
public sealed class RecordingJobRuntime(TimeProvider? timeProvider = null) : IJobRuntime
{
    private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
    private readonly List<RecordedProgress> _progress = [];
    private int _heartbeats;

    /// <inheritdoc/>
    public DateTimeOffset UtcNow => _timeProvider.GetUtcNow();

    /// <summary>How many times the job heartbeated.</summary>
    public int Heartbeats => Volatile.Read(ref _heartbeats);

    /// <summary>Every progress report, in order.</summary>
    public IReadOnlyList<RecordedProgress> Progress
    {
        get
        {
            lock (_progress)
            {
                return [.. _progress];
            }
        }
    }

    /// <inheritdoc/>
    public Task HeartbeatAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _heartbeats);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task ReportProgressAsync(double percent, string? message, CancellationToken cancellationToken)
    {
        lock (_progress)
        {
            _progress.Add(new RecordedProgress(percent, message, UtcNow));
        }

        return Task.CompletedTask;
    }
}

/// <summary>One progress report recorded by <see cref="RecordingJobRuntime"/>.</summary>
/// <param name="Percent">How far along the work was, from 0 to 100.</param>
/// <param name="Message">The step's description, if any.</param>
/// <param name="At">When it was reported.</param>
public sealed record RecordedProgress(double Percent, string? Message, DateTimeOffset At);

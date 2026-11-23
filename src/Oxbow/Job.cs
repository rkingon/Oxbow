using System.Text.Json;
using Oxbow.Processing;

namespace Oxbow;

/// <summary>A job being worked, with its deserialized args.</summary>
/// <typeparam name="TArgs">The args type.</typeparam>
public sealed class Job<TArgs>
    where TArgs : IJobArgs
{
    private const int MaxProgressMessageLength = 1_000;

    /// <summary>Creates a job, for example to unit test a worker.</summary>
    /// <param name="row">The stored row.</param>
    /// <param name="args">The deserialized args.</param>
    public Job(JobRow row, TArgs args)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(args);
        Row = row;
        Args = args;
    }

    /// <summary>The deserialized args.</summary>
    public TArgs Args { get; }

    /// <summary>The stored row.</summary>
    public JobRow Row { get; }

    /// <summary>The job's ID.</summary>
    public long Id => Row.Id;

    /// <summary>The job kind.</summary>
    public string Kind => Row.Kind;

    /// <summary>The queue the job runs in.</summary>
    public string Queue => Row.Queue;

    /// <summary>The current attempt number, starting at 1.</summary>
    public int Attempt => Row.Attempt;

    /// <summary>The number of attempts after which a failing job is discarded.</summary>
    public int MaxAttempts => Row.MaxAttempts;

    /// <summary>One entry per earlier failed attempt.</summary>
    public IReadOnlyList<AttemptError> Errors => Row.Errors;

    /// <summary>Arbitrary JSON metadata.</summary>
    public JsonElement Metadata => Row.Metadata;

    /// <summary>
    /// The progress this attempt last reported, or <see langword="null"/>. <see cref="JobRow.Progress"/> is what was
    /// stored when the attempt started, possibly by an earlier one.
    /// </summary>
    public JobProgress? Progress { get; private set; }

    /// <summary>Set by the executor; absent when the job is created directly, e.g. in a unit test.</summary>
    internal JobControl? Control { get; set; }

    /// <summary>
    /// Records that the attempt is alive, under <c>oxbow:heartbeat_at</c> in metadata. Once an attempt heartbeats, the
    /// rescuer judges it by its last heartbeat instead of its start and timeout: it's rescued only after
    /// <see cref="OxbowOptions.RescueStuckJobsAfter"/> without one. Calls are throttled, so call it as often as is
    /// convenient. Does nothing outside Oxbow's executor, or once the attempt has ended.
    /// </summary>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the heartbeat is written or skipped.</returns>
    public Task HeartbeatAsync(CancellationToken cancellationToken = default) =>
        Control?.HeartbeatAsync(cancellationToken) ?? Task.CompletedTask;

    /// <summary>
    /// Records progress under <c>oxbow:progress</c> in metadata, readable through <see cref="IOxbowClient.GetAsync"/>.
    /// Also counts as a heartbeat. Not throttled: every call is a write. Outside Oxbow's executor, only
    /// <see cref="Progress"/> is updated.
    /// </summary>
    /// <param name="percent">How far along the work is, from 0 to 100.</param>
    /// <param name="message">A description of the current step, at most 1,000 characters.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the progress is written.</returns>
    public Task ReportProgressAsync(double percent, string? message = null, CancellationToken cancellationToken = default)
    {
        if (!double.IsFinite(percent) || percent is < 0 or > 100)
        {
            throw new ArgumentOutOfRangeException(nameof(percent), percent, "Progress must be between 0 and 100.");
        }

        if (message?.Length > MaxProgressMessageLength)
        {
            throw new ArgumentException($"A progress message may be at most {MaxProgressMessageLength} characters.", nameof(message));
        }

        Progress = new JobProgress(percent, message, Attempt, Control?.Now ?? DateTimeOffset.UtcNow);
        return Control?.ReportProgressAsync(percent, message, cancellationToken) ?? Task.CompletedTask;
    }

    /// <summary>The batch this job is a member or callback of.</summary>
    public long? BatchId => Row.BatchId;
}

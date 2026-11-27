using System.Data.Common;
using System.Text.Json;

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

    /// <summary>Creates a job whose heartbeat and progress calls go to <paramref name="runtime"/>.</summary>
    /// <param name="row">The stored row.</param>
    /// <param name="args">The deserialized args.</param>
    /// <param name="runtime">Receives heartbeats and progress, e.g. a recording runtime in a unit test.</param>
    public Job(JobRow row, TArgs args, IJobRuntime runtime)
        : this(row, args)
    {
        ArgumentNullException.ThrowIfNull(runtime);
        _runtime = runtime;
    }

    private readonly IJobRuntime? _runtime;

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

    /// <summary>
    /// Records that the attempt is alive, under <c>oxbow:heartbeat_at</c> in metadata. Once an attempt heartbeats, the
    /// rescuer judges it by its last heartbeat instead of its start and timeout: it's rescued only after
    /// <c>OxbowOptions.RescueStuckJobsAfter</c> without one. Calls are throttled, so call it as often as is
    /// convenient. Does nothing without a runtime, or once the attempt has ended.
    /// </summary>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the heartbeat is written or skipped.</returns>
    public Task HeartbeatAsync(CancellationToken cancellationToken = default) =>
        _runtime?.HeartbeatAsync(cancellationToken) ?? Task.CompletedTask;

    /// <summary>
    /// Records progress under <c>oxbow:progress</c> in metadata, readable through <see cref="IOxbowClient.GetAsync"/>.
    /// Also counts as a heartbeat. Not throttled: every call is a write. Without a runtime, only
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

        Progress = new JobProgress(percent, message, Attempt, _runtime?.UtcNow ?? DateTimeOffset.UtcNow);
        return _runtime?.ReportProgressAsync(percent, message, cancellationToken) ?? Task.CompletedTask;
    }

    /// <summary>
    /// Runs a step whose result is recorded under <paramref name="name"/>, so a later execution of this job (after a
    /// failed attempt, a snooze, a rescue, a shutdown or a retry) returns the recorded result instead of running it again.
    /// A step that fails records nothing. At least once: an execution that dies after the step's effect but before its result
    /// is recorded runs it again. Steps are heartbeats.
    /// </summary>
    /// <typeparam name="T">The result type, serialized with the client's <c>JsonSerializerOptions</c>.</typeparam>
    /// <param name="name">
    /// The step's identity within the job, at most 255 characters, and unique within an execution. Renaming a step runs it
    /// again.
    /// </param>
    /// <param name="run">The step.</param>
    /// <param name="cancellationToken">Passed to <paramref name="run"/>.</param>
    /// <returns>The step's result, or the one recorded earlier, which wins if another execution recorded one first.</returns>
    /// <exception cref="JobAttemptSupersededException">
    /// This attempt is no longer the one running. Thrown before the step runs if that's already known; a step it was already
    /// running still records its result if none is recorded yet.
    /// </exception>
    public async Task<T> StepAsync<T>(string name, Func<CancellationToken, Task<T>> run, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(run);
        if (name.Length > MaxStepNameLength)
        {
            throw new ArgumentException($"A step name may be at most {MaxStepNameLength} characters.", nameof(name));
        }

        lock (_steps)
        {
            if (!_steps.Add(name))
            {
                throw new InvalidOperationException(
                    $"Step '{name}' already ran in this execution of job {Id}. A step's name identifies its result, so it must be unique.");
            }
        }

        try
        {
            return await (_runtime?.StepAsync(name, run, cancellationToken) ?? run(cancellationToken)).ConfigureAwait(false);
        }
        catch
        {
            // Nothing was recorded, so the worker may try the step again.
            lock (_steps)
            {
                _steps.Remove(name);
            }

            throw;
        }
    }

    /// <inheritdoc cref="StepAsync{T}(string, Func{CancellationToken, Task{T}}, CancellationToken)"/>
    public Task StepAsync(string name, Func<CancellationToken, Task> run, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(run);
        return StepAsync<object?>(name, async ct =>
        {
            await run(ct).ConfigureAwait(false);
            return null;
        }, cancellationToken);
    }

    /// <summary>
    /// Locks the job's row in <paramref name="transaction"/> until it commits or rolls back, provided this attempt is
    /// still the one running; otherwise fails the transaction, so nothing written in it can commit, and throws. While it's
    /// held, the rescuer can't take the job, so the transaction commits only as this attempt's. Under
    /// <c>REPEATABLE READ</c>, lock first: a later lock fails on any write to the row since the snapshot. Commit before
    /// the worker returns: the attempt's outcome waits for the lock. Heartbeats and progress are skipped while it's held.
    /// Does nothing without a runtime.
    /// </summary>
    /// <param name="transaction">An open transaction on the database Oxbow uses.</param>
    /// <param name="cancellationToken">Cancels the lock.</param>
    /// <returns>A task that completes when the row is locked.</returns>
    /// <exception cref="JobAttemptSupersededException">This attempt is no longer the one running.</exception>
    public Task LockAttemptAsync(DbTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return _runtime?.LockAttemptAsync(transaction, cancellationToken) ?? Task.CompletedTask;
    }

    private const int MaxStepNameLength = 255;

    private readonly HashSet<string> _steps = new(StringComparer.Ordinal);

    /// <summary>The batch this job is a member or callback of.</summary>
    public long? BatchId => Row.BatchId;

    /// <summary>The output recorded with <see cref="SetOutput"/>, not yet serialized.</summary>
    public object? Output => _output;

    private volatile object? _output;

    /// <summary>
    /// Records the job's result, stored in <c>metadata.output</c> when the job completes and readable with
    /// <see cref="JobRow.GetOutput{T}"/>. Discarded if the attempt doesn't complete. A later call replaces an earlier
    /// one.
    /// </summary>
    /// <param name="output">
    /// The result, serialized as its runtime type with the client's <c>JsonSerializerOptions</c>. At most
    /// <see cref="JobRow.MaxOutputSize"/> bytes of JSON; a larger output fails the attempt.
    /// </param>
    public void SetOutput(object output)
    {
        ArgumentNullException.ThrowIfNull(output);
        _output = output;
    }
}

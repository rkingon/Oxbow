using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Oxbow.Testing;

/// <summary>
/// An <see cref="IJobRuntime"/> that records heartbeats, progress, steps and attempt locks, for unit tests that create a
/// <see cref="Job{TArgs}"/> directly. Seed steps with <see cref="RecordStep{T}"/> to test a replay.
/// </summary>
/// <param name="timeProvider">The clock stamped on progress. Default: the system clock.</param>
public sealed class RecordingJobRuntime(TimeProvider? timeProvider = null) : IJobRuntime
{
    private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
    private readonly List<RecordedProgress> _progress = [];
    private readonly Dictionary<string, JsonElement> _steps = new(StringComparer.Ordinal);
    private int _heartbeats;
    private int _attemptLocks;

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

    /// <summary>
    /// Serializes step results, as the client's <c>OxbowOptions.JsonSerializerOptions</c> would. Default:
    /// <see cref="System.Text.Json.JsonSerializerOptions.Web"/>.
    /// </summary>
    public JsonSerializerOptions JsonSerializerOptions { get; init; } = JsonSerializerOptions.Web;

    /// <summary>Every recorded step result by name, seeded or run.</summary>
    public IReadOnlyDictionary<string, JsonElement> Steps
    {
        get
        {
            lock (_steps)
            {
                return new Dictionary<string, JsonElement>(_steps, StringComparer.Ordinal);
            }
        }
    }

    /// <summary>How many times the job locked its attempt in a transaction.</summary>
    public int AttemptLocks => Volatile.Read(ref _attemptLocks);

    /// <summary>Records a step's result as an earlier execution would have, so the job returns it instead of running the step.</summary>
    /// <typeparam name="T">The step's result type.</typeparam>
    /// <param name="name">The step's name.</param>
    /// <param name="result">The result.</param>
    public void RecordStep<T>(string name, T result)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var element = Serialize(name, result);
        lock (_steps)
        {
            _steps[name] = element;
        }
    }

    /// <inheritdoc/>
    public async Task<T> StepAsync<T>(string name, Func<CancellationToken, Task<T>> run, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(run);
        lock (_steps)
        {
            if (_steps.TryGetValue(name, out var recorded))
            {
                return recorded.Deserialize(TypeInfo<T>())!;
            }
        }

        var result = await run(cancellationToken).ConfigureAwait(false);
        var element = Serialize(name, result);
        lock (_steps)
        {
            if (!_steps.TryAdd(name, element))
            {
                return _steps[name].Deserialize(TypeInfo<T>())!;
            }
        }

        return result;
    }

    /// <inheritdoc/>
    public Task LockAttemptAsync(DbTransaction transaction, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _attemptLocks);
        return Task.CompletedTask;
    }

    // Mirrors Oxbow's executor: the declared type, the client's options, and the output size limit.
    private JsonElement Serialize<T>(string name, T result)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(result, TypeInfo<T>());
        if (bytes.Length > JobRow.MaxOutputSize)
        {
            throw new InvalidOperationException(string.Format(
                CultureInfo.InvariantCulture,
                "The result of step '{0}' is {1:N0} bytes of JSON, over the {2:N0}-byte limit (JobRow.MaxOutputSize).",
                name,
                bytes.Length,
                JobRow.MaxOutputSize));
        }

        using var document = JsonDocument.Parse(bytes);
        return document.RootElement.Clone();
    }

    private JsonTypeInfo<T> TypeInfo<T>()
    {
        var options = JsonSerializerOptions;
        if (!options.IsReadOnly)
        {
            options = new JsonSerializerOptions(options);
            options.TypeInfoResolver ??= new DefaultJsonTypeInfoResolver();
        }

        return (JsonTypeInfo<T>)options.GetTypeInfo(typeof(T));
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

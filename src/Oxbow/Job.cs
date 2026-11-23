using System.Text.Json;

namespace Oxbow;

/// <summary>A job being worked, with its deserialized args.</summary>
/// <typeparam name="TArgs">The args type.</typeparam>
public sealed class Job<TArgs>
    where TArgs : IJobArgs
{
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

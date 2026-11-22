using System.Text.Json;

namespace Oxbow;

/// <summary>A job being worked, with its deserialized args.</summary>
/// <typeparam name="TArgs">The args type.</typeparam>
public sealed class Job<TArgs>
    where TArgs : IJobArgs
{
    internal Job(JobRow row, TArgs args)
    {
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
}

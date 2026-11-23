using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Nodes;

namespace Oxbow;

/// <summary>The next step in the insert pipeline.</summary>
/// <param name="context">The jobs being inserted.</param>
/// <returns>The insert results, in order.</returns>
[SuppressMessage("Naming", "CA1711", Justification = "Mirrors ASP.NET Core's RequestDelegate.")]
public delegate Task<IReadOnlyList<InsertResult>> JobInsertDelegate(JobInsertContext context);

/// <summary>Wraps every insert, single or bulk. Resolved from the root DI container.</summary>
public interface IJobInsertMiddleware
{
    /// <summary>Runs around the rest of the pipeline. Call <paramref name="next"/> to continue.</summary>
    /// <param name="context">The jobs being inserted.</param>
    /// <param name="next">The rest of the pipeline.</param>
    /// <returns>The insert results, in order.</returns>
    [SuppressMessage("Naming", "CA1716", Justification = "Mirrors ASP.NET Core's IMiddleware.")]
    Task<IReadOnlyList<InsertResult>> InvokeAsync(JobInsertContext context, JobInsertDelegate next);
}

/// <summary>The jobs being inserted, as seen by insert middleware.</summary>
public sealed class JobInsertContext
{
    /// <summary>Creates a context, for example to unit test middleware.</summary>
    /// <param name="jobs">The jobs, in insert order.</param>
    /// <param name="cancellationToken">The insert's cancellation token.</param>
    public JobInsertContext(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken)
    {
        Jobs = jobs;
        CancellationToken = cancellationToken;
    }

    /// <summary>The jobs, in insert order.</summary>
    public IReadOnlyList<JobInsertParams> Jobs { get; }

    /// <summary>The insert's cancellation token.</summary>
    public CancellationToken CancellationToken { get; }
}

/// <summary>
/// One job about to be inserted. Middleware may change its metadata, tags, queue, priority and partition key.
/// </summary>
public sealed class JobInsertParams
{
    /// <summary>Creates a job to insert, for example to unit test middleware.</summary>
    /// <param name="args">The args.</param>
    /// <param name="kind">The job kind.</param>
    public JobInsertParams(IJobArgs args, string kind)
    {
        Args = args;
        Kind = kind;
    }

    /// <summary>The args.</summary>
    public IJobArgs Args { get; }

    /// <summary>The job kind.</summary>
    public string Kind { get; }

    /// <summary>The queue.</summary>
    public string Queue { get; set; } = "default";

    /// <summary>Priority from 1 (highest) to 4.</summary>
    public int Priority { get; set; } = 1;

    /// <summary>Attempts before a failing job is discarded.</summary>
    public int MaxAttempts { get; set; }

    /// <summary>When the job becomes eligible to run; <see langword="null"/> for now.</summary>
    public DateTimeOffset? ScheduledAt { get; set; }

    /// <summary>Metadata stored with the job.</summary>
    public JsonObject Metadata { get; } = [];

    /// <summary>Tags stored with the job.</summary>
    public IList<string> Tags { get; } = [];

    /// <summary>The partition key for the queue's concurrency limit; <see langword="null"/> for none.</summary>
    public string? PartitionKey { get; set; }

    internal string? EncodedArgs { get; set; }

    internal byte[]? UniqueKey { get; set; }

    internal int UniqueStates { get; set; }

    internal bool Scheduled { get; set; }

    internal string? SequenceKey { get; set; }

    internal SequenceContinueOn SequenceContinueOn { get; set; }
}

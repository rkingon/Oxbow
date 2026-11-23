using System.Diagnostics.CodeAnalysis;

namespace Oxbow;

/// <summary>The next step in the work pipeline.</summary>
/// <param name="context">The job being worked.</param>
/// <returns>A task that completes when the rest of the pipeline has run.</returns>
[SuppressMessage("Naming", "CA1711", Justification = "Mirrors ASP.NET Core's RequestDelegate.")]
public delegate Task JobWorkDelegate(JobContext context);

/// <summary>
/// Wraps the working of every job, like ASP.NET Core middleware. Resolved from the job's DI scope.
/// </summary>
public interface IJobWorkMiddleware
{
    /// <summary>Runs around the rest of the pipeline. Call <paramref name="next"/> to continue.</summary>
    /// <param name="context">The job being worked.</param>
    /// <param name="next">The rest of the pipeline.</param>
    /// <returns>A task that completes when the middleware is done.</returns>
    [SuppressMessage("Naming", "CA1716", Justification = "Mirrors ASP.NET Core's IMiddleware.")]
    Task InvokeAsync(JobContext context, JobWorkDelegate next);
}

/// <summary>The job being worked, as seen by work middleware.</summary>
public sealed class JobContext
{
    /// <summary>Creates a context, for example to unit test middleware.</summary>
    /// <param name="job">The stored job.</param>
    /// <param name="args">The deserialized args.</param>
    /// <param name="services">The job's DI scope.</param>
    /// <param name="cancellationToken">The job's cancellation token.</param>
    public JobContext(JobRow job, IJobArgs args, IServiceProvider services, CancellationToken cancellationToken)
    {
        Job = job;
        Args = args;
        Services = services;
        CancellationToken = cancellationToken;
    }

    /// <summary>The stored job.</summary>
    public JobRow Job { get; }

    /// <summary>The deserialized args.</summary>
    public IJobArgs Args { get; }

    /// <summary>The job's DI scope.</summary>
    public IServiceProvider Services { get; }

    /// <summary>The job's cancellation token, as passed to the worker.</summary>
    public CancellationToken CancellationToken { get; }
}

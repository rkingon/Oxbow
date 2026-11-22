using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Oxbow.Testing;

/// <summary>
/// Runs a worker inline, without a database: a real DI scope, the registered work middleware, the worker's timeout,
/// and the outcome Oxbow would record.
/// </summary>
/// <typeparam name="TArgs">The args type.</typeparam>
/// <typeparam name="TWorker">The worker. Resolved from DI, or created with its dependencies if not registered.</typeparam>
/// <param name="services">The service provider, typically the app's or a test container's.</param>
public sealed class WorkerHarness<TArgs, TWorker>(IServiceProvider services)
    where TArgs : IJobArgs
    where TWorker : class, IWorker<TArgs>
{
    private long _nextId;

    /// <summary>Works a job with <paramref name="args"/>.</summary>
    /// <param name="args">The job's args.</param>
    /// <param name="attempt">The attempt number the worker sees.</param>
    /// <param name="cancellationToken">Passed to the worker, linked with its timeout.</param>
    /// <returns>What happened.</returns>
    public async Task<WorkResult> WorkAsync(TArgs args, int attempt = 1, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        var options = services.GetService<IOptions<OxbowOptions>>()?.Value ?? new OxbowOptions();
        var json = new JsonSerializerOptions(options.JsonSerializerOptions);
        json.TypeInfoResolver ??= new System.Text.Json.Serialization.Metadata.DefaultJsonTypeInfoResolver();

        var kind = typeof(TArgs).GetCustomAttributes(typeof(JobKindAttribute), inherit: false) is [JobKindAttribute attribute, ..]
            ? attribute.Kind
            : throw new InvalidOperationException($"'{typeof(TArgs).FullName}' has no [JobKind] attribute.");
        var now = DateTimeOffset.UtcNow;
        var row = new JobRow
        {
            Id = Interlocked.Increment(ref _nextId),
            State = JobState.Running,
            Attempt = attempt,
            MaxAttempts = options.MaxAttempts,
            AttemptedAt = now,
            AttemptedBy = ["worker-harness"],
            CreatedAt = now,
            ScheduledAt = now,
            Priority = 1,
            Args = JsonSerializer.SerializeToElement(args, json.GetTypeInfo(typeof(TArgs))),
            Errors = [],
            Kind = kind,
            Metadata = JsonDocument.Parse("{}").RootElement,
            Queue = "default",
            Tags = [],
        };

        var scope = services.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            IWorker<TArgs> worker = scope.ServiceProvider.GetService<TWorker>()
                ?? ActivatorUtilities.CreateInstance<TWorker>(scope.ServiceProvider);
            var job = new Job<TArgs>(row, args);
            var timeout = worker.GetTimeout(job) ?? options.JobTimeout;
            using var timeoutSource = timeout > TimeSpan.Zero && timeout != Timeout.InfiniteTimeSpan
                ? new CancellationTokenSource(timeout)
                : new CancellationTokenSource();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token);

            JobWorkDelegate pipeline = context => worker.WorkAsync(job, context.CancellationToken);
            var middleware = scope.ServiceProvider.GetServices<IJobWorkMiddleware>().ToArray();
            for (var i = middleware.Length - 1; i >= 0; i--)
            {
                var current = middleware[i];
                var next = pipeline;
                pipeline = context => current.InvokeAsync(context, next);
            }

            try
            {
                await pipeline(new JobContext(row, args, scope.ServiceProvider, linked.Token)).ConfigureAwait(false);
                return new WorkResult(WorkOutcome.Completed, null, null);
            }
            catch (JobSnoozeException ex)
            {
                return new WorkResult(WorkOutcome.Snoozed, ex, ex.Duration);
            }
            catch (JobCancelException ex)
            {
                return new WorkResult(WorkOutcome.Cancelled, ex, null);
            }
            catch (OperationCanceledException ex) when (timeoutSource.IsCancellationRequested)
            {
                return new WorkResult(WorkOutcome.TimedOut, ex, null);
            }
#pragma warning disable CA1031 // The harness reports every failure as a result.
            catch (Exception ex)
#pragma warning restore CA1031
            {
                return new WorkResult(WorkOutcome.Errored, ex, null);
            }
        }
    }
}

/// <summary>What a worker did with a job.</summary>
public enum WorkOutcome
{
    /// <summary>Returned normally; Oxbow would complete the job.</summary>
    Completed,

    /// <summary>Threw; Oxbow would retry or discard the job.</summary>
    Errored,

    /// <summary>Threw <see cref="JobSnoozeException"/>.</summary>
    Snoozed,

    /// <summary>Threw <see cref="JobCancelException"/>.</summary>
    Cancelled,

    /// <summary>Exceeded its timeout; Oxbow would retry or discard the job.</summary>
    TimedOut,
}

/// <summary>The result of running a worker through <see cref="WorkerHarness{TArgs, TWorker}"/>.</summary>
/// <param name="Outcome">What happened.</param>
/// <param name="Exception">The exception the worker threw, if any.</param>
/// <param name="SnoozeDuration">The snooze duration, if snoozed.</param>
public sealed record WorkResult(WorkOutcome Outcome, Exception? Exception, TimeSpan? SnoozeDuration);

using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Oxbow.Processing;

namespace Oxbow.Internal;

internal abstract class WorkerRegistration
{
    public abstract Type ArgsType { get; }

    public abstract WorkUnit CreateUnit(JobRow row, IServiceProvider services, JsonSerializerOptions json, IJobArgsProtector? protector);
}

internal sealed class WorkerRegistration<TArgs, TWorker> : WorkerRegistration
    where TArgs : IJobArgs
    where TWorker : class, IWorker<TArgs>
{
    public override Type ArgsType => typeof(TArgs);

    public override WorkUnit CreateUnit(JobRow row, IServiceProvider services, JsonSerializerOptions json, IJobArgsProtector? protector)
    {
        var typeInfo = (JsonTypeInfo<TArgs>)json.GetTypeInfo(typeof(TArgs));
        var args = ArgsEnvelope.Open(row, protector).Deserialize(typeInfo)
            ?? throw new JsonException($"Args for job {row.Id} deserialized to null.");
        IWorker<TArgs> worker = services.GetRequiredService<TWorker>();
        return new Unit(worker, new Job<TArgs>(row, args));
    }

    private sealed class Unit(IWorker<TArgs> worker, Job<TArgs> job) : WorkUnit
    {
        public override IJobArgs Args => job.Args;

        public override TimeSpan? GetTimeout() => worker.GetTimeout(job);

        public override DateTimeOffset? GetNextRetry() => worker.GetNextRetry(job);

        public override Task WorkAsync(CancellationToken cancellationToken) => worker.WorkAsync(job, cancellationToken);

        public override void Attach(JobControl control) => job.Control = control;
        public override object? Output => job.Output;
    }
}

internal abstract class WorkUnit
{
    public abstract IJobArgs Args { get; }

    public abstract TimeSpan? GetTimeout();

    public abstract DateTimeOffset? GetNextRetry();

    public abstract Task WorkAsync(CancellationToken cancellationToken);

    public abstract void Attach(JobControl control);
    public abstract object? Output { get; }
}

/// <summary>Every registered worker by kind and alias, validated when first resolved (at host start).</summary>
internal sealed class WorkerRegistry
{
    private readonly Dictionary<string, WorkerRegistration> _byKind = new(StringComparer.Ordinal);

    public WorkerRegistry(IEnumerable<WorkerRegistration> registrations)
    {
        foreach (var registration in registrations)
        {
            var attribute = JobKinds.Get(registration.ArgsType);
            foreach (var kind in attribute.Aliases.Prepend(attribute.Kind))
            {
                if (_byKind.TryGetValue(kind, out var existing))
                {
                    throw new InvalidOperationException(
                        $"Job kind '{kind}' is registered twice: by '{existing.ArgsType.FullName}' and '{registration.ArgsType.FullName}'.");
                }

                _byKind[kind] = registration;
            }
        }
    }

    public bool IsEmpty => _byKind.Count == 0;

    public WorkerRegistration? Find(string kind) => _byKind.GetValueOrDefault(kind);
}

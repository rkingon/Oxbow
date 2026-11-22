using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Oxbow.Internal;

internal static class OxbowDiagnostics
{
    public const string Name = "Oxbow";

    private static readonly string? Version = typeof(OxbowDiagnostics).Assembly.GetName().Version?.ToString();
    private static readonly Meter Meter = new(Name, Version);
    private static volatile IReadOnlyList<(string Queue, string State, long Count)> queueDepth = [];

    public static ActivitySource ActivitySource { get; } = new(Name, Version);

    public static Counter<long> JobsInserted { get; } =
        Meter.CreateCounter<long>("oxbow.jobs.inserted", "{job}", "Jobs inserted, excluding unique duplicates.");

    public static Counter<long> JobsFinished { get; } =
        Meter.CreateCounter<long>("oxbow.jobs.attempts", "{attempt}", "Job attempts by outcome.");

    public static Histogram<double> JobDuration { get; } =
        Meter.CreateHistogram<double>("oxbow.job.duration", "s", "How long an attempt ran.");

    public static Histogram<double> JobQueueWait { get; } =
        Meter.CreateHistogram<double>("oxbow.job.queue_wait", "s", "Time from scheduled to started.");

    static OxbowDiagnostics()
    {
        Meter.CreateObservableGauge(
            "oxbow.queue.depth",
            () => queueDepth.Select(d => new Measurement<long>(
                d.Count,
                new KeyValuePair<string, object?>("queue", d.Queue),
                new KeyValuePair<string, object?>("state", d.State))),
            "{job}",
            "Jobs by queue and state, published by the elected leader.");
    }

    public static void SetQueueDepth(IReadOnlyList<(string Queue, string State, long Count)> depth) => queueDepth = depth;
}

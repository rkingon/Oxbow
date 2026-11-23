using System.Globalization;
using Cronos;

namespace Oxbow;

/// <summary>A job enqueued on a schedule by the elected leader.</summary>
/// <param name="Schedule">When it runs.</param>
/// <param name="CreateArgs">Creates the args for each run.</param>
/// <param name="Options">Insert options for each run.</param>
/// <param name="RunOnStart">Enqueue the first run at once, rather than at the schedule's first occurrence.</param>
public sealed record PeriodicJob(
    PeriodicSchedule Schedule,
    Func<IJobArgs> CreateArgs,
    InsertOptions? Options = null,
    bool RunOnStart = false)
{
    /// <summary>
    /// The name its run times are persisted under. Default: the kind and the schedule, so changing the schedule starts
    /// a fresh history.
    /// </summary>
    public string? Name { get; init; }
}

/// <summary>When a periodic job runs.</summary>
public abstract class PeriodicSchedule
{
    private protected PeriodicSchedule()
    {
    }

    /// <summary>The cron expression, or <see langword="null"/> for an interval schedule.</summary>
    public virtual string? Expression => null;

    /// <summary>The time zone a cron expression is evaluated in, or <see langword="null"/> for an interval schedule.</summary>
    public virtual TimeZoneInfo? TimeZone => null;

    /// <summary>The interval, or <see langword="null"/> for a cron schedule.</summary>
    public virtual TimeSpan? Interval => null;

    /// <summary>Runs every <paramref name="interval"/>.</summary>
    /// <param name="interval">The interval, at least one second.</param>
    /// <returns>The schedule.</returns>
    public static PeriodicSchedule Every(TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(interval, TimeSpan.FromSeconds(1));
        return new IntervalSchedule(interval);
    }

    /// <summary>Runs on a cron expression: five fields, or six with seconds first.</summary>
    /// <param name="expression">The cron expression.</param>
    /// <param name="timeZone">The time zone it's evaluated in. Default UTC.</param>
    /// <returns>The schedule.</returns>
    public static PeriodicSchedule Cron(string expression, TimeZoneInfo? timeZone = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);
        var format = expression.Split(' ', StringSplitOptions.RemoveEmptyEntries).Length == 6
            ? CronFormat.IncludeSeconds
            : CronFormat.Standard;
        return new CronSchedule(expression, CronExpression.Parse(expression, format), timeZone ?? TimeZoneInfo.Utc);
    }

    /// <summary>The next run strictly after <paramref name="from"/>.</summary>
    /// <param name="from">The reference time.</param>
    /// <returns>The next run time, or <see langword="null"/> if there is none.</returns>
    public abstract DateTimeOffset? GetNextOccurrence(DateTimeOffset from);

    internal static PeriodicSchedule FromStored(string? cron, string? timeZoneId, double? everySeconds) =>
        cron is not null
            ? Cron(cron, TimeZoneInfo.FindSystemTimeZoneById(timeZoneId ?? "UTC"))
            : Every(TimeSpan.FromSeconds(everySeconds ?? throw new InvalidOperationException("A stored schedule has neither a cron expression nor an interval.")));

    private sealed class IntervalSchedule(TimeSpan interval) : PeriodicSchedule
    {
        public override TimeSpan? Interval => interval;

        public override DateTimeOffset? GetNextOccurrence(DateTimeOffset from) => from + interval;

        public override string ToString() => $"every {interval.ToString("c", CultureInfo.InvariantCulture)}";
    }

    private sealed class CronSchedule(string text, CronExpression expression, TimeZoneInfo timeZone) : PeriodicSchedule
    {
        public override string Expression => text;

        public override TimeZoneInfo TimeZone => timeZone;

        public override DateTimeOffset? GetNextOccurrence(DateTimeOffset from) => expression.GetNextOccurrence(from, timeZone);

        public override string ToString() =>
            timeZone.Id == TimeZoneInfo.Utc.Id ? $"cron {text}" : $"cron {text} {timeZone.Id}";
    }
}

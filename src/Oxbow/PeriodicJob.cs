using Cronos;

namespace Oxbow;

/// <summary>A job enqueued on a schedule by the elected leader.</summary>
/// <param name="Schedule">When it runs.</param>
/// <param name="CreateArgs">Creates the args for each run.</param>
/// <param name="Options">Insert options for each run.</param>
/// <param name="RunOnStart">Also enqueue it when the leader starts the periodic enqueuer.</param>
public sealed record PeriodicJob(
    PeriodicSchedule Schedule,
    Func<IJobArgs> CreateArgs,
    InsertOptions? Options = null,
    bool RunOnStart = false);

/// <summary>When a periodic job runs.</summary>
public abstract class PeriodicSchedule
{
    private protected PeriodicSchedule()
    {
    }

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
        return new CronSchedule(CronExpression.Parse(expression, format), timeZone ?? TimeZoneInfo.Utc);
    }

    /// <summary>The next run strictly after <paramref name="from"/>.</summary>
    /// <param name="from">The reference time.</param>
    /// <returns>The next run time, or <see langword="null"/> if there is none.</returns>
    public abstract DateTimeOffset? GetNextOccurrence(DateTimeOffset from);

    private sealed class IntervalSchedule(TimeSpan interval) : PeriodicSchedule
    {
        public override DateTimeOffset? GetNextOccurrence(DateTimeOffset from) => from + interval;
    }

    private sealed class CronSchedule(CronExpression expression, TimeZoneInfo timeZone) : PeriodicSchedule
    {
        public override DateTimeOffset? GetNextOccurrence(DateTimeOffset from) => expression.GetNextOccurrence(from, timeZone);
    }
}

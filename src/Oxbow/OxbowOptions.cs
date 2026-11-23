using System.Text.Json;

namespace Oxbow;

/// <summary>Configures Oxbow. Validated on start.</summary>
public sealed class OxbowOptions
{
    /// <summary>The Postgres schema holding Oxbow's objects. Default <c>public</c>.</summary>
    public string Schema { get; set; } = "public";

    /// <summary>The prefix on every object Oxbow creates. Default <c>oxbow_</c>.</summary>
    public string TablePrefix { get; set; } = "oxbow_";

    /// <summary>
    /// Queues this node works, by name. Leave empty for an insert-only client.
    /// </summary>
    public IDictionary<string, QueueOptions> Queues { get; } = new Dictionary<string, QueueOptions>(StringComparer.Ordinal);

    /// <summary>Periodic jobs, enqueued by the elected leader.</summary>
    public IList<PeriodicJob> PeriodicJobs { get; } = [];

    /// <summary>Default attempts before a failing job is discarded. Default 25.</summary>
    public int MaxAttempts { get; set; } = 25;

    /// <summary>Default job timeout. <see cref="Timeout.InfiniteTimeSpan"/> disables it. Default 1 minute.</summary>
    public TimeSpan JobTimeout { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Minimum time between fetches in one queue. Default 100 ms.</summary>
    public TimeSpan FetchCooldown { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>How often a queue polls for jobs without being notified. Default 1 second.</summary>
    public TimeSpan FetchPollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Don't use LISTEN/NOTIFY; rely on polling only. Needed behind PgBouncer in transaction mode.
    /// </summary>
    public bool PollOnly { get; set; }

    /// <summary>How often the leader makes due scheduled and retryable jobs available. Default 5 seconds.</summary>
    public TimeSpan SchedulerInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How long a job may stay running before the rescuer retries it. Default 1 hour.</summary>
    public TimeSpan RescueStuckJobsAfter { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// How long completed jobs are kept. <see cref="Timeout.InfiniteTimeSpan"/> keeps them forever. Default 24 hours.
    /// </summary>
    public TimeSpan CompletedJobRetention { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// How long cancelled jobs are kept. <see cref="Timeout.InfiniteTimeSpan"/> keeps them forever. Default 24 hours.
    /// </summary>
    public TimeSpan CancelledJobRetention { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// How long discarded jobs are kept. <see cref="Timeout.InfiniteTimeSpan"/> keeps them forever. Default 7 days.
    /// </summary>
    public TimeSpan DiscardedJobRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// How long a finished batch is kept, and with it its members, which the job cleaner leaves alone while their batch
    /// exists. A batch whose callbacks haven't finished is kept regardless. <see cref="Timeout.InfiniteTimeSpan"/> keeps
    /// batches forever. Default 24 hours.
    /// </summary>
    public TimeSpan BatchRetention { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// Allow inserting kinds that have no worker registered on this client. Only consulted when workers are registered;
    /// insert-only clients skip the check.
    /// </summary>
    public bool SkipUnknownKindCheck { get; set; }

    /// <summary>
    /// Serializer options for args. Set <see cref="JsonSerializerOptions.TypeInfoResolver"/> to a
    /// <c>JsonSerializerContext</c> for source-generated serialization.
    /// </summary>
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// The DI key of the <c>NpgsqlDataSource</c> to use. Default: the unkeyed registration.
    /// </summary>
    public object? DataSourceServiceKey { get; set; }

    /// <summary>
    /// Encrypt every kind's args at rest, not only those marked <see cref="EncryptArgsAttribute"/>. Needs an
    /// <see cref="IJobArgsProtector"/>, e.g. <c>AddArgsEncryption()</c> from <c>Oxbow.DataProtection</c>.
    /// </summary>
    public bool EncryptAllArgs { get; set; }

    /// <summary>This node's ID, recorded in <see cref="JobRow.AttemptedBy"/>. Default: host name plus a random suffix.</summary>
    public string? ClientId { get; set; }

    // Maintenance cadences, internal so tests can shorten them.
    internal TimeSpan RescuerInterval { get; set; } = TimeSpan.FromSeconds(30);

    internal TimeSpan CleanerInterval { get; set; } = TimeSpan.FromSeconds(30);

    internal TimeSpan ElectInterval { get; set; } = TimeSpan.FromSeconds(5);

    internal TimeSpan BatchFinisherInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Registers a periodic job.</summary>
    /// <param name="schedule">When it runs.</param>
    /// <param name="createArgs">Creates the args for each run.</param>
    /// <param name="options">Insert options for each run.</param>
    /// <param name="runOnStart">Enqueue the first run at once, rather than at the schedule's first occurrence.</param>
    /// <returns>These options.</returns>
    public OxbowOptions AddPeriodicJob(
        PeriodicSchedule schedule,
        Func<IJobArgs> createArgs,
        InsertOptions? options = null,
        bool runOnStart = false)
    {
        PeriodicJobs.Add(new PeriodicJob(schedule, createArgs, options, runOnStart));
        return this;
    }

    /// <summary>Registers a periodic job whose run times are persisted under <paramref name="name"/>.</summary>
    /// <param name="name">A stable name, unique among periodic jobs. Keeps the job's history across schedule changes.</param>
    /// <param name="schedule">When it runs.</param>
    /// <param name="createArgs">Creates the args for each run.</param>
    /// <param name="options">Insert options for each run.</param>
    /// <param name="runOnStart">Enqueue the first run at once, rather than at the schedule's first occurrence.</param>
    /// <returns>These options.</returns>
    public OxbowOptions AddPeriodicJob(
        string name,
        PeriodicSchedule schedule,
        Func<IJobArgs> createArgs,
        InsertOptions? options = null,
        bool runOnStart = false)
    {
        PeriodicJobs.Add(new PeriodicJob(schedule, createArgs, options, runOnStart) { Name = name });
        return this;
    }
}

/// <summary>Settings for one queue.</summary>
public sealed class QueueOptions
{
    /// <summary>The most jobs this node works at once in the queue. Default 100.</summary>
    public int MaxWorkers { get; set; } = 100;

    /// <summary>
    /// Per-key limits on running jobs, keyed by <see cref="InsertOptions.PartitionKey"/>. Default: unlimited.
    /// </summary>
    public ConcurrencyLimit? ConcurrencyLimit { get; set; }

    /// <summary>
    /// Caps job starts over time for the whole queue, across every node working it. Configure the same value on every
    /// node. Default: unlimited.
    /// </summary>
    public RateLimit? RateLimit { get; set; }

    /// <summary>
    /// Caps job starts over time per <see cref="InsertOptions.PartitionKey"/>, across every node working the queue. Jobs
    /// without a key aren't limited by it. Default: unlimited.
    /// </summary>
    public RateLimit? PartitionRateLimit { get; set; }
}

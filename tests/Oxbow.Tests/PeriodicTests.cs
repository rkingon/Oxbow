using System.Text.Json;
using System.Text.Json.Nodes;
using Oxbow.Internal;

namespace Oxbow.Tests;

public sealed class PeriodicTests
{
    [Fact]
    public void Schedules_describe_themselves_and_round_trip_through_storage()
    {
        var every = PeriodicSchedule.Every(TimeSpan.FromMinutes(15));
        Assert.Equal("every 00:15:00", every.ToString());
        Assert.Equal(TimeSpan.FromMinutes(15), PeriodicSchedule.FromStored(null, null, 900).Interval);

        var london = TimeZoneInfo.FindSystemTimeZoneById("Europe/London");
        var cron = PeriodicSchedule.Cron("0 3 * * *", london);
        Assert.Equal("cron 0 3 * * * Europe/London", cron.ToString());
        Assert.Equal("cron */5 * * * * *", PeriodicSchedule.Cron("*/5 * * * * *").ToString());

        var stored = PeriodicSchedule.FromStored(cron.Expression, cron.TimeZone!.Id, null);
        var from = new DateTimeOffset(2030, 7, 1, 0, 0, 0, TimeSpan.Zero);
        Assert.Equal(cron.GetNextOccurrence(from), stored.GetNextOccurrence(from));
        Assert.Equal(new DateTimeOffset(2030, 7, 1, 2, 0, 0, TimeSpan.Zero), stored.GetNextOccurrence(from));
    }

    [Fact]
    public void Derived_names_combine_kind_and_schedule()
    {
        Assert.Equal("report every 01:00:00", PeriodicJobNames.Derive("report", PeriodicSchedule.Every(TimeSpan.FromHours(1))));
        Assert.False(PeriodicJobNames.IsValid(" "));
        Assert.False(PeriodicJobNames.IsValid(new string('x', 256)));
    }

    [Fact]
    public void Options_validation_rejects_bad_and_duplicate_periodic_job_names()
    {
        var options = new OxbowOptions();
        options.AddPeriodicJob("a", PeriodicSchedule.Every(TimeSpan.FromMinutes(1)), () => throw new InvalidOperationException());
        options.AddPeriodicJob("a", PeriodicSchedule.Every(TimeSpan.FromMinutes(2)), () => throw new InvalidOperationException());
        options.AddPeriodicJob(" ", PeriodicSchedule.Every(TimeSpan.FromMinutes(1)), () => throw new InvalidOperationException());
        options.AddPeriodicJob(PeriodicSchedule.Every(TimeSpan.FromMinutes(1)), () => throw new InvalidOperationException());

        var result = new OxbowOptionsValidator().Validate(null, options);
        Assert.Equal(2, result.Failures!.Count());
    }

    [Fact]
    public void Stored_insert_options_round_trip()
    {
        var options = new InsertOptions
        {
            Queue = "reports",
            Priority = 3,
            MaxAttempts = 4,
            Tags = ["a", "b"],
            Metadata = new JsonObject { ["team"] = "ops" },
            Unique = new UniqueOptions { ByPeriod = TimeSpan.FromHours(1), ByState = [JobState.Available, JobState.Pending, JobState.Running, JobState.Scheduled] },
        };

        var json = JsonSerializer.Serialize(options, PeriodicJsonContext.Default.InsertOptions);
        Assert.Contains("\"max_attempts\":4", json, StringComparison.Ordinal);
        Assert.Contains("\"available\"", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("scheduled_at", json, StringComparison.Ordinal);

        var read = JsonSerializer.Deserialize(json, PeriodicJsonContext.Default.InsertOptions)!;
        Assert.Equal(options.Queue, read.Queue);
        Assert.Equal(options.Priority, read.Priority);
        Assert.Equal(options.MaxAttempts, read.MaxAttempts);
        Assert.Equal(options.Tags, read.Tags!);
        Assert.Equal("ops", read.Metadata!["team"]!.GetValue<string>());
        Assert.Equal(options.Unique.ByPeriod, read.Unique!.ByPeriod);
        Assert.Equal(options.Unique.ByState, read.Unique.ByState!);
    }
}

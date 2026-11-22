using System.Text.Json;
using Microsoft.Extensions.Time.Testing;
using Oxbow.Internal;

namespace Oxbow.Tests;

public sealed class MiscTests
{
    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 16)]
    [InlineData(4, 625)]
    public void Default_retry_policy_is_attempt_to_the_fourth_with_ten_percent_jitter(int priorErrors, double baseSeconds)
    {
        var time = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var policy = new DefaultRetryPolicy(time);
        var job = Row(Enumerable.Range(1, priorErrors).Select(i => new AttemptError(time.GetUtcNow(), i, "x", null)).ToArray());

        for (var i = 0; i < 100; i++)
        {
            var delay = (policy.NextRetry(job) - time.GetUtcNow()).TotalSeconds;
            Assert.InRange(delay, baseSeconds * 0.9, baseSeconds * 1.1);
        }
    }

    [Fact]
    public void Migrations_render_schema_and_prefix_and_run_in_order()
    {
        var up = OxbowSchema.GetMigrations(0, 1, "app", "q_");
        var down = OxbowSchema.GetMigrations(1, 0, "app", "q_");
        Assert.Single(up);
        Assert.True(up[0].Up);
        Assert.Contains("CREATE TABLE app.q_job", up[0].Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("{{", up[0].Sql, StringComparison.Ordinal);
        Assert.False(down[0].Up);
        Assert.Contains("DROP TABLE app.q_job", down[0].Sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Public", "oxbow_")]
    [InlineData("public", "Oxbow_")]
    [InlineData("public; drop table x", "oxbow_")]
    [InlineData("a_very_long_schema_name_that_goes_on", "and_a_long_prefix_too_")]
    public void Unsafe_or_overlong_identifiers_are_rejected(string schema, string prefix) =>
        Assert.Throws<ArgumentException>(() => OxbowSchema.GetMigrations(0, 1, schema, prefix));

    [Fact]
    public void Grants_quote_the_role()
    {
        var grants = OxbowSchema.GetGrants("app\"role");
        Assert.Contains("TO \"app\"\"role\"", grants, StringComparison.Ordinal);
    }

    [Fact]
    public void Options_validation_reports_every_problem()
    {
        var options = new OxbowOptions { TablePrefix = "Bad-", MaxAttempts = 0, JobTimeout = TimeSpan.Zero };
        options.Queues["*"] = new QueueOptions { MaxWorkers = 0 };
        var result = new OxbowOptionsValidator().Validate(null, options);
        Assert.True(result.Failed);
        Assert.Equal(5, result.Failures!.Count());
    }

    [Fact]
    public void Cron_schedules_honor_five_and_six_fields()
    {
        var from = new DateTimeOffset(2030, 1, 1, 10, 0, 30, TimeSpan.Zero);
        Assert.Equal(from.AddSeconds(30), PeriodicSchedule.Cron("0 * * * *").GetNextOccurrence(from)!.Value.AddMinutes(-59));
        Assert.Equal(from.AddSeconds(5), PeriodicSchedule.Cron("*/5 * * * * *").GetNextOccurrence(from));
        Assert.Equal(from.AddMinutes(5), PeriodicSchedule.Every(TimeSpan.FromMinutes(5)).GetNextOccurrence(from));
    }

    [Fact]
    public void Args_without_a_kind_attribute_are_rejected_with_the_type_name()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => JobKinds.Get(typeof(NoKind)));
        Assert.Contains(nameof(NoKind), ex.Message, StringComparison.Ordinal);
    }

    private sealed record NoKind : IJobArgs;

    private static JobRow Row(AttemptError[] errors) => new()
    {
        Id = 1,
        State = JobState.Running,
        Attempt = errors.Length + 1,
        MaxAttempts = 25,
        AttemptedBy = [],
        CreatedAt = DateTimeOffset.UnixEpoch,
        ScheduledAt = DateTimeOffset.UnixEpoch,
        Priority = 1,
        Args = JsonDocument.Parse("{}").RootElement,
        Errors = errors,
        Kind = "k",
        Metadata = JsonDocument.Parse("{}").RootElement,
        Queue = "default",
        Tags = [],
    };
}

using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Oxbow.Testing;

namespace Oxbow.Tests;

public sealed class ProgressTests
{
    [JobKind("import")]
    public sealed record Import(int Rows) : IJobArgs;

    public sealed class ImportWorker : IWorker<Import>
    {
        public async Task WorkAsync(Job<Import> job, CancellationToken cancellationToken)
        {
            await job.HeartbeatAsync(cancellationToken);
            await job.ReportProgressAsync(50, "halfway", cancellationToken);
            await job.ReportProgressAsync(100, cancellationToken: cancellationToken);
        }
    }

    [Fact]
    public async Task The_harness_reports_the_last_progress_and_ignores_heartbeats()
    {
        var harness = new WorkerHarness<Import, ImportWorker>(new ServiceCollection().BuildServiceProvider());

        var result = await harness.WorkAsync(new Import(10), attempt: 2);

        Assert.Equal(WorkOutcome.Completed, result.Outcome);
        Assert.Equal(100, result.Progress!.Percent);
        Assert.Null(result.Progress.Message);
        Assert.Equal(2, result.Progress.Attempt);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(100.5)]
    [InlineData(double.NaN)]
    public async Task Progress_outside_0_to_100_throws(double percent)
    {
        var job = new Job<Import>(Row("{}"), new Import(1));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => job.ReportProgressAsync(percent));
        Assert.Null(job.Progress);
    }

    [Fact]
    public void Heartbeat_and_progress_are_read_from_metadata()
    {
        var row = Row("""
            {"oxbow:heartbeat_at": "2026-11-23T10:00:00.123456+00:00",
             "oxbow:progress": {"percent": 12.5, "message": "m", "attempt": 3, "at": "2026-11-23T09:59:00+00:00"}}
            """);

        Assert.Equal(new DateTimeOffset(2026, 11, 23, 10, 0, 0, TimeSpan.Zero).AddTicks(1_234_560), row.HeartbeatAt);
        Assert.Equal(
            new JobProgress(12.5, "m", 3, new DateTimeOffset(2026, 11, 23, 9, 59, 0, TimeSpan.Zero)),
            row.Progress);
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("""{"oxbow:heartbeat_at": "soon", "oxbow:progress": {"percent": "lots"}}""")]
    [InlineData("""{"oxbow:heartbeat_at": 12, "oxbow:progress": "half"}""")]
    public void Missing_or_malformed_heartbeat_and_progress_read_as_null(string metadata)
    {
        var row = Row(metadata);
        Assert.Null(row.HeartbeatAt);
        Assert.Null(row.Progress);
    }

    private static JobRow Row(string metadata) => new()
    {
        Id = 1,
        State = JobState.Running,
        Attempt = 1,
        MaxAttempts = 25,
        AttemptedBy = [],
        CreatedAt = DateTimeOffset.UnixEpoch,
        ScheduledAt = DateTimeOffset.UnixEpoch,
        Priority = 1,
        Args = JsonDocument.Parse("{}").RootElement,
        Errors = [],
        Kind = "import",
        Metadata = JsonDocument.Parse(metadata).RootElement,
        Queue = "default",
        Tags = [],
    };
}

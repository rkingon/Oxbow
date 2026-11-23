using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Oxbow.Processing;
using Oxbow.Testing;

namespace Oxbow.Tests;

public sealed class OutputAndDiscardTests
{
    [JobKind("sum")]
    public sealed record Sum(int[] Values, string Mode = "ok") : IJobArgs;

    public sealed record SumResult(int Total);

    public sealed class SumWorker : IWorker<Sum>
    {
        public Task WorkAsync(Job<Sum> job, CancellationToken cancellationToken)
        {
            if (job.Args.Mode == "big")
            {
                job.SetOutput(new string('x', JobRow.MaxOutputSize));
                return Task.CompletedTask;
            }

            job.SetOutput(new SumResult(job.Args.Values.Sum()));
            return job.Args.Mode == "fail" ? throw new InvalidOperationException("After output.") : Task.CompletedTask;
        }
    }

    private static WorkerHarness<Sum, SumWorker> Harness() => new(new ServiceCollection().BuildServiceProvider());

    [Fact]
    public async Task The_harness_returns_the_output_as_Oxbow_would_store_it()
    {
        var result = await Harness().WorkAsync(new Sum([1, 2, 3]));

        Assert.Equal(WorkOutcome.Completed, result.Outcome);
        Assert.Equal(6, result.Output!.Value.GetProperty("total").GetInt32());
    }

    [Fact]
    public async Task The_harness_fails_an_output_over_the_limit()
    {
        var result = await Harness().WorkAsync(new Sum([], "big"));

        Assert.Equal(WorkOutcome.Errored, result.Outcome);
        Assert.Contains("byte limit", result.Exception!.Message, StringComparison.Ordinal);
        Assert.Null(result.Output);
    }

    [Fact]
    public async Task The_harness_drops_the_output_of_a_failed_attempt()
    {
        var result = await Harness().WorkAsync(new Sum([1], "fail"));

        Assert.Equal(WorkOutcome.Errored, result.Outcome);
        Assert.Null(result.Output);
    }

    [Fact]
    public void A_row_reads_its_output_from_metadata()
    {
        var with = Row("""{"output": {"total": 6}, "other": 1}""");
        var without = Row("""{"other": 1}""");

        Assert.Equal(new SumResult(6), with.GetOutput<SumResult>());
        Assert.Null(without.Output);
        Assert.Null(without.GetOutput<SumResult>());

        var custom = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        Assert.Equal(new SumResult(6), with.GetOutput<SumResult>(custom));
        Assert.False(custom.IsReadOnly);
    }

    [Theory]
    [InlineData(JobState.Discarded, JobState.Discarded, JobDiscardReason.AttemptsExhausted)]
    [InlineData(JobState.Cancelled, JobState.Cancelled, JobDiscardReason.WorkerCancelled)]
    [InlineData(JobState.Retryable, JobState.Cancelled, JobDiscardReason.CancelRequested)]
    [InlineData(JobState.Available, JobState.Cancelled, JobDiscardReason.CancelRequested)]
    [InlineData(JobState.Scheduled, JobState.Cancelled, JobDiscardReason.CancelRequested)]
    [InlineData(JobState.Retryable, JobState.Retryable, null)]
    [InlineData(JobState.Scheduled, JobState.Discarded, null)]
    [InlineData(JobState.Discarded, null, null)]
    public void Executor_outcomes_map_to_discards_by_the_state_written(JobState requested, JobState? written, JobDiscardReason? expected) =>
        Assert.Equal(expected, DiscardHooks.Classify(requested, written));

    private static JobRow Row(string metadata) => new()
    {
        Id = 1,
        State = JobState.Completed,
        Attempt = 1,
        MaxAttempts = 25,
        AttemptedBy = [],
        CreatedAt = DateTimeOffset.UnixEpoch,
        ScheduledAt = DateTimeOffset.UnixEpoch,
        Priority = 1,
        Args = JsonDocument.Parse("{}").RootElement,
        Errors = [],
        Kind = "sum",
        Metadata = JsonDocument.Parse(metadata).RootElement,
        Queue = "default",
        Tags = [],
    };
}

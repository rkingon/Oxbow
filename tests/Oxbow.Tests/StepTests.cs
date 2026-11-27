using System.Text.Json;
using Oxbow.Testing;

namespace Oxbow.Tests;

public sealed class StepTests
{
    [JobKind("turn")]
    public sealed record Turn(int Id) : IJobArgs;

    public sealed record Reply(string Text);

    [Fact]
    public async Task A_recorded_step_is_returned_without_running_and_a_new_one_is_recorded()
    {
        var runtime = new RecordingJobRuntime();
        runtime.RecordStep("ask", new Reply("recorded"));
        var job = new Job<Turn>(Row(), new Turn(1), runtime);
        var ran = new List<string>();

        var asked = await job.StepAsync("ask", _ =>
        {
            ran.Add("ask");
            return Task.FromResult(new Reply("fresh"));
        });
        var answered = await job.StepAsync("answer", _ =>
        {
            ran.Add("answer");
            return Task.FromResult(42);
        });

        Assert.Equal("recorded", asked.Text);
        Assert.Equal(42, answered);
        Assert.Equal(["answer"], ran);
        Assert.Equal(42, runtime.Steps["answer"].GetInt32());
    }

    [Fact]
    public async Task A_step_without_a_result_records_null()
    {
        var runtime = new RecordingJobRuntime();
        var job = new Job<Turn>(Row(), new Turn(1), runtime);

        await job.StepAsync("notify", _ => Task.CompletedTask);

        Assert.Equal(JsonValueKind.Null, runtime.Steps["notify"].ValueKind);
    }

    [Fact]
    public async Task A_name_runs_once_per_execution_unless_its_step_failed()
    {
        var job = new Job<Turn>(Row(), new Turn(1), new RecordingJobRuntime());

        await Assert.ThrowsAsync<TimeoutException>(() => job.StepAsync<int>("call", _ => throw new TimeoutException()));
        Assert.Equal(2, await job.StepAsync("call", _ => Task.FromResult(2)));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => job.StepAsync("call", _ => Task.FromResult(3)));
        Assert.Contains("'call'", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public async Task A_blank_step_name_throws(string name)
    {
        var job = new Job<Turn>(Row(), new Turn(1));
        await Assert.ThrowsAsync<ArgumentException>(() => job.StepAsync(name, _ => Task.FromResult(1)));
    }

    [Fact]
    public async Task A_step_name_may_be_at_most_255_characters()
    {
        var job = new Job<Turn>(Row(), new Turn(1));
        Assert.Equal(1, await job.StepAsync(new string('s', 255), _ => Task.FromResult(1)));
        await Assert.ThrowsAsync<ArgumentException>(() => job.StepAsync(new string('s', 256), _ => Task.FromResult(1)));
    }

    [Fact]
    public async Task Without_a_runtime_a_step_just_runs_and_locking_does_nothing()
    {
        var job = new Job<Turn>(Row(), new Turn(1));

        Assert.Equal(7, await job.StepAsync("only", _ => Task.FromResult(7)));
        await job.LockAttemptAsync(new NoTransaction());
    }

    [Fact]
    public async Task A_runtime_that_predates_steps_and_locks_refuses_them_rather_than_skipping_them()
    {
        var job = new Job<Turn>(Row(), new Turn(1), new HeartbeatOnlyRuntime());
        var ran = false;

        await Assert.ThrowsAsync<NotSupportedException>(() => job.StepAsync("call", _ =>
        {
            ran = true;
            return Task.FromResult(1);
        }));
        await Assert.ThrowsAsync<NotSupportedException>(() => job.LockAttemptAsync(new NoTransaction()));
        Assert.False(ran);
    }

    private sealed class HeartbeatOnlyRuntime : IJobRuntime
    {
        public DateTimeOffset UtcNow => DateTimeOffset.UnixEpoch;

        public Task HeartbeatAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task ReportProgressAsync(double percent, string? message, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    [Fact]
    public async Task The_recording_runtime_counts_attempt_locks()
    {
        var runtime = new RecordingJobRuntime();
        var job = new Job<Turn>(Row(), new Turn(1), runtime);

        await job.LockAttemptAsync(new NoTransaction());

        Assert.Equal(1, runtime.AttemptLocks);
    }

    [Fact]
    public async Task A_step_result_over_the_output_limit_fails_the_step_and_records_nothing()
    {
        var runtime = new RecordingJobRuntime();
        var job = new Job<Turn>(Row(), new Turn(1), runtime);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            job.StepAsync("big", _ => Task.FromResult(new string('x', JobRow.MaxOutputSize))));

        Assert.Empty(runtime.Steps);
    }

    private sealed class NoTransaction : System.Data.Common.DbTransaction
    {
        public override System.Data.IsolationLevel IsolationLevel => System.Data.IsolationLevel.ReadCommitted;

        protected override System.Data.Common.DbConnection? DbConnection => null;

        public override void Commit()
        {
        }

        public override void Rollback()
        {
        }
    }

    private static JobRow Row() => new()
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
        Kind = "turn",
        Metadata = JsonDocument.Parse("{}").RootElement,
        Queue = "default",
        Tags = [],
    };
}

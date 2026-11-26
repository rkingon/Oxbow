using Microsoft.Extensions.DependencyInjection;
using Oxbow.Testing;

namespace Oxbow.Tests;

public sealed class WorkerHarnessTests
{
    [JobKind("greet")]
    public sealed record Greet(string Name) : IJobArgs;

    public sealed class Greetings
    {
        public List<string> Sent { get; } = [];
    }

    public sealed class GreetWorker(Greetings greetings) : IWorker<Greet>
    {
        public Task WorkAsync(Job<Greet> job, CancellationToken cancellationToken)
        {
            switch (job.Args.Name)
            {
                case "later":
                    throw new JobSnoozeException(TimeSpan.FromMinutes(5));
                case "never":
                    throw new JobCancelException("Never greet.");
                case "throttled":
                    throw new JobRetryException(TimeSpan.FromSeconds(30), "greet.rate_limited: retry after 30s");
                case "slow":
                    return Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                case "broken":
                    throw new InvalidOperationException("Broken.");
                default:
                    greetings.Sent.Add(job.Args.Name);
                    return Task.CompletedTask;
            }
        }

        public TimeSpan? GetTimeout(Job<Greet> job) => job.Args.Name == "slow" ? TimeSpan.FromMilliseconds(50) : null;
    }

    private sealed class Tag(Greetings greetings) : IJobWorkMiddleware
    {
        public Task InvokeAsync(JobContext context, JobWorkDelegate next)
        {
            greetings.Sent.Add("middleware");
            return next(context);
        }
    }

    [Theory]
    [InlineData("ada", WorkOutcome.Completed)]
    [InlineData("later", WorkOutcome.Snoozed)]
    [InlineData("never", WorkOutcome.Cancelled)]
    [InlineData("slow", WorkOutcome.TimedOut)]
    [InlineData("broken", WorkOutcome.Errored)]
    [InlineData("throttled", WorkOutcome.Errored)]
    public async Task Outcomes_match_what_Oxbow_would_record(string name, WorkOutcome expected)
    {
        var services = new ServiceCollection()
            .AddSingleton<Greetings>()
            .AddScoped<IJobWorkMiddleware, Tag>()
            .BuildServiceProvider();
        var harness = new WorkerHarness<Greet, GreetWorker>(services);

        var result = await harness.WorkAsync(new Greet(name));

        Assert.Equal(expected, result.Outcome);
        Assert.Equal("middleware", services.GetRequiredService<Greetings>().Sent[0]);
    }

    [Theory]
    [InlineData("throttled", 30)]
    [InlineData("broken", null)]
    public async Task A_worker_chosen_retry_delay_is_reported(string name, int? seconds)
    {
        var services = new ServiceCollection().AddSingleton<Greetings>().BuildServiceProvider();
        var harness = new WorkerHarness<Greet, GreetWorker>(services);

        var result = await harness.WorkAsync(new Greet(name));

        Assert.Equal(WorkOutcome.Errored, result.Outcome);
        Assert.Equal(seconds is { } s ? TimeSpan.FromSeconds(s) : null, result.RetryDelay);
    }

    [Fact]
    public void A_retry_delay_may_be_zero_but_not_negative()
    {
        Assert.Equal(TimeSpan.Zero, new JobRetryException(TimeSpan.Zero, "now").Delay);
        Assert.Throws<ArgumentOutOfRangeException>(() => new JobRetryException(TimeSpan.FromTicks(-1), "never"));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new JobRetryException(TimeSpan.FromTicks(-1), "never", new InvalidOperationException()));
    }
}

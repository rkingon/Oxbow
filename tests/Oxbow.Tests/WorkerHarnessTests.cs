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
}

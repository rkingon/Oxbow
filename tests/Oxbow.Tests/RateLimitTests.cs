using Oxbow.Internal;

namespace Oxbow.Tests;

public sealed class RateLimitTests
{
    [Theory]
    [InlineData(1, 1_000, 1_000_000)]
    [InlineData(20, 1_000, 50_000)]
    [InlineData(3, 1_000, 333_334)]
    public void The_emission_interval_rounds_up_to_a_microsecond(int limit, int periodMilliseconds, long expected)
    {
        Assert.Equal(expected, new RateLimit(limit, TimeSpan.FromMilliseconds(periodMilliseconds)).EmissionIntervalMicroseconds);
    }

    [Fact]
    public void Invalid_limits_are_rejected()
    {
        var options = new OxbowOptions();
        options.Queues["limit"] = new QueueOptions { RateLimit = new(0, TimeSpan.FromSeconds(1)) };
        options.Queues["burst"] = new QueueOptions { PartitionRateLimit = new(1, TimeSpan.FromSeconds(1)) { Burst = 0 } };
        options.Queues["period"] = new QueueOptions { RateLimit = new(1, TimeSpan.Zero) };
        options.Queues["too-fast"] = new QueueOptions { RateLimit = new(2_000_000, TimeSpan.FromSeconds(1)) };
        options.Queues["too-long"] = new QueueOptions { RateLimit = new(1, TimeSpan.FromDays(1)) { Burst = 400 } };
        options.Queues["ok"] = new QueueOptions
        {
            RateLimit = new(1_000_000, TimeSpan.FromSeconds(1)) { Burst = 1_000 },
            PartitionRateLimit = new(1, TimeSpan.FromDays(365)),
        };

        var result = new OxbowOptionsValidator().Validate(null, options);

        Assert.Equal(["burst", "limit", "period", "too-fast", "too-long"],
            result.Failures!.Select(f => f.Split('\'')[1]).Order(StringComparer.Ordinal));
    }
}

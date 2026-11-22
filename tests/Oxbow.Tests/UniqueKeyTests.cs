using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Oxbow.Internal;

namespace Oxbow.Tests;

public sealed class UniqueKeyTests
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };
    private static readonly DateTimeOffset At = new(2030, 1, 1, 10, 30, 0, TimeSpan.Zero);

    [JobKind("email")]
    private sealed record Email([property: UniqueKey] string To, [property: UniqueKey] string Subject, string Body) : IJobArgs;

    [JobKind("plain")]
    private sealed record Plain(string B, string A) : IJobArgs;

    [JobKind("nested")]
    private sealed record Nested(Customer Customer, string TraceId) : IJobArgs;

    private sealed record Customer([property: UniqueKey] string Id, string Name);

    private static byte[] Key<T>(T args, UniqueOptions options, string queue = "default")
        where T : IJobArgs =>
        UniqueKeys.Build(options, JobKinds.Get(typeof(T)).Kind, JsonSerializer.Serialize(args, Json), Json.GetTypeInfo(typeof(T)), queue, At);

    private static byte[] Hash(string key) => SHA256.HashData(Encoding.UTF8.GetBytes(key));

    [Fact]
    public void By_args_uses_only_marked_properties_sorted()
    {
        var key = Key(new Email("a@b.c", "Hi", "ignored"), new UniqueOptions { ByArgs = true });
        Assert.Equal(Hash("&kind=email&args={\"subject\":\"Hi\",\"to\":\"a@b.c\"}"), key);
    }

    [Fact]
    public void By_args_without_marked_properties_uses_the_whole_object_sorted()
    {
        var key = Key(new Plain("b", "a"), new UniqueOptions { ByArgs = true });
        Assert.Equal(Hash("&kind=plain&args={\"a\":\"a\",\"b\":\"b\"}"), key);
    }

    [Fact]
    public void By_args_recurses_into_nested_objects()
    {
        var key = Key(new Nested(new Customer("42", "ignored"), "ignored"), new UniqueOptions { ByArgs = true });
        Assert.Equal(Hash("&kind=nested&args={\"customer\":{\"id\":\"42\"}}"), key);
    }

    [Fact]
    public void By_period_truncates_and_by_queue_appends()
    {
        var key = Key(new Plain("b", "a"), new UniqueOptions { ByPeriod = TimeSpan.FromHours(1), ByQueue = true }, "mail");
        Assert.Equal(Hash("&kind=plain&period=2030-01-01T10:00:00Z&queue=mail"), key);
    }

    [Fact]
    public void Default_states_exclude_cancelled_and_discarded()
    {
        var bitmask = UniqueKeys.StatesBitmask(new UniqueOptions { ByArgs = true });
        Assert.Equal(0, bitmask & JobStates.Bit(JobState.Cancelled));
        Assert.Equal(0, bitmask & JobStates.Bit(JobState.Discarded));
        Assert.NotEqual(0, bitmask & JobStates.Bit(JobState.Completed));
    }

    [Theory]
    [InlineData(500, false)]
    [InlineData(0, true)]
    public void Invalid_options_are_rejected(int periodMilliseconds, bool excludeKindAlone)
    {
        var options = new UniqueOptions
        {
            ByPeriod = periodMilliseconds > 0 ? TimeSpan.FromMilliseconds(periodMilliseconds) : null,
            ExcludeKind = excludeKindAlone,
        };
        Assert.Throws<ArgumentException>(() => UniqueKeys.Validate(options));
    }

    [Fact]
    public void By_state_must_include_the_required_states()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            UniqueKeys.Validate(new UniqueOptions { ByState = [JobState.Available, JobState.Running] }));
        Assert.Contains("pending", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}

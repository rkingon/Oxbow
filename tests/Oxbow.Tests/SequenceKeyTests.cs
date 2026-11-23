using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Oxbow.Internal;

namespace Oxbow.Tests;

public sealed class SequenceKeyTests
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };

    [JobKind("deliver")]
    private sealed record Deliver([property: SequenceKey] string Endpoint, [property: SequenceKey] int Tenant, string Body) : IJobArgs;

    [JobKind("plain")]
    private sealed record Plain(string A) : IJobArgs;

    private static string? Key<T>(T args, string? explicitKey = null)
        where T : IJobArgs =>
        SequenceKeys.Resolve(explicitKey, JobKinds.Get(typeof(T)).Kind, JsonSerializer.Serialize(args, Json), Json.GetTypeInfo(typeof(T)));

    [Fact]
    public void Marked_properties_make_the_key_with_the_kind_sorted_by_json_name() =>
        Assert.Equal("deliver:{\"endpoint\":\"ep_1\",\"tenant\":7}", Key(new Deliver("ep_1", 7, "ignored")));

    [Fact]
    public void An_explicit_key_wins_and_is_used_verbatim() =>
        Assert.Equal("customer:42", Key(new Deliver("ep_1", 7, "ignored"), "customer:42"));

    [Fact]
    public void Args_without_marked_properties_have_no_sequence() => Assert.Null(Key(new Plain("a")));

    [Fact]
    public void An_encrypted_kind_stores_a_keyed_hash_instead_of_the_marked_values()
    {
        var args = new Deliver("ep_secret", 7, "ignored");
        var key = SequenceKeys.Resolve(
            null, "deliver", JsonSerializer.Serialize(args, Json), Json.GetTypeInfo(typeof(Deliver)), new ReversingProtector());

        Assert.StartsWith("deliver:hmac:", key, StringComparison.Ordinal);
        Assert.DoesNotContain("ep_secret", key, StringComparison.Ordinal);
        Assert.Equal(key, SequenceKeys.Resolve(
            null, "deliver", JsonSerializer.Serialize(args, Json), Json.GetTypeInfo(typeof(Deliver)), new ReversingProtector()));
    }

    private sealed class ReversingProtector : IJobArgsProtector
    {
        public string Protect(string kind, string args) => args;

        public string Unprotect(string kind, string payload) => payload;

        public byte[] HashUniqueKey(ReadOnlySpan<byte> uniqueKey) =>
            System.Security.Cryptography.HMACSHA256.HashData("test-key"u8, uniqueKey);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(256)]
    public void Keys_outside_1_to_255_characters_are_rejected(int length) =>
        Assert.Throws<ArgumentException>(() => Key(new Plain("a"), new string('k', length)));
}

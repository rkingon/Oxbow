using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Oxbow.Internal;

namespace Oxbow.Tests;

public sealed partial class PartitionKeyTests
{
    private static readonly JsonSerializerOptions Reflection = new(JsonSerializerDefaults.Web) { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };

    private static readonly JsonSerializerOptions SourceGenerated = new(JsonSerializerDefaults.Web)
    {
        TypeInfoResolver = KeyJsonContext.Default,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    [JobKind("sync")]
    internal sealed record Sync(
        [property: PartitionKey] string Tenant,
        [property: PartitionKey] int Region,
        string TraceId) : IJobArgs;

    [JobKind("plain")]
    internal sealed record Plain(string Tenant) : IJobArgs;

    [JobKind("optional")]
    internal sealed record Optional([property: PartitionKey] string? Tenant) : IJobArgs;

    [JsonSerializable(typeof(Sync))]
    [JsonSerializable(typeof(Plain))]
    [JsonSerializable(typeof(Optional))]
    internal sealed partial class KeyJsonContext : JsonSerializerContext;

    private sealed class HashingProtector : IJobArgsProtector
    {
        public string Protect(string kind, string args) => throw new NotSupportedException();

        public string Unprotect(string kind, string payload) => throw new NotSupportedException();

        public byte[] HashUniqueKey(ReadOnlySpan<byte> uniqueKey) => SHA256.HashData(uniqueKey);
    }

    private static string? Derive<T>(T args, JsonSerializerOptions json) =>
        PartitionKeys.Derive(JsonSerializer.Serialize(args, json.GetTypeInfo(typeof(T))), json.GetTypeInfo(typeof(T)));

    [Fact]
    public void Marked_properties_form_a_sorted_json_object()
    {
        Assert.Equal("""{"region":7,"tenant":"acme"}""", Derive(new Sync("acme", 7, "ignored"), Reflection));
    }

    [Fact]
    public void Source_generated_metadata_finds_the_same_properties()
    {
        Assert.Equal("""{"region":7,"tenant":"acme"}""", Derive(new Sync("acme", 7, "ignored"), SourceGenerated));
    }

    [Fact]
    public void Args_without_marked_properties_have_no_key()
    {
        Assert.Null(Derive(new Plain("acme"), Reflection));
    }

    [Fact]
    public void An_omitted_value_leaves_its_property_out()
    {
        Assert.Equal("{}", Derive(new Optional(null), SourceGenerated));
        Assert.Equal("""{"tenant":null}""", Derive(new Optional(null), Reflection));
    }

    [Fact]
    public void Encrypted_args_store_a_keyed_hash_of_the_key()
    {
        var plain = Derive(new Sync("acme", 7, "ignored"), Reflection)!;
        var hashed = PartitionKeys.Derive(
            JsonSerializer.Serialize(new Sync("acme", 7, "ignored"), Reflection), Reflection.GetTypeInfo(typeof(Sync)), new HashingProtector());
        Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(plain))), hashed);
    }

    [Fact]
    public void Raw_args_have_no_derived_key()
    {
        Assert.Null(PartitionKeys.Derive("""{"tenant":"acme"}""", typeInfo: null));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(256)]
    public void Keys_must_be_1_to_255_characters(int length)
    {
        Assert.Throws<ArgumentException>(() => PartitionKeys.Validate(new string('k', length)));
    }

    [Fact]
    public void Limits_below_one_are_rejected()
    {
        var options = new OxbowOptions();
        options.Queues["a"] = new QueueOptions { ConcurrencyLimit = new() { Global = 0 } };
        options.Queues["b"] = new QueueOptions { ConcurrencyLimit = new() { Local = -1 } };
        options.Queues["c"] = new QueueOptions { ConcurrencyLimit = new() { Global = 5, Local = 1 } };
        var result = new OxbowOptionsValidator().Validate(null, options);
        Assert.Equal(2, result.Failures!.Count());
    }
}

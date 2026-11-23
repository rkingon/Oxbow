using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.AspNetCore.DataProtection;
using Oxbow.DataProtection;
using Oxbow.Internal;

namespace Oxbow.Tests;

public sealed class ArgsEncryptionTests
{
    private static readonly byte[] Secret = RandomNumberGenerator.GetBytes(32);
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };

    private readonly DataProtectionJobArgsProtector _protector = new(new EphemeralDataProtectionProvider(), Secret);

    [JobKind("invoice"), EncryptArgs]
    private sealed record Invoice([property: UniqueKey] string InvoiceId) : IJobArgs;

    [Fact]
    public void Payloads_round_trip_and_are_bound_to_their_kind()
    {
        var payload = _protector.Protect("invoice", """{"invoiceId":"inv-1"}""");

        Assert.DoesNotContain("inv-1", payload, StringComparison.Ordinal);
        Assert.Equal("""{"invoiceId":"inv-1"}""", _protector.Unprotect("invoice", payload));
        Assert.ThrowsAny<CryptographicException>(() => _protector.Unprotect("other_kind", payload));
    }

    [Fact]
    public void By_args_unique_keys_of_encrypted_kinds_are_an_hmac()
    {
        var key = UniqueKeys.Build(
            new UniqueOptions { ByArgs = true }, "invoice", """{"invoiceId":"inv-1"}""", Json.GetTypeInfo(typeof(Invoice)), "default",
            DateTimeOffset.UnixEpoch, _protector);

        var plain = Encoding.UTF8.GetBytes("""&kind=invoice&args={"invoiceId":"inv-1"}""");
        Assert.Equal(HMACSHA256.HashData(Secret, plain), key);
        Assert.NotEqual(SHA256.HashData(plain), key);
    }

    [Fact]
    public void The_unique_key_secret_is_checked()
    {
        Assert.Throws<ArgumentException>(() => new DataProtectionJobArgsProtector(new EphemeralDataProtectionProvider(), new byte[16]));
        var withoutSecret = new DataProtectionJobArgsProtector(new EphemeralDataProtectionProvider());
        Assert.Throws<InvalidOperationException>(() => withoutSecret.HashUniqueKey("key"u8));
    }

    [Fact]
    public void Envelopes_open_by_shape()
    {
        var sealedArgs = ArgsEnvelope.Seal(_protector.Protect("invoice", """{"invoiceId":"inv-1"}"""));
        Assert.StartsWith("\"oxbow:enc:v1:", sealedArgs, StringComparison.Ordinal);

        Assert.Equal("inv-1", _protector.UnprotectArgs(Row(sealedArgs)).GetProperty("invoiceId").GetString());
        Assert.Equal("plain", _protector.UnprotectArgs(Row("""{"invoiceId":"plain"}""")).GetProperty("invoiceId").GetString());
        Assert.Throws<JobArgsDecryptionException>(() => ArgsEnvelope.Open(Row(sealedArgs), protector: null));
        Assert.Throws<JobArgsDecryptionException>(() => _protector.UnprotectArgs(Row("\"oxbow:enc:v2:payload\"")));
        Assert.Throws<JobArgsDecryptionException>(() => _protector.UnprotectArgs(Row("\"oxbow:enc:v1:tampered\"")));
    }

    private static JobRow Row(string args) => new()
    {
        Id = 1,
        State = JobState.Available,
        Attempt = 0,
        MaxAttempts = 1,
        AttemptedBy = [],
        CreatedAt = DateTimeOffset.UnixEpoch,
        ScheduledAt = DateTimeOffset.UnixEpoch,
        Priority = 1,
        Args = JsonDocument.Parse(args).RootElement.Clone(),
        Errors = [],
        Kind = "invoice",
        Metadata = JsonDocument.Parse("{}").RootElement.Clone(),
        Queue = "default",
        Tags = [],
    };
}

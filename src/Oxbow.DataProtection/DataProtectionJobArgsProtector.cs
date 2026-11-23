using System.Collections.Concurrent;
using System.Security.Cryptography;
using Microsoft.AspNetCore.DataProtection;

namespace Oxbow.DataProtection;

/// <summary>
/// An <see cref="IJobArgsProtector"/> on ASP.NET Core Data Protection. Payloads carry their key ID, so jobs encrypted
/// before a key rotation still decrypt. Each kind gets its own purpose, so a payload can't be replayed as another kind.
/// </summary>
public sealed class DataProtectionJobArgsProtector : IJobArgsProtector
{
    /// <summary>The root purpose string; the job kind is the sub-purpose.</summary>
    public const string Purpose = "Oxbow.Args";

    private const int MinSecretLength = 32;
    private readonly IDataProtector _root;
    private readonly byte[]? _uniqueKeySecret;
    private readonly ConcurrentDictionary<string, IDataProtector> _byKind = new(StringComparer.Ordinal);

    /// <summary>Creates the protector.</summary>
    /// <param name="provider">The Data Protection provider. Every node must share its key ring.</param>
    /// <param name="uniqueKeySecret">See <see cref="ArgsEncryptionOptions.UniqueKeySecret"/>.</param>
    public DataProtectionJobArgsProtector(IDataProtectionProvider provider, byte[]? uniqueKeySecret = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        if (uniqueKeySecret is { Length: < MinSecretLength })
        {
            throw new ArgumentException($"The unique key secret must be at least {MinSecretLength} bytes.", nameof(uniqueKeySecret));
        }

        _root = provider.CreateProtector(Purpose);
        _uniqueKeySecret = uniqueKeySecret?.ToArray();
    }

    /// <inheritdoc />
    public string Protect(string kind, string args) => ForKind(kind).Protect(args);

    /// <inheritdoc />
    public string Unprotect(string kind, string payload) => ForKind(kind).Unprotect(payload);

    /// <inheritdoc />
    public byte[] HashUniqueKey(ReadOnlySpan<byte> uniqueKey)
    {
        if (_uniqueKeySecret is null)
        {
            throw new InvalidOperationException(
                "A job kind that encrypts its args is unique by args, which needs a keyed hash. Set ArgsEncryptionOptions.UniqueKeySecret " +
                $"to at least {MinSecretLength} random bytes, the same on every node.");
        }

        return HMACSHA256.HashData(_uniqueKeySecret, uniqueKey);
    }

    private IDataProtector ForKind(string kind) => _byKind.GetOrAdd(kind, static (k, root) => root.CreateProtector(k), _root);
}

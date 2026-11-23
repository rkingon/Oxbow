using System.Text.Json;
using Oxbow.Internal;

namespace Oxbow;

/// <summary>
/// Encrypts this kind's args at rest. Needs an <see cref="IJobArgsProtector"/>, e.g. <c>AddArgsEncryption()</c> from
/// <c>Oxbow.DataProtection</c>. <see cref="OxbowOptions.EncryptAllArgs"/> encrypts every kind.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class EncryptArgsAttribute : Attribute;

/// <summary>
/// Encrypts and decrypts job args, and keys the unique-key hash of encrypted kinds. Register one as a singleton.
/// <c>Oxbow.DataProtection</c> implements it on ASP.NET Core Data Protection.
/// </summary>
public interface IJobArgsProtector
{
    /// <summary>Encrypts serialized args.</summary>
    /// <param name="kind">The job kind, as stored. Bind the payload to it so it can't be replayed as another kind.</param>
    /// <param name="args">The args as JSON.</param>
    /// <returns>An opaque payload, stored as a JSON string.</returns>
    string Protect(string kind, string args);

    /// <summary>Decrypts a payload from <see cref="Protect"/>, including ones made with keys since rotated out.</summary>
    /// <param name="kind">The job kind, as stored.</param>
    /// <param name="payload">The payload.</param>
    /// <returns>The args as JSON.</returns>
    string Unprotect(string kind, string payload);

    /// <summary>
    /// A keyed hash (e.g. HMAC-SHA256) of a unique key, used instead of SHA-256 when an encrypted kind is unique by
    /// args, so the key can't confirm a guessed args value. Must be deterministic: equal input, equal output.
    /// </summary>
    /// <param name="uniqueKey">The unique key string, UTF-8.</param>
    /// <returns>The hash stored in the unique key column.</returns>
    byte[] HashUniqueKey(ReadOnlySpan<byte> uniqueKey);
}

/// <summary>Reading encrypted args outside a worker.</summary>
public static class JobArgsProtectorExtensions
{
    /// <summary>
    /// The row's args as plaintext JSON. <see cref="JobRow.Args"/> of an encrypted job, e.g. from
    /// <see cref="IOxbowClient.GetAsync"/>, holds the encrypted envelope; unencrypted args are returned as they are.
    /// </summary>
    /// <param name="protector">The protector the job was inserted with.</param>
    /// <param name="row">The job.</param>
    /// <returns>The args.</returns>
    /// <exception cref="JobArgsDecryptionException">The payload can't be decrypted.</exception>
    public static JsonElement UnprotectArgs(this IJobArgsProtector protector, JobRow row)
    {
        ArgumentNullException.ThrowIfNull(protector);
        ArgumentNullException.ThrowIfNull(row);
        return ArgsEnvelope.Open(row, protector);
    }
}

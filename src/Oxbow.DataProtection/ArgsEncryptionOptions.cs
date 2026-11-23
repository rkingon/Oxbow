namespace Oxbow.DataProtection;

/// <summary>Configures <c>AddArgsEncryption()</c>.</summary>
public sealed class ArgsEncryptionOptions
{
    /// <summary>
    /// The HMAC-SHA256 key for unique keys of encrypted kinds that are unique by args: at least 32 random bytes, the same
    /// on every node, kept as secret as the Data Protection keys. Data Protection doesn't expose key material and
    /// rotates its default key, so this is separate. Changing it changes those unique keys: a job inserted before the
    /// change doesn't block a duplicate inserted after it. Only required when such a kind is inserted.
    /// </summary>
    public byte[]? UniqueKeySecret { get; set; }
}

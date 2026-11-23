using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Oxbow.Internal;

/// <summary>
/// Encrypted args are stored as the JSON string <c>"oxbow:enc:v1:&lt;payload&gt;"</c>. A string, not an object, so a node
/// that predates encryption fails to deserialize it (a retried attempt) instead of running the worker with default args.
/// </summary>
internal static class ArgsEnvelope
{
    public const string Marker = "oxbow:enc:";
    public const string Prefix = Marker + "v1:";

    private static readonly ConcurrentDictionary<Type, bool> Marked = new();

    public static bool IsMarked(Type argsType) =>
        Marked.GetOrAdd(argsType, static type => type.IsDefined(typeof(EncryptArgsAttribute), inherit: false));

    public static string Seal(string payload) => JsonValue.Create(Prefix + payload).ToJsonString();

    /// <summary>
    /// The plaintext args. Recognized by shape rather than by the kind's current settings, so turning encryption on or
    /// off for a kind leaves its existing jobs workable.
    /// </summary>
    public static JsonElement Open(JobRow row, IJobArgsProtector? protector)
    {
        if (row.Args.ValueKind != JsonValueKind.String || row.Args.GetString() is not { } stored || !stored.StartsWith(Marker, StringComparison.Ordinal))
        {
            return row.Args;
        }

        if (!stored.StartsWith(Prefix, StringComparison.Ordinal))
        {
            throw new JobArgsDecryptionException($"Args of job {row.Id} use an unknown encryption format; a newer Oxbow wrote them.");
        }

        if (protector is null)
        {
            throw new JobArgsDecryptionException(
                $"Args of job {row.Id} are encrypted, but no IJobArgsProtector is registered. Call AddArgsEncryption().");
        }

        string plaintext;
        try
        {
            plaintext = protector.Unprotect(row.Kind, stored[Prefix.Length..]);
        }
        catch (Exception ex)
        {
            throw new JobArgsDecryptionException($"Args of job {row.Id} can't be decrypted: {ex.Message}", ex);
        }

        using var document = JsonDocument.Parse(plaintext);
        return document.RootElement.Clone();
    }
}

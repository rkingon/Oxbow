using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Oxbow.Internal;

/// <summary>Encrypted args are stored as <c>{"oxbow:enc": "&lt;payload&gt;"}</c>, so the column stays jsonb.</summary>
internal static class ArgsEnvelope
{
    public const string Property = "oxbow:enc";

    private static readonly ConcurrentDictionary<Type, bool> Marked = new();

    public static bool IsMarked(Type argsType) =>
        Marked.GetOrAdd(argsType, static type => type.IsDefined(typeof(EncryptArgsAttribute), inherit: false));

    public static string Seal(string payload) => new JsonObject { [Property] = payload }.ToJsonString();

    /// <summary>
    /// The plaintext args. Recognized by shape rather than by the kind's current settings, so turning encryption on or
    /// off for a kind leaves its existing jobs workable.
    /// </summary>
    public static JsonElement Open(JobRow row, IJobArgsProtector? protector)
    {
        if (row.Args.ValueKind != JsonValueKind.Object
            || !row.Args.TryGetProperty(Property, out var payload)
            || payload.ValueKind != JsonValueKind.String)
        {
            return row.Args;
        }

        if (protector is null)
        {
            throw new JobArgsDecryptionException(
                $"Args of job {row.Id} are encrypted, but no IJobArgsProtector is registered. Call AddArgsEncryption().");
        }

        string plaintext;
        try
        {
            plaintext = protector.Unprotect(row.Kind, payload.GetString()!);
        }
        catch (Exception ex)
        {
            throw new JobArgsDecryptionException($"Args of job {row.Id} can't be decrypted: {ex.Message}", ex);
        }

        using var document = JsonDocument.Parse(plaintext);
        return document.RootElement.Clone();
    }
}

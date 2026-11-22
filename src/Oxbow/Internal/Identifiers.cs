using System.Text.RegularExpressions;

namespace Oxbow.Internal;

internal static partial class Identifiers
{
    // Postgres truncates NOTIFY channel names past 63 bytes; the longest is "{schema}.{prefix}leadership".
    private const int MaxChannelLength = 63;
    private const string LongestChannelSuffix = "leadership";

    public static void Validate(string schema, string prefix)
    {
        if (!SchemaPattern().IsMatch(schema))
        {
            throw new ArgumentException($"Schema '{schema}' must match {SchemaPattern()}.", nameof(schema));
        }

        if (!PrefixPattern().IsMatch(prefix))
        {
            throw new ArgumentException($"Table prefix '{prefix}' must match {PrefixPattern()}.", nameof(prefix));
        }

        if (schema.Length + 1 + prefix.Length + LongestChannelSuffix.Length > MaxChannelLength)
        {
            throw new ArgumentException(
                $"Schema and table prefix are too long together: '{schema}.{prefix}{LongestChannelSuffix}' exceeds Postgres's {MaxChannelLength}-byte NOTIFY channel limit.",
                nameof(prefix));
        }
    }

    public static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    [GeneratedRegex("^[a-z_][a-z0-9_]*$")]
    private static partial Regex SchemaPattern();

    [GeneratedRegex("^[a-z_][a-z0-9_]*$|^$")]
    private static partial Regex PrefixPattern();
}

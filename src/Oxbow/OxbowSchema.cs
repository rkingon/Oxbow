using System.Globalization;
using Oxbow.Internal;

namespace Oxbow;

/// <summary>
/// Oxbow's schema as SQL, for the consumer's own migrations. Oxbow never runs DDL at runtime.
/// </summary>
public static class OxbowSchema
{
    /// <summary>The schema version this release requires.</summary>
    public const int LatestVersion = 3;

    /// <summary>
    /// The SQL to move the schema from <paramref name="fromVersion"/> to <paramref name="toVersion"/>, one script per
    /// version. Upgrades when <paramref name="toVersion"/> is higher, downgrades when it's lower.
    /// </summary>
    /// <param name="fromVersion">The installed version; 0 for none.</param>
    /// <param name="toVersion">The target version.</param>
    /// <param name="schema">The Postgres schema.</param>
    /// <param name="tablePrefix">The prefix on every object Oxbow creates.</param>
    /// <returns>The scripts, in the order to run them.</returns>
    public static IReadOnlyList<OxbowMigration> GetMigrations(
        int fromVersion,
        int toVersion = LatestVersion,
        string schema = "public",
        string tablePrefix = "oxbow_")
    {
        ArgumentOutOfRangeException.ThrowIfNegative(fromVersion);
        ArgumentOutOfRangeException.ThrowIfNegative(toVersion);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(fromVersion, LatestVersion);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(toVersion, LatestVersion);
        Identifiers.Validate(schema, tablePrefix);

        var migrations = new List<OxbowMigration>();
        if (toVersion > fromVersion)
        {
            for (var version = fromVersion + 1; version <= toVersion; version++)
            {
                migrations.Add(new OxbowMigration(version, true, Render(ScriptName(version, "up"), schema, tablePrefix)));
            }
        }
        else
        {
            for (var version = fromVersion; version > toVersion; version--)
            {
                migrations.Add(new OxbowMigration(version, false, Render(ScriptName(version, "down"), schema, tablePrefix)));
            }
        }

        return migrations;
    }

    /// <summary>The grants a least-privilege runtime role needs on the latest schema, and nothing more.</summary>
    /// <param name="role">The role workers and clients connect as.</param>
    /// <param name="schema">The Postgres schema.</param>
    /// <param name="tablePrefix">The prefix on every object Oxbow creates.</param>
    /// <returns>The grant script.</returns>
    public static string GetGrants(string role, string schema = "public", string tablePrefix = "oxbow_") =>
        string.Concat(Enumerable.Range(1, LatestVersion).Select(v => GetGrants(role, v, schema, tablePrefix)));

    /// <summary>
    /// The grants on the objects schema <paramref name="version"/> adds, for the migration that installs it. Empty when
    /// the version adds nothing the runtime role needs.
    /// </summary>
    /// <param name="role">The role workers and clients connect as.</param>
    /// <param name="version">The schema version.</param>
    /// <param name="schema">The Postgres schema.</param>
    /// <param name="tablePrefix">The prefix on every object Oxbow creates.</param>
    /// <returns>The grant script.</returns>
    public static string GetGrants(string role, int version, string schema = "public", string tablePrefix = "oxbow_")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(role);
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(version, LatestVersion);
        Identifiers.Validate(schema, tablePrefix);
        return Render(ScriptName(version, "grants"), schema, tablePrefix).Replace("{{role}}", Identifiers.Quote(role), StringComparison.Ordinal);
    }

    private static string ScriptName(int version, string direction) => version switch
    {
        1 => $"001_initial.{direction}.sql",
        2 => $"002_v2.{direction}.sql",
        3 => $"003_v3.{direction}.sql",
        _ => throw new ArgumentOutOfRangeException(nameof(version), version.ToString(CultureInfo.InvariantCulture)),
    };

    private static string Render(string resource, string schema, string prefix)
    {
        using var stream = typeof(OxbowSchema).Assembly.GetManifestResourceStream("Oxbow.Schema." + resource)
            ?? throw new InvalidOperationException($"Embedded schema script '{resource}' is missing.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd()
            .Replace("{{schema}}", schema, StringComparison.Ordinal)
            .Replace("{{prefix}}", prefix, StringComparison.Ordinal);
    }
}

/// <summary>One schema migration script.</summary>
/// <param name="Version">The schema version the script installs (up) or removes (down).</param>
/// <param name="Up">Whether it's an upgrade.</param>
/// <param name="Sql">The SQL.</param>
public sealed record OxbowMigration(int Version, bool Up, string Sql);

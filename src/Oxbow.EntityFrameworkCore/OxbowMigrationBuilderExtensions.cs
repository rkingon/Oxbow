using Oxbow;

namespace Microsoft.EntityFrameworkCore.Migrations;

/// <summary>Adds Oxbow's schema to EF Core migrations, one Oxbow schema version per call.</summary>
public static class OxbowMigrationBuilderExtensions
{
    /// <summary>
    /// Installs Oxbow schema <paramref name="version"/>, which must be the next version after the one installed. Call
    /// it in <c>Up</c>, and <see cref="RemoveOxbowSchema"/> with the same version in <c>Down</c>. The version is required,
    /// not defaulted to the latest, so a migration replays the same SQL after an Oxbow upgrade.
    /// </summary>
    /// <param name="migrationBuilder">The migration builder.</param>
    /// <param name="version">The schema version to install.</param>
    /// <param name="schema">The Postgres schema.</param>
    /// <param name="tablePrefix">The prefix on every object Oxbow creates.</param>
    /// <returns>The migration builder.</returns>
    public static MigrationBuilder AddOxbowSchema(
        this MigrationBuilder migrationBuilder,
        int version,
        string schema = "public",
        string tablePrefix = "oxbow_")
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 1);
        migrationBuilder.Sql(OxbowSchema.GetMigrations(version - 1, version, schema, tablePrefix)[0].Sql);
        return migrationBuilder;
    }

    /// <summary>Removes Oxbow schema <paramref name="version"/>, undoing <see cref="AddOxbowSchema"/>.</summary>
    /// <param name="migrationBuilder">The migration builder.</param>
    /// <param name="version">The schema version to remove.</param>
    /// <param name="schema">The Postgres schema.</param>
    /// <param name="tablePrefix">The prefix on every object Oxbow creates.</param>
    /// <returns>The migration builder.</returns>
    public static MigrationBuilder RemoveOxbowSchema(
        this MigrationBuilder migrationBuilder,
        int version,
        string schema = "public",
        string tablePrefix = "oxbow_")
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 1);
        migrationBuilder.Sql(OxbowSchema.GetMigrations(version, version - 1, schema, tablePrefix)[0].Sql);
        return migrationBuilder;
    }

    /// <summary>
    /// Grants a least-privilege runtime role what it needs on the objects schema <paramref name="version"/> adds. Call it
    /// in the same migration as <see cref="AddOxbowSchema"/> for that version.
    /// </summary>
    /// <param name="migrationBuilder">The migration builder.</param>
    /// <param name="role">The role workers and clients connect as.</param>
    /// <param name="version">The schema version whose objects to grant.</param>
    /// <param name="schema">The Postgres schema.</param>
    /// <param name="tablePrefix">The prefix on every object Oxbow creates.</param>
    /// <returns>The migration builder.</returns>
    public static MigrationBuilder GrantOxbowAccess(
        this MigrationBuilder migrationBuilder,
        string role,
        int version,
        string schema = "public",
        string tablePrefix = "oxbow_")
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        var grants = OxbowSchema.GetGrants(role, version, schema, tablePrefix);
        if (!string.IsNullOrWhiteSpace(grants))
        {
            migrationBuilder.Sql(grants);
        }

        return migrationBuilder;
    }
}

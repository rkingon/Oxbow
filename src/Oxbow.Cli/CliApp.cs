using System.Globalization;
using Npgsql;

namespace Oxbow.Cli;

internal static class CliApp
{
    private const string Usage = """
        Usage:
          oxbow sql      [--from N] [--to N] [--schema S] [--prefix P]   Print migration SQL (default: 0 to latest).
          oxbow grants   --role R [--schema S] [--prefix P]               Print the least-privilege grants.
          oxbow migrate  [--connection CS] [--to N] [--schema S] [--prefix P] [--dry-run]
                                                                          Apply migrations as the schema owner.
                                                                          CS defaults to $OXBOW_CONNECTION_STRING.
          oxbow version                                                   Print the schema version this tool installs.
        """;

    public static async Task<int> RunAsync(string[] args, TextWriter output, TextWriter error)
    {
        try
        {
            if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
            {
                await output.WriteLineAsync(Usage).ConfigureAwait(false);
                return args.Length == 0 ? 1 : 0;
            }

            var options = ParseOptions(args.AsSpan(1));
            var schema = options.GetValueOrDefault("schema", "public");
            var prefix = options.GetValueOrDefault("prefix", "oxbow_");
            switch (args[0])
            {
                case "sql":
                    foreach (var migration in OxbowSchema.GetMigrations(Int(options, "from", 0), Int(options, "to", OxbowSchema.LatestVersion), schema, prefix))
                    {
                        await output.WriteLineAsync($"-- Oxbow schema version {migration.Version} ({(migration.Up ? "up" : "down")})").ConfigureAwait(false);
                        await output.WriteLineAsync(migration.Sql).ConfigureAwait(false);
                    }

                    return 0;

                case "grants":
                    var role = options.GetValueOrDefault("role") ?? throw new ArgumentException("--role is required.");
                    await output.WriteLineAsync(OxbowSchema.GetGrants(role, schema, prefix)).ConfigureAwait(false);
                    return 0;

                case "migrate":
                    var connectionString = options.GetValueOrDefault("connection")
                        ?? Environment.GetEnvironmentVariable("OXBOW_CONNECTION_STRING")
                        ?? throw new ArgumentException("Pass --connection or set OXBOW_CONNECTION_STRING.");
                    return await MigrateAsync(connectionString, Int(options, "to", OxbowSchema.LatestVersion), schema, prefix, options.ContainsKey("dry-run"), output)
                        .ConfigureAwait(false);

                case "version":
                    await output.WriteLineAsync(OxbowSchema.LatestVersion.ToString(CultureInfo.InvariantCulture)).ConfigureAwait(false);
                    return 0;

                default:
                    throw new ArgumentException($"Unknown command '{args[0]}'.");
            }
        }
        catch (Exception ex) when (ex is ArgumentException or NpgsqlException)
        {
            await error.WriteLineAsync($"oxbow: {ex.Message}").ConfigureAwait(false);
            return 2;
        }
    }

    private static async Task<int> MigrateAsync(string connectionString, int to, string schema, string prefix, bool dryRun, TextWriter output)
    {
        // Validates schema and prefix before they reach SQL.
        _ = OxbowSchema.GetGrants("validation", schema, prefix);

        var connection = new NpgsqlConnection(connectionString);
        await using (connection.ConfigureAwait(false))
        {
            await connection.OpenAsync().ConfigureAwait(false);
            var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                var current = await CurrentVersionAsync(connection, transaction, $"{schema}.{prefix}migration").ConfigureAwait(false);
                var migrations = OxbowSchema.GetMigrations(current, to, schema, prefix);
                if (migrations.Count == 0)
                {
                    await output.WriteLineAsync($"Already at version {current}.").ConfigureAwait(false);
                    return 0;
                }

                foreach (var migration in migrations)
                {
                    await output.WriteLineAsync($"{(dryRun ? "Would apply" : "Applying")} version {migration.Version} ({(migration.Up ? "up" : "down")})").ConfigureAwait(false);
                    if (!dryRun)
                    {
                        var command = new NpgsqlCommand(migration.Sql, connection, transaction);
                        await using (command.ConfigureAwait(false))
                        {
                            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                    }
                }

                if (!dryRun)
                {
                    await transaction.CommitAsync().ConfigureAwait(false);
                }

                await output.WriteLineAsync($"{(dryRun ? "Would be" : "Now")} at version {to}.").ConfigureAwait(false);
                return 0;
            }
        }
    }

    private static async Task<int> CurrentVersionAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string table)
    {
        var exists = new NpgsqlCommand("SELECT to_regclass(@table) IS NOT NULL", connection, transaction);
        await using (exists.ConfigureAwait(false))
        {
            exists.Parameters.AddWithValue("table", table);
            if (await exists.ExecuteScalarAsync().ConfigureAwait(false) is not true)
            {
                return 0;
            }
        }

        var version = new NpgsqlCommand($"SELECT coalesce(max(version), 0) FROM {table}", connection, transaction);
        await using (version.ConfigureAwait(false))
        {
            return Convert.ToInt32(await version.ExecuteScalarAsync().ConfigureAwait(false), CultureInfo.InvariantCulture);
        }
    }

    private static Dictionary<string, string> ParseOptions(ReadOnlySpan<string> args)
    {
        var options = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < args.Length; i++)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException($"Unexpected argument '{args[i]}'.");
            }

            var name = args[i][2..];
            options[name] = name == "dry-run" ? "true"
                : i + 1 < args.Length ? args[++i]
                : throw new ArgumentException($"--{name} needs a value.");
        }

        return options;
    }

    private static int Int(Dictionary<string, string> options, string name, int fallback) =>
        !options.TryGetValue(name, out var value) ? fallback
        : int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) ? parsed
        : throw new ArgumentException($"--{name} must be a non-negative integer.");
}

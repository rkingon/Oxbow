using Oxbow.Cli;

namespace Oxbow.IntegrationTests;

public sealed class CliTests(PostgresFixture database)
{
    private static async Task<(int ExitCode, string Output, string Error)> RunAsync(params string[] args)
    {
        using var output = new StringWriter();
        using var error = new StringWriter();
        var exitCode = await CliApp.RunAsync(args, output, error);
        return (exitCode, output.ToString(), error.ToString());
    }

    [Fact]
    public async Task Migrate_applies_once_then_is_a_no_op_and_can_go_back_down()
    {
        const string prefix = "cli_";
        var up = await RunAsync("migrate", "--connection", database.OwnerConnectionString, "--prefix", prefix);
        Assert.Equal(0, up.ExitCode);
        Assert.Contains("Now at version 1", up.Output, StringComparison.Ordinal);

        var again = await RunAsync("migrate", "--connection", database.OwnerConnectionString, "--prefix", prefix);
        Assert.Contains("Already at version 1", again.Output, StringComparison.Ordinal);

        var down = await RunAsync("migrate", "--connection", database.OwnerConnectionString, "--prefix", prefix, "--to", "0");
        Assert.Equal(0, down.ExitCode);
        Assert.Equal(0L, await database.ScalarAsOwnerAsync<long>($"SELECT count(*) FROM pg_class WHERE relname LIKE '{prefix}%'"));
    }

    [Fact]
    public async Task Sql_and_grants_print_rendered_scripts()
    {
        var sql = await RunAsync("sql", "--schema", "app", "--prefix", "x_");
        Assert.Contains("CREATE TABLE app.x_job", sql.Output, StringComparison.Ordinal);

        var grants = await RunAsync("grants", "--role", "worker");
        Assert.Contains("TO \"worker\"", grants.Output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Bad_input_fails_with_a_message()
    {
        var result = await RunAsync("grants");
        Assert.Equal(2, result.ExitCode);
        Assert.Contains("--role", result.Error, StringComparison.Ordinal);

        result = await RunAsync("sql", "--prefix", "Bad");
        Assert.Equal(2, result.ExitCode);
    }
}

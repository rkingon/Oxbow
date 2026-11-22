using Npgsql;

namespace Oxbow.IntegrationTests;

public sealed class TransactionalInsertTests(PostgresFixture database)
{
    [Fact]
    public async Task A_rolled_back_transaction_leaves_no_job()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, register: b => b.AddWorker<RecordArgs, RecordWorker>(), start: false);

        await using var connection = await app.DataSource.OpenConnectionAsync();
        long jobId;
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            jobId = (await app.Client.InsertAsync(connection, transaction, new RecordArgs("rolled back"))).Job.Id;
            Assert.NotNull(await GetInTransactionAsync(app, connection, transaction, jobId));
            await transaction.RollbackAsync();
        }

        Assert.Null(await app.Client.GetAsync(jobId));
    }

    [Fact]
    public async Task A_committed_transaction_is_worked_alongside_rls_protected_data()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(),
            b => b.AddWorker<RecordArgs, RecordWorker>());

        await using var connection = await app.DataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var tenant = new NpgsqlCommand("SELECT set_config('app.tenant', 'acme', true)", connection, transaction))
        {
            await tenant.ExecuteNonQueryAsync();
        }

        await using (var order = new NpgsqlCommand("INSERT INTO orders (tenant, item) VALUES ('acme', 'anvil')", connection, transaction))
        {
            await order.ExecuteNonQueryAsync();
        }

        var result = await app.Client.InsertAsync(connection, transaction, new RecordArgs("committed"));
        await transaction.CommitAsync();

        await app.WaitForStateAsync(result.Job.Id, JobState.Completed);
        Assert.Equal(1, app.Recorder.RunCounts[result.Job.Id]);
    }

    [Fact]
    public async Task An_uncommitted_job_is_not_visible_to_workers()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix, o => o.Queues["default"] = new QueueOptions(),
            b => b.AddWorker<RecordArgs, RecordWorker>());

        await using var connection = await app.DataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        var result = await app.Client.InsertAsync(connection, transaction, new RecordArgs("pending commit"));

        await Task.Delay(1000);
        Assert.False(app.Recorder.RunCounts.ContainsKey(result.Job.Id));

        await transaction.CommitAsync();
        await app.WaitForStateAsync(result.Job.Id, JobState.Completed);
    }

    private static async Task<object?> GetInTransactionAsync(TestApp app, NpgsqlConnection connection, NpgsqlTransaction transaction, long id)
    {
        await using var command = new NpgsqlCommand($"SELECT id FROM public.{app.Prefix}job WHERE id = @id", connection, transaction);
        command.Parameters.AddWithValue("id", id);
        return await command.ExecuteScalarAsync();
    }
}

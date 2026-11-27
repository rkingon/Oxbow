using System.Collections.Concurrent;
using System.Data;
using Microsoft.AspNetCore.DataProtection;
using Npgsql;

namespace Oxbow.IntegrationTests;

[JobKind("stepped")]
public sealed record SteppedArgs : IJobArgs;

public sealed record SteppedOutput(Guid First, int Second);

/// <summary>Records a step, snoozes, fails, then records a second step and completes.</summary>
public sealed class SteppedWorker(JobRecorder recorder) : IWorker<SteppedArgs>
{
    public async Task WorkAsync(Job<SteppedArgs> job, CancellationToken cancellationToken)
    {
        recorder.Started(job.Id);
        var execution = recorder.RunCounts[job.Id];
        var first = await job.StepAsync("first", _ =>
        {
            recorder.Events.Enqueue("first");
            return Task.FromResult(Guid.NewGuid());
        }, cancellationToken);

        switch (execution)
        {
            case 1:
                throw new JobSnoozeException(TimeSpan.Zero);
            case 2:
                throw new InvalidOperationException("Failed after the first step.");
        }

        var second = await job.StepAsync("second", _ =>
        {
            recorder.Events.Enqueue("second");
            return Task.FromResult(execution);
        }, cancellationToken);
        job.SetOutput(new SteppedOutput(first, second));
    }

    public DateTimeOffset? GetNextRetry(Job<SteppedArgs> job) => DateTimeOffset.UtcNow;
}

/// <summary>Records a step, then snoozes once, so the second execution replays it.</summary>
[JobKind("replayed")]
public sealed record ReplayedArgs(string Result) : IJobArgs;

public sealed class ReplayedWorker(JobRecorder recorder) : IWorker<ReplayedArgs>
{
    public async Task WorkAsync(Job<ReplayedArgs> job, CancellationToken cancellationToken)
    {
        recorder.Started(job.Id);
        var result = await job.StepAsync("only", _ =>
        {
            recorder.Events.Enqueue("ran");
            return Task.FromResult(job.Args.Result);
        }, cancellationToken);
        if (recorder.RunCounts[job.Id] == 1)
        {
            throw new JobSnoozeException(TimeSpan.Zero);
        }

        job.SetOutput(result);
    }
}

[JobKind("secret_replayed"), EncryptArgs]
public sealed record SecretReplayedArgs(string Email) : IJobArgs;

public sealed class SecretReplayedWorker(JobRecorder recorder) : IWorker<SecretReplayedArgs>
{
    public async Task WorkAsync(Job<SecretReplayedArgs> job, CancellationToken cancellationToken)
    {
        recorder.Started(job.Id);
        var account = await job.StepAsync("lookup", _ => Task.FromResult($"account of {job.Args.Email}"), cancellationToken);
        if (recorder.RunCounts[job.Id] == 1)
        {
            throw new JobSnoozeException(TimeSpan.Zero);
        }

        job.SetOutput(account);
    }
}

/// <summary>Records a step, then waits for the host to stop, so the next host's execution replays it.</summary>
[JobKind("step_then_block")]
public sealed record StepThenBlockArgs : IJobArgs;

public sealed class StepThenBlockWorker(JobRecorder recorder) : IWorker<StepThenBlockArgs>
{
    public async Task WorkAsync(Job<StepThenBlockArgs> job, CancellationToken cancellationToken)
    {
        recorder.Started(job.Id);
        var value = await job.StepAsync("before", _ =>
        {
            recorder.Events.Enqueue("before");
            return Task.FromResult(Guid.NewGuid());
        }, cancellationToken);
        if (recorder.RunCounts[job.Id] == 1)
        {
            recorder.Events.Enqueue("blocking");
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }

        job.SetOutput(value);
    }

    public TimeSpan? GetTimeout(Job<StepThenBlockArgs> job) => Timeout.InfiniteTimeSpan;
}

/// <summary>Two real attempts of one job, the first rescued while its step is in flight.</summary>
[JobKind("in_flight_step")]
public sealed record InFlightStepArgs : IJobArgs;

public sealed class InFlightGates
{
    public TaskCompletionSource FirstInStep { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource FirstRelease { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource FirstSuperseded { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource SecondStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource SecondRelease { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

public sealed class InFlightStepWorker(InFlightGates gates) : IWorker<InFlightStepArgs>
{
    public async Task WorkAsync(Job<InFlightStepArgs> job, CancellationToken cancellationToken)
    {
        var first = job.Attempt == 1;
        if (!first)
        {
            gates.SecondStarted.TrySetResult();
            await gates.SecondRelease.Task.WaitAsync(TimeSpan.FromSeconds(20), CancellationToken.None);
        }

        try
        {
            var result = await job.StepAsync("call", async _ =>
            {
                if (first)
                {
                    gates.FirstInStep.TrySetResult();
                    await gates.FirstRelease.Task.WaitAsync(TimeSpan.FromSeconds(20), CancellationToken.None);
                }

                return $"attempt {job.Attempt}";
            }, CancellationToken.None);
            job.SetOutput(result);
        }
        catch (JobAttemptSupersededException) when (first)
        {
            gates.FirstSuperseded.TrySetResult();
            throw;
        }
    }

    public TimeSpan? GetTimeout(Job<InFlightStepArgs> job) =>
        job.Attempt == 1 ? TimeSpan.FromMilliseconds(200) : Timeout.InfiniteTimeSpan;

    public DateTimeOffset? GetNextRetry(Job<InFlightStepArgs> job) => DateTimeOffset.UtcNow;
}

/// <summary>Locks its attempt in a transaction and returns without ending it; the test ends it.</summary>
[JobKind("leaked_lock")]
public sealed record LeakedLockArgs(Guid Key) : IJobArgs;

public sealed class LeakedLockWorker(NpgsqlDataSource dataSource) : IWorker<LeakedLockArgs>
{
    public static ConcurrentDictionary<Guid, NpgsqlTransaction> Open { get; } = new();

    public async Task WorkAsync(Job<LeakedLockArgs> job, CancellationToken cancellationToken)
    {
        var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await job.LockAttemptAsync(transaction, cancellationToken);
        Open[job.Args.Key] = transaction;
    }
}

public sealed class StepTests(PostgresFixture database) : IDisposable
{
    private readonly DirectoryInfo _keys = Directory.CreateTempSubdirectory("oxbow-keys-");

    public void Dispose() => _keys.Delete(recursive: true);

    private async Task<TestApp> StartAsync(
        Action<OxbowOptions>? configure = null,
        string? prefix = null,
        JobRecorder? recorder = null,
        bool start = true) =>
        await TestApp.StartAsync(database, prefix ?? await database.CreateSchemaAsync(), o =>
        {
            o.Queues["default"] = new QueueOptions();
            configure?.Invoke(o);
        }, b =>
        {
            b.Services.AddDataProtection().SetApplicationName("oxbow-tests").PersistKeysToFileSystem(_keys);
            b.AddArgsEncryption()
                .AddWorker<SteppedArgs, SteppedWorker>()
                .AddWorker<ReplayedArgs, ReplayedWorker>()
                .AddWorker<SecretReplayedArgs, SecretReplayedWorker>()
                .AddWorker<StepThenBlockArgs, StepThenBlockWorker>()
                .AddWorker<LeakedLockArgs, LeakedLockWorker>()
                .AddWorker<RecordArgs, RecordWorker>()
                .AddWorker<HoldArgs, HoldWorker>();
        }, recorder, start: start);

    private async Task<(TestApp App, Job<HoldArgs> Job, TaskCompletionSource Release)> HoldAsync()
    {
        var app = await StartAsync();
        var key = Guid.NewGuid();
        await app.Client.InsertAsync(new HoldArgs(key));
        var (job, release) = await HoldWorker.WaitHeldAsync(key);
        return (app, job, release);
    }

    private Task<string?> StoredStepAsync(TestApp app, long jobId, string name) =>
        database.ScalarAsOwnerAsync<string>(
            $"SELECT output::text FROM public.{app.Prefix}job_step WHERE job_id = {jobId} AND name = '{name}'");

    private Task<long> StepCountAsync(TestApp app, long jobId) =>
        database.ScalarAsOwnerAsync<long>($"SELECT count(*) FROM public.{app.Prefix}job_step WHERE job_id = {jobId}");

    private Task RecordAsOwnerAsync(TestApp app, long jobId, string name, string json) =>
        database.ExecuteAsOwnerAsync(
            $"INSERT INTO public.{app.Prefix}job_step (job_id, name, output) VALUES ({jobId}, '{name}', '{json}')");

    private Task SupersedeAsync(TestApp app, long jobId) =>
        database.ExecuteAsOwnerAsync($"UPDATE public.{app.Prefix}job SET attempted_at = attempted_at + interval '1 second' WHERE id = {jobId}");

    private Task<long> WidgetsAsync(string name) =>
        database.ScalarAsOwnerAsync<long>($"SELECT count(*) FROM widgets WHERE name = '{name}'");

    private static async Task InsertWidgetAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string name)
    {
        await using var insert = new NpgsqlCommand($"INSERT INTO widgets (name) VALUES ('{name}')", connection, transaction);
        await insert.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task A_recorded_step_is_replayed_after_a_snooze_and_a_failed_attempt()
    {
        await using var app = await StartAsync();
        var id = (await app.Client.InsertAsync(new SteppedArgs())).Job.Id;

        var row = await app.WaitForStateAsync(id, JobState.Completed);

        Assert.Equal(3, app.Recorder.RunCounts[id]);
        Assert.Equal(["first", "second"], app.Recorder.Events);
        Assert.Equal(2, row.Attempt);
        Assert.Contains("Failed after the first step.", Assert.Single(row.Errors).Error, StringComparison.Ordinal);
        var output = row.GetOutput<SteppedOutput>()!;
        Assert.Equal($"\"{output.First}\"", await StoredStepAsync(app, id, "first"));
        Assert.Equal(3, output.Second);
    }

    [Fact]
    public async Task A_retried_job_replays_its_recorded_steps()
    {
        await using var app = await StartAsync();
        var id = (await app.Client.InsertAsync(new SteppedArgs())).Job.Id;
        var first = (await app.WaitForStateAsync(id, JobState.Completed)).GetOutput<SteppedOutput>();

        await app.Client.RetryAsync(id);
        var again = await app.WaitForAsync(id, j => j.State == JobState.Completed && app.Recorder.RunCounts[id] == 4);

        Assert.Equal(["first", "second"], app.Recorder.Events);
        Assert.Equal(first, again.GetOutput<SteppedOutput>());
    }

    [Fact]
    public async Task A_step_recorded_before_shutdown_is_replayed_by_the_next_host()
    {
        var prefix = await database.CreateSchemaAsync();
        var recorder = new JobRecorder();
        long id;
        await using (var first = await StartAsync(prefix: prefix, recorder: recorder))
        {
            id = (await first.Client.InsertAsync(new StepThenBlockArgs())).Job.Id;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            while (!recorder.Events.Contains("blocking"))
            {
                await Task.Delay(20, cts.Token);
            }
        }

        await using var second = await StartAsync(prefix: prefix, recorder: recorder);
        var row = await second.WaitForStateAsync(id, JobState.Completed);

        Assert.Equal(["before", "blocking"], recorder.Events);
        Assert.Equal(1, row.Attempt);
        Assert.Equal($"\"{row.GetOutput<Guid>()}\"", await StoredStepAsync(second, id, "before"));
    }

    [Fact]
    public async Task A_step_is_a_heartbeat()
    {
        var (app, job, release) = await HoldAsync();
        await using var _ = app;

        await job.StepAsync("beat", _ => Task.FromResult(1));

        Assert.True((await app.Client.GetAsync(job.Id))!.HeartbeatAt >= job.Row.AttemptedAt);
        release.SetResult();
    }

    [Fact]
    public async Task A_result_recorded_first_wins_over_the_one_just_run()
    {
        var (app, job, release) = await HoldAsync();
        await using var _ = app;

        var result = await job.StepAsync("raced", async _ =>
        {
            await RecordAsOwnerAsync(app, job.Id, "raced", "\"theirs\"");
            return "mine";
        });

        Assert.Equal("theirs", result);
        release.SetResult();
    }

    [Fact]
    public async Task A_known_superseded_attempt_neither_replays_nor_starts_a_step()
    {
        var (app, stale, release) = await HoldAsync();
        await using var _ = app;
        await RecordAsOwnerAsync(app, stale.Id, "taken", "\"replacement\"");
        await SupersedeAsync(app, stale.Id);
        var ran = false;

        await Assert.ThrowsAsync<JobAttemptSupersededException>(() => stale.StepAsync("missing", _ =>
        {
            ran = true;
            return Task.FromResult("zombie");
        }));
        await Assert.ThrowsAsync<JobAttemptSupersededException>(() => stale.StepAsync("taken", _ => Task.FromResult("zombie")));

        Assert.False(ran);
        Assert.Null(await StoredStepAsync(app, stale.Id, "missing"));
        release.SetResult();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_step_in_flight_when_its_attempt_is_rescued_records_only_if_it_finishes_first(bool replacementFinishesFirst)
    {
        var gates = new InFlightGates();
        var prefix = await database.CreateSchemaAsync();
        void Configure(OxbowOptions options)
        {
            options.Queues["default"] = new QueueOptions { MaxWorkers = 1 };
            options.RescueStuckJobsAfter = TimeSpan.FromMilliseconds(500);
        }
        void Register(IOxbowBuilder builder)
        {
            builder.Services.AddSingleton(gates);
            builder.AddWorker<InFlightStepArgs, InFlightStepWorker>();
        }

        await using var original = await TestApp.StartAsync(database, prefix, Configure, Register);
        var id = (await original.Client.InsertAsync(new InFlightStepArgs())).Job.Id;
        await gates.FirstInStep.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await using var replacement = await TestApp.StartAsync(database, prefix, Configure, Register);
        try
        {
            await gates.SecondStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
            string expected;
            if (replacementFinishesFirst)
            {
                gates.SecondRelease.TrySetResult();
                await replacement.WaitForStateAsync(id, JobState.Completed);
                gates.FirstRelease.TrySetResult();
                expected = "attempt 2";
            }
            else
            {
                gates.FirstRelease.TrySetResult();
                await gates.FirstSuperseded.Task.WaitAsync(TimeSpan.FromSeconds(10));
                gates.SecondRelease.TrySetResult();
                expected = "attempt 1";
            }

            await gates.FirstSuperseded.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var row = await replacement.WaitForStateAsync(id, JobState.Completed);
            Assert.Equal(expected, row.GetOutput<string>());
            Assert.Equal($"\"{expected}\"", await StoredStepAsync(replacement, id, "call"));
        }
        finally
        {
            gates.FirstRelease.TrySetResult();
            gates.SecondRelease.TrySetResult();
        }
    }

    [Fact]
    public async Task A_locked_attempt_holds_off_other_writers_until_commit()
    {
        var (app, job, release) = await HoldAsync();
        await using var _ = app;
        var committed = $"locked-{Guid.NewGuid()}";

        await using (var connection = await app.DataSource.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await job.LockAttemptAsync(transaction);

            // The attempt's own writes skip the row instead of waiting for the transaction they're inside.
            await job.HeartbeatAsync().WaitAsync(TimeSpan.FromSeconds(5));
            await job.ReportProgressAsync(50).WaitAsync(TimeSpan.FromSeconds(5));

            // What the rescuer and the completer would do waits for the commit.
            var blocked = await Assert.ThrowsAsync<PostgresException>(() => database.ExecuteAsOwnerAsync($"""
                SET lock_timeout = '200ms';
                UPDATE public.{app.Prefix}job SET state = 'retryable' WHERE id = {job.Id};
                """));
            Assert.Equal(PostgresErrorCodes.LockNotAvailable, blocked.SqlState);

            await InsertWidgetAsync(connection, transaction, committed);
            await transaction.CommitAsync();
        }

        var row = (await app.Client.GetAsync(job.Id))!;
        Assert.Equal(JobState.Running, row.State);
        Assert.Null(row.HeartbeatAt);
        Assert.Null(row.Progress);
        Assert.Equal(1L, await WidgetsAsync(committed));

        // The skipped heartbeat didn't count toward the throttle.
        await job.HeartbeatAsync();
        Assert.NotNull((await app.Client.GetAsync(job.Id))!.HeartbeatAt);
        release.SetResult();
    }

    [Fact]
    public async Task A_superseded_attempt_cannot_lock_and_its_transaction_cannot_commit()
    {
        var (app, job, release) = await HoldAsync();
        await using var _ = app;
        var refused = $"refused-{Guid.NewGuid()}";
        await SupersedeAsync(app, job.Id);

        await using (var connection = await app.DataSource.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await InsertWidgetAsync(connection, transaction, refused);
            await Assert.ThrowsAsync<JobAttemptSupersededException>(() => job.LockAttemptAsync(transaction));

            // Even a caller that swallows the exception and commits writes nothing.
            try
            {
                await transaction.CommitAsync();
            }
            catch (PostgresException)
            {
            }
        }

        Assert.Equal(0L, await WidgetsAsync(refused));
        release.SetResult();
    }

    [Fact]
    public async Task Under_repeatable_read_a_job_row_written_since_the_snapshot_fails_the_lock_until_retried()
    {
        var (app, job, release) = await HoldAsync();
        await using var _ = app;

        await using (var connection = await app.DataSource.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead))
        {
            await InsertWidgetAsync(connection, transaction, $"snapshot-{Guid.NewGuid()}");
            await job.ReportProgressAsync(10);
            var ex = await Assert.ThrowsAsync<PostgresException>(() => job.LockAttemptAsync(transaction));
            Assert.Equal(PostgresErrorCodes.SerializationFailure, ex.SqlState);
        }

        await using (var connection = await app.DataSource.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead))
        {
            await job.LockAttemptAsync(transaction);
            await job.ReportProgressAsync(20);
            await transaction.CommitAsync();
        }

        release.SetResult();
    }

    [Fact]
    public async Task A_worker_that_returns_with_its_attempt_locked_holds_only_its_own_outcome()
    {
        await using var app = await StartAsync();
        var key = Guid.NewGuid();
        var leaked = (await app.Client.InsertAsync(new LeakedLockArgs(key))).Job.Id;
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
        {
            while (!LeakedLockWorker.Open.ContainsKey(key))
            {
                await Task.Delay(20, cts.Token);
            }
        }

        var other = (await app.Client.InsertAsync(new RecordArgs("unrelated"))).Job.Id;
        await app.WaitForStateAsync(other, JobState.Completed);
        Assert.Equal(JobState.Running, (await app.Client.GetAsync(leaked))!.State);

        var transaction = LeakedLockWorker.Open[key];
        var connection = transaction.Connection!;
        await transaction.CommitAsync();
        await connection.DisposeAsync();
        await app.WaitForStateAsync(leaked, JobState.Completed);
    }

    [Fact]
    public async Task The_rescuer_skips_a_locked_attempt_and_rescues_the_rest()
    {
        await using var app = await StartAsync(o =>
        {
            o.RescueStuckJobsAfter = TimeSpan.FromMinutes(10);
            o.JobTimeout = TimeSpan.FromSeconds(1);
        });
        async Task<long> InsertStuckAsync() => await database.ScalarAsOwnerAsync<long>($"""
            INSERT INTO public.{app.Prefix}job (args, kind, queue, state, attempt, attempted_at, max_attempts)
            VALUES ('{"{"}"value": "x"{"}"}', 'record', 'default', 'running', 1, now() - interval '1 hour', 1)
            RETURNING id
            """);
        var locked = await InsertStuckAsync();
        var free = await InsertStuckAsync();

        await using (var connection = await app.DataSource.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await using (var share = new NpgsqlCommand($"SELECT 1 FROM public.{app.Prefix}job WHERE id = {locked} FOR SHARE", connection, transaction))
            {
                await share.ExecuteNonQueryAsync();
            }

            await app.WaitForStateAsync(free, JobState.Discarded);
            Assert.Equal(JobState.Running, (await app.Client.GetAsync(locked))!.State);
            await transaction.CommitAsync();
        }

        await app.WaitForStateAsync(locked, JobState.Discarded);
    }

    [Fact]
    public async Task Deleting_jobs_deletes_their_steps_by_every_path()
    {
        await using var app = await StartAsync(o => o.CompletedJobRetention = TimeSpan.FromMilliseconds(500));
        var single = (await app.Client.InsertAsync(new ReplayedArgs("single"))).Job.Id;
        await app.WaitForStateAsync(single, JobState.Completed);
        Assert.Equal(1L, await StepCountAsync(app, single));
        await app.Client.DeleteAsync(single);
        Assert.Equal(0L, await StepCountAsync(app, single));

        var bulk = (await app.Client.InsertAsync(new ReplayedArgs("bulk"), new InsertOptions { Tags = ["bulk"] })).Job.Id;
        await app.WaitForStateAsync(bulk, JobState.Completed);
        await app.Client.DeleteManyAsync(new JobFilter { Tags = ["bulk"] });
        Assert.Equal(0L, await StepCountAsync(app, bulk));

        var cleaned = (await app.Client.InsertAsync(new ReplayedArgs("cleaned"))).Job.Id;
        await app.WaitForStateAsync(cleaned, JobState.Completed);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (await app.Client.GetAsync(cleaned) is not null)
        {
            await Task.Delay(50, cts.Token);
        }

        Assert.Equal(0L, await StepCountAsync(app, cleaned));
    }

    [Fact]
    public async Task A_plaintext_result_that_looks_encrypted_replays_as_plaintext()
    {
        await using var app = await StartAsync();
        const string marker = "oxbow:enc:v1:literal";
        var id = (await app.Client.InsertAsync(new ReplayedArgs(marker))).Job.Id;

        var row = await app.WaitForStateAsync(id, JobState.Completed);

        Assert.Equal(2, app.Recorder.RunCounts[id]);
        Assert.Equal(["ran"], app.Recorder.Events);
        Assert.Equal(marker, row.GetOutput<string>());
    }

    [Fact]
    public async Task An_encrypted_jobs_step_results_are_encrypted_at_rest()
    {
        await using var app = await StartAsync();
        var id = (await app.Client.InsertAsync(new SecretReplayedArgs("ada@example.com"))).Job.Id;

        var row = await app.WaitForStateAsync(id, JobState.Completed);

        Assert.Equal(2, app.Recorder.RunCounts[id]);
        Assert.Equal("account of ada@example.com", row.GetOutput<string>());
        var stored = await StoredStepAsync(app, id, "lookup");
        Assert.StartsWith("\"oxbow:enc:v1:", stored, StringComparison.Ordinal);
        Assert.DoesNotContain("example.com", stored, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("\"account of someone\"", "isn't encrypted")]
    [InlineData("\"oxbow:enc:v1:not-a-payload\"", "can't be decrypted")]
    public async Task An_encrypted_jobs_step_result_that_isnt_its_ciphertext_fails_the_attempt(string stored, string error)
    {
        await using var app = await StartAsync(start: false);
        var id = (await app.Client.InsertAsync(new SecretReplayedArgs("ada@example.com"), new InsertOptions { MaxAttempts = 1 })).Job.Id;
        await RecordAsOwnerAsync(app, id, "lookup", stored);

        await app.Host.StartAsync();
        var row = await app.WaitForStateAsync(id, JobState.Discarded);

        Assert.Contains(error, Assert.Single(row.Errors).Error, StringComparison.Ordinal);
        Assert.Empty(app.Recorder.Events);
    }
}

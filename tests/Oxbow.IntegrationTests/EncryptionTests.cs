using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Oxbow.Testing;

namespace Oxbow.IntegrationTests;

[JobKind("secret"), EncryptArgs]
public sealed record SecretArgs(string Email) : IJobArgs;

public sealed class SecretWorker(JobRecorder recorder) : IWorker<SecretArgs>
{
    public Task WorkAsync(Job<SecretArgs> job, CancellationToken cancellationToken)
    {
        recorder.Started(job.Id);
        recorder.Events.Enqueue(job.Args.Email);
        return Task.CompletedTask;
    }

    public DateTimeOffset? GetNextRetry(Job<SecretArgs> job) => DateTimeOffset.UtcNow.AddMilliseconds(100);
}

[JobKind("secret_unique"), EncryptArgs]
public sealed record SecretUniqueArgs([property: UniqueKey] string InvoiceId, string TraceId) : IJobArgs
{
    public InsertOptions GetInsertOptions() => new() { Unique = new() { ByArgs = true } };
}

[JobKind("secret_token"), EncryptArgs, JsonConverter(typeof(Converter))]
public sealed record SecretTokenArgs(string Value) : IJobArgs
{
    public sealed class Converter : JsonConverter<SecretTokenArgs>
    {
        public override SecretTokenArgs Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => new(reader.GetString()!);

        public override void Write(Utf8JsonWriter writer, SecretTokenArgs value, JsonSerializerOptions options) => writer.WriteStringValue(value.Value);
    }
}

public sealed class EncryptionTests(PostgresFixture database) : IDisposable
{
    private static readonly byte[] Secret = RandomNumberGenerator.GetBytes(32);
    private readonly DirectoryInfo _keys = Directory.CreateTempSubdirectory("oxbow-keys-");

    public void Dispose() => _keys.Delete(recursive: true);

    [Fact]
    public async Task Every_insert_path_stores_ciphertext_and_workers_see_plaintext()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o =>
            o.AddPeriodicJob(PeriodicSchedule.Every(TimeSpan.FromHours(1)), () => new SecretArgs("periodic@example.com"), runOnStart: true));

        await app.Client.InsertAsync(new SecretArgs("single@example.com"));
        await app.Client.InsertManyAsync([new InsertManyItem(new SecretArgs("many@example.com"))]);
        await app.Client.InsertManyFastAsync([new InsertManyItem(new SecretArgs("copy@example.com"))]);
        await app.Client.InsertAsync(new RecordArgs("plain@example.com"));

        string[] expected = ["copy@example.com", "many@example.com", "periodic@example.com", "plain@example.com", "single@example.com"];
        await WaitForEventsAsync(app, expected.Length);
        Assert.Equal(expected, app.Recorder.Events.Order(StringComparer.Ordinal));

        Assert.Equal(0L, await database.ScalarAsOwnerAsync<long>(
            $"SELECT count(*) FROM public.{prefix}job WHERE kind = 'secret' AND (args::text LIKE '%example.com%' OR jsonb_typeof(args) <> 'string' OR args #>> '{{}}' NOT LIKE 'oxbow:enc:v1:%')"));
        Assert.Equal(4L, await database.ScalarAsOwnerAsync<long>($"SELECT count(*) FROM public.{prefix}job WHERE kind = 'secret'"));
        Assert.Equal(1L, await database.ScalarAsOwnerAsync<long>(
            $"SELECT count(*) FROM public.{prefix}job WHERE kind = 'record' AND args = '{{\"value\": \"plain@example.com\"}}'"));
    }

    [Fact]
    public async Task Stored_periodic_jobs_keep_encrypted_args_encrypted_at_rest()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix);

        var typed = await app.Client.CreatePeriodicJobAsync("typed", PeriodicSchedule.Every(TimeSpan.FromSeconds(1)), new SecretArgs("typed@example.com"));
        await app.Client.CreatePeriodicJobAsync(
            "untyped", PeriodicSchedule.Every(TimeSpan.FromSeconds(1)), "secret", JsonDocument.Parse("""{"email":"untyped@example.com"}""").RootElement);
        await app.Client.CreatePeriodicJobAsync("plain", PeriodicSchedule.Every(TimeSpan.FromHours(1)), new RecordArgs("plain@example.com"));

        Assert.StartsWith("oxbow:enc:v1:", typed.Args!.Value.GetString(), StringComparison.Ordinal);
        Assert.Equal(0L, await database.ScalarAsOwnerAsync<long>(
            $"SELECT count(*) FROM public.{prefix}periodic_job WHERE kind = 'secret' AND (args::text LIKE '%example.com%' OR args #>> '{{}}' NOT LIKE 'oxbow:enc:v1:%')"));
        Assert.Equal(1L, await database.ScalarAsOwnerAsync<long>(
            $"SELECT count(*) FROM public.{prefix}periodic_job WHERE name = 'plain' AND args = '{{\"value\": \"plain@example.com\"}}'"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!(app.Recorder.Events.Contains("typed@example.com") && app.Recorder.Events.Contains("untyped@example.com")))
        {
            await Task.Delay(50, cts.Token);
        }

        Assert.Equal(0L, await database.ScalarAsOwnerAsync<long>(
            $"SELECT count(*) FROM public.{prefix}job WHERE kind = 'secret' AND args::text LIKE '%example.com%'"));
    }

    [Fact]
    public async Task A_node_that_predates_encryption_fails_to_deserialize_instead_of_running_with_defaults()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, start: false);

        var job = (await app.Client.InsertAsync(new SecretArgs("ada@example.com"))).Job;

        // What 0.1.0-alpha.3's WorkerRegistration.CreateUnit does with the stored args.
        Assert.Throws<JsonException>(() => job.Args.Deserialize<SecretArgs>(JsonSerializerOptions.Web));
        Assert.Throws<JsonException>(() => job.Args.Deserialize<RecordArgs>(JsonSerializerOptions.Web));
    }

    [Fact]
    public async Task Encrypted_args_must_serialize_to_an_object()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, start: false);

        // A string-reading converter on an older node would accept the encrypted string as args.
        await Assert.ThrowsAsync<ArgumentException>(() => app.Client.InsertAsync(new SecretTokenArgs("token")));
    }

    [Fact]
    public async Task Encrypt_all_args_covers_unmarked_kinds()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o => o.EncryptAllArgs = true);

        var job = (await app.Client.InsertAsync(new RecordArgs("hidden"))).Job;
        await app.WaitForStateAsync(job.Id, JobState.Completed);

        Assert.Equal(["hidden"], app.Recorder.Events);
        Assert.StartsWith("oxbow:enc:v1:", job.Args.GetString(), StringComparison.Ordinal);
        Assert.Equal("hidden", Protector(app).UnprotectArgs(job).GetProperty("value").GetString());
    }

    [Fact]
    public async Task Unique_by_args_dedupes_on_an_hmac()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix);

        var first = await app.Client.InsertAsync(new SecretUniqueArgs("inv-1", "a"));
        var second = await app.Client.InsertAsync(new SecretUniqueArgs("inv-1", "b"));
        var other = await app.Client.InsertAsync(new SecretUniqueArgs("inv-2", "a"));

        Assert.True(second.UniqueSkippedAsDuplicate);
        Assert.Equal(first.Job.Id, second.Job.Id);
        Assert.False(other.UniqueSkippedAsDuplicate);

        var plainKey = Encoding.UTF8.GetBytes("""&kind=secret_unique&args={"invoiceId":"inv-1"}""");
        var stored = await database.ScalarAsOwnerAsync<byte[]>($"SELECT unique_key FROM public.{prefix}job WHERE id = {first.Job.Id}");
        Assert.Equal(HMACSHA256.HashData(Secret, plainKey), stored);
        Assert.NotEqual(SHA256.HashData(plainKey), stored);
    }

    [Fact]
    public async Task Jobs_encrypted_before_a_key_rotation_still_decrypt()
    {
        var prefix = await database.CreateSchemaAsync();
        JobRow before;
        IKey newKey;
        await using (var old = await StartAsync(prefix, start: false))
        {
            before = (await old.Client.InsertAsync(new SecretArgs("before@example.com"))).Job;
            newKey = old.Host.Services.GetRequiredService<IKeyManager>().CreateNewKey(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(90));
        }

        await using var app = await StartAsync(prefix);
        var after = (await app.Client.InsertAsync(new SecretArgs("after@example.com"))).Job;

        Assert.NotEqual(newKey.KeyId, KeyIdOf(before));
        Assert.Equal(newKey.KeyId, KeyIdOf(after));
        await app.WaitForStateAsync(before.Id, JobState.Completed);
        await app.WaitForStateAsync(after.Id, JobState.Completed);
        Assert.Equal(["after@example.com", "before@example.com"], app.Recorder.Events.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task A_worker_chosen_retry_on_encrypted_args_is_recorded_and_retried()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o => o.EncryptAllArgs = true);

        var result = await app.Client.InsertAsync(new RetryAfterArgs(TimeSpan.Zero), new InsertOptions { MaxAttempts = 2 });

        var job = await app.WaitForStateAsync(result.Job.Id, JobState.Discarded);
        Assert.Equal(2, job.Attempt);
        Assert.All(job.Errors, e => Assert.Equal("JobRetryException: vendor.rate_limited", e.Error));
    }

    [Fact]
    public async Task Args_that_fail_to_decrypt_are_retried_then_discarded()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, start: false);

        var job = (await app.Client.InsertAsync(new SecretArgs("x@example.com"), new InsertOptions { MaxAttempts = 2 })).Job;
        await database.ExecuteAsOwnerAsync($$"""UPDATE public.{{prefix}}job SET args = '"oxbow:enc:v1:tampered"' WHERE id = {{job.Id}}""");
        await app.Host.StartAsync();

        var discarded = await app.WaitForStateAsync(job.Id, JobState.Discarded);
        Assert.Equal(2, discarded.Errors.Count);
        Assert.All(discarded.Errors, e => Assert.StartsWith(nameof(JobArgsDecryptionException), e.Error, StringComparison.Ordinal));
        Assert.Empty(app.Recorder.Events);
    }

    [Fact]
    public async Task The_rescuer_retries_a_stuck_job_whose_args_fail_to_decrypt()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o =>
        {
            o.RescueStuckJobsAfter = TimeSpan.FromSeconds(1);
            o.JobTimeout = TimeSpan.FromSeconds(1);
        });

        var id = await database.ScalarAsOwnerAsync<long>($$"""
            INSERT INTO public.{{prefix}}job (args, kind, queue, state, attempt, attempted_at, max_attempts)
            VALUES ('"oxbow:enc:v1:tampered"', 'secret', 'default', 'running', 1, now() - interval '1 hour', 2)
            RETURNING id
            """);

        var job = await app.WaitForStateAsync(id, JobState.Discarded);
        Assert.Contains("rescued", job.Errors[0].Error, StringComparison.Ordinal);
        Assert.StartsWith(nameof(JobArgsDecryptionException), job.Errors[1].Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_rescuer_leaves_a_recent_job_it_cannot_decrypt_to_the_default_timeout()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, o =>
        {
            o.RescueStuckJobsAfter = TimeSpan.FromSeconds(1);
            o.JobTimeout = TimeSpan.FromHours(1);
        });

        // Another node may hold the key and still be running it.
        var id = await database.ScalarAsOwnerAsync<long>($$"""
            INSERT INTO public.{{prefix}}job (args, kind, queue, state, attempt, attempted_at, max_attempts)
            VALUES ('"oxbow:enc:v1:tampered"', 'secret', 'default', 'running', 1, now() - interval '5 seconds', 2)
            RETURNING id
            """);

        await Task.Delay(TimeSpan.FromSeconds(2));
        var job = await app.Client.GetAsync(id);
        Assert.Equal(JobState.Running, job!.State);
        Assert.Empty(job.Errors);
    }

    [Fact]
    public async Task Inserting_an_encrypted_kind_without_a_protector_throws()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await TestApp.StartAsync(database, prefix);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => app.Client.InsertAsync(new SecretArgs("x@example.com")));
        Assert.Contains("AddArgsEncryption", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OxbowAssert_decrypts_with_the_protector()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix, start: false);

        await using var connection = await app.DataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await app.Client.InsertAsync(connection, transaction, new SecretArgs("ada@example.com"));

        var assert = new OxbowAssert(tablePrefix: prefix) { ArgsProtector = Protector(app) };
        var job = await assert.InsertedAsync<SecretArgs>(connection, transaction, new() { Args = a => a.Email == "ada@example.com" });
        Assert.Equal("ada@example.com", job.Args.Email);

        await Assert.ThrowsAsync<OxbowAssertionException>(() => new OxbowAssert(tablePrefix: prefix).InsertedAsync<SecretArgs>(connection, transaction));
    }

    [Fact]
    public async Task Batch_members_and_callbacks_are_stored_as_ciphertext()
    {
        var prefix = await database.CreateSchemaAsync();
        await using var app = await StartAsync(prefix);

        await app.Client.InsertBatchAsync(
            [new InsertManyItem(new SecretArgs("member@example.com"))],
            new BatchOptions { OnComplete = new BatchCallback(new SecretArgs("callback@example.com")) });
        Assert.Equal(0L, await database.ScalarAsOwnerAsync<long>(
            $"SELECT count(*) FROM public.{prefix}batch WHERE on_complete::text LIKE '%example.com%'"));

        await WaitForEventsAsync(app, 2);
        Assert.Contains("callback@example.com", app.Recorder.Events);
        Assert.Equal(0L, await database.ScalarAsOwnerAsync<long>(
            $"SELECT count(*) FROM public.{prefix}job WHERE args::text LIKE '%example.com%'"));
    }

    private Task<TestApp> StartAsync(string prefix, Action<OxbowOptions>? configure = null, bool start = true) =>
        TestApp.StartAsync(
            database,
            prefix,
            o =>
            {
                o.Queues["default"] = new QueueOptions();
                o.SkipUnknownKindCheck = true;
                configure?.Invoke(o);
            },
            b =>
            {
                b.Services.AddDataProtection().SetApplicationName("oxbow-tests").PersistKeysToFileSystem(_keys);
                b.AddArgsEncryption(o => o.UniqueKeySecret = Secret)
                    .AddWorker<SecretArgs, SecretWorker>()
                    .AddWorker<RecordArgs, RecordWorker>()
                    .AddWorker<RetryAfterArgs, RetryAfterWorker>();
            },
            start: start);

    private static IJobArgsProtector Protector(TestApp app) => app.Host.Services.GetRequiredService<IJobArgsProtector>();

    private static async Task WaitForEventsAsync(TestApp app, int count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (app.Recorder.Events.Count < count)
        {
            await Task.Delay(50, cts.Token);
        }
    }

    /// <summary>Data Protection payloads start with a 4-byte magic header, then the key ID.</summary>
    private static Guid KeyIdOf(JobRow job)
    {
        var payload = job.Args.GetString()!["oxbow:enc:v1:".Length..].Replace('-', '+').Replace('_', '/');
        var bytes = Convert.FromBase64String(payload.PadRight(payload.Length + ((4 - (payload.Length % 4)) % 4), '='));
        return new Guid(bytes.AsSpan(4, 16));
    }
}

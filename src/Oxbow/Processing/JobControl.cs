using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using Oxbow.Internal;

namespace Oxbow.Processing;

/// <summary>
/// One attempt's heartbeats, progress, steps and attempt locks. Each is guarded by the attempt's <c>attempted_at</c> and
/// the running state, so a finished, rescued or superseded attempt can't write, except the result of a step it had already
/// started, if no result is recorded yet.
/// </summary>
internal sealed class JobControl(OxbowRuntime runtime, JobRow row, TimeProvider timeProvider) : IJobRuntime
{
    private static readonly TimeSpan MaxHeartbeatInterval = TimeSpan.FromSeconds(30);

    private const long NeverWritten = long.MinValue;

    private static readonly TimeSpan AttemptLockPollInterval = TimeSpan.FromMilliseconds(100);

    private readonly DateTimeOffset _attemptedAt = row.AttemptedAt ?? throw new ArgumentException("A fetched job has an attempted_at.", nameof(row));

    private long _lastWrite = NeverWritten;

    private volatile bool _locked;

    public DateTimeOffset UtcNow => timeProvider.GetUtcNow();

    // Each write updates a hot row; a tenth of the rescue horizon keeps the rescuer's view fresh enough.
    private TimeSpan HeartbeatInterval =>
        TimeSpan.FromTicks(Math.Min(MaxHeartbeatInterval.Ticks, runtime.Options.RescueStuckJobsAfter.Ticks / 10));

    public async Task HeartbeatAsync(CancellationToken cancellationToken)
    {
        var last = Interlocked.Read(ref _lastWrite);
        var now = timeProvider.GetTimestamp();
        if ((last != NeverWritten && timeProvider.GetElapsedTime(last, now) < HeartbeatInterval)
            || Interlocked.CompareExchange(ref _lastWrite, now, last) != last)
        {
            return;
        }

        var written = false;
        try
        {
            written = await WriteAsync(runtime.Sql.Heartbeat, null, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // A skipped write (the row was locked) mustn't hold off the next heartbeat.
            if (!written)
            {
                Interlocked.CompareExchange(ref _lastWrite, last, now);
            }
        }
    }

    public async Task ReportProgressAsync(double percent, string? message, CancellationToken cancellationToken)
    {
        var written = await WriteAsync(runtime.Sql.ReportProgress, command =>
        {
            command.Parameters.AddWithValue("percent", percent);
            command.Parameters.Add(new NpgsqlParameter("message", NpgsqlDbType.Text) { Value = (object?)message ?? DBNull.Value });
        }, cancellationToken).ConfigureAwait(false);
        if (written)
        {
            Interlocked.Exchange(ref _lastWrite, timeProvider.GetTimestamp());
        }
    }

    public async Task<T> StepAsync<T>(string name, Func<CancellationToken, Task<T>> run, CancellationToken cancellationToken)
    {
        var typeInfo = (JsonTypeInfo<T>)runtime.Json.GetTypeInfo(typeof(T));
        await HeartbeatAsync(cancellationToken).ConfigureAwait(false);
        string? recorded;
        var command = runtime.DataSource.CreateCommand(runtime.Sql.StepGet);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("id", row.Id);
            command.Parameters.AddWithValue("name", name);
            command.Parameters.AddWithValue("attempted_at", _attemptedAt);
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                if (!reader.GetBoolean(1))
                {
                    throw new JobAttemptSupersededException(row.Id);
                }

                recorded = reader.IsDBNull(0) ? null : reader.GetString(0);
            }
        }

        if (recorded is not null)
        {
            return ReadStep(name, recorded, typeInfo);
        }

        var result = await run(cancellationToken).ConfigureAwait(false);
        var (current, first) = await RecordStepAsync(name, WriteStep(name, result, typeInfo), cancellationToken)
            .ConfigureAwait(false);
        if (!current)
        {
            throw new JobAttemptSupersededException(row.Id);
        }

        return first is null ? result : ReadStep(name, first, typeInfo);
    }

    /// <summary>
    /// Records <paramref name="output"/> unless a result is recorded already. Returns whether this attempt is still the
    /// running one, and the recorded result if it isn't <paramref name="output"/>.
    /// </summary>
    private async Task<(bool Current, string? First)> RecordStepAsync(
        string name,
        string output,
        CancellationToken cancellationToken)
    {
        var lockAttempt = new NpgsqlBatchCommand(runtime.Sql.StepRecordLock);
        lockAttempt.Parameters.AddWithValue("id", row.Id);
        lockAttempt.Parameters.AddWithValue("attempted_at", _attemptedAt);
        var insert = new NpgsqlBatchCommand(runtime.Sql.StepRecordInsert);
        insert.Parameters.AddWithValue("id", row.Id);
        insert.Parameters.AddWithValue("name", name);
        insert.Parameters.Add(new NpgsqlParameter("output", NpgsqlDbType.Text) { Value = output });
        var read = new NpgsqlBatchCommand(runtime.Sql.StepRecordRead);
        read.Parameters.AddWithValue("id", row.Id);
        read.Parameters.AddWithValue("name", name);

        var batch = runtime.DataSource.CreateBatch();
        await using (batch.ConfigureAwait(false))
        {
            batch.BatchCommands.Add(lockAttempt);
            batch.BatchCommands.Add(insert);
            batch.BatchCommands.Add(read);
            try
            {
                var reader = await batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var current = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                    await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
                    var inserted = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                    await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
                    await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                    return (current, inserted ? null : reader.GetString(0));
                }
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ForeignKeyViolation)
            {
                throw new JobAttemptSupersededException(row.Id);
            }
        }
    }

    public async Task LockAttemptAsync(DbTransaction transaction, CancellationToken cancellationToken)
    {
        if (transaction is not NpgsqlTransaction { Connection: { } connection } npgsqlTransaction)
        {
            throw new ArgumentException("The attempt can only be locked in an open NpgsqlTransaction on Oxbow's database.", nameof(transaction));
        }

        var command = new NpgsqlCommand(runtime.Sql.LockAttempt, connection, npgsqlTransaction);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("id", row.Id);
            command.Parameters.AddWithValue("attempted_at", _attemptedAt);
            try
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.DivisionByZero)
            {
                throw new JobAttemptSupersededException(row.Id);
            }
        }

        _locked = true;
    }

    /// <summary>
    /// After the worker returns from an attempt that locked itself: waits while the job's row is still locked, likely by a
    /// transaction the worker left open, so that the completer's batch of other jobs' outcomes doesn't wait on it instead.
    /// </summary>
    public async Task WaitForAttemptLockAsync(ILogger logger, CancellationToken cancellationToken)
    {
        if (!_locked)
        {
            return;
        }

        var warned = false;
        try
        {
            while (await AttemptLockHeldAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!warned)
                {
                    Log.AttemptLockHeld(logger, row.Id);
                    warned = true;
                }

                await Task.Delay(AttemptLockPollInterval, timeProvider, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
#pragma warning disable CA1031 // The probe is a courtesy to other jobs; the outcome is written either way.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            Log.AttemptLockProbeFailed(logger, ex, row.Id);
        }
    }

    private async Task<bool> AttemptLockHeldAsync(CancellationToken cancellationToken)
    {
        var command = runtime.DataSource.CreateCommand(runtime.Sql.AttemptLockHeld);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("id", row.Id);
            return (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }
    }

    // Whether a job's step results are encrypted follows its args, never the shape of a result: a plaintext result may
    // look like an envelope.
    private bool Encrypted => ArgsEnvelope.IsSealed(row.Args);

    private string WriteStep<T>(string name, T result, JsonTypeInfo<T> typeInfo)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(result, typeInfo);
        if (bytes.Length > JobRow.MaxOutputSize)
        {
            throw new InvalidOperationException(string.Format(
                CultureInfo.InvariantCulture,
                "The result of step '{0}' is {1:N0} bytes of JSON, over the {2:N0}-byte limit (JobRow.MaxOutputSize).",
                name,
                bytes.Length,
                JobRow.MaxOutputSize));
        }

        var json = Encoding.UTF8.GetString(bytes);
        return Encrypted ? ArgsEnvelope.Seal(runtime.ArgsProtector!.ProtectStepResult(row.Kind, json)) : json;
    }

    private T ReadStep<T>(string name, string stored, JsonTypeInfo<T> typeInfo)
    {
        using var document = JsonDocument.Parse(stored);
        if (!Encrypted)
        {
            return document.RootElement.Deserialize(typeInfo)!;
        }

        if (document.RootElement.ValueKind != JsonValueKind.String
            || document.RootElement.GetString() is not { } envelope
            || !envelope.StartsWith(ArgsEnvelope.Prefix, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"The recorded result of job {row.Id}'s step '{name}' isn't encrypted, but the job's args are, so Oxbow didn't record it.");
        }

        string plaintext;
        try
        {
            plaintext = runtime.ArgsProtector!.UnprotectStepResult(row.Kind, envelope[ArgsEnvelope.Prefix.Length..]);
        }
        catch (Exception ex) when (ex is not NotSupportedException)
        {
            throw new InvalidOperationException($"The recorded result of job {row.Id}'s step '{name}' can't be decrypted: {ex.Message}", ex);
        }

        return JsonSerializer.Deserialize(plaintext, typeInfo)!;
    }

    /// <summary>Runs an attempt-guarded write; false if it was skipped, because the attempt isn't current or its row is locked.</summary>
    private async Task<bool> WriteAsync(string sql, Action<NpgsqlCommand>? addParameters, CancellationToken cancellationToken)
    {
        var command = runtime.DataSource.CreateCommand(sql);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("id", row.Id);
            command.Parameters.AddWithValue("attempted_at", _attemptedAt);
            addParameters?.Invoke(command);
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }
    }
}

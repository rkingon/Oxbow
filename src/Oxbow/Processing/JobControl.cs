using Npgsql;
using NpgsqlTypes;
using Oxbow.Internal;

namespace Oxbow.Processing;

/// <summary>
/// Writes one attempt's heartbeat and progress. Each write is guarded by the attempt's <c>attempted_at</c> and the
/// running state, so a finished, rescued or superseded attempt can't write.
/// </summary>
internal sealed class JobControl(OxbowRuntime runtime, JobRow row, TimeProvider timeProvider) : IJobRuntime
{
    private static readonly TimeSpan MaxHeartbeatInterval = TimeSpan.FromSeconds(30);

    private const long NeverWritten = long.MinValue;

    private long _lastWrite = NeverWritten;

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

        try
        {
            await WriteAsync(runtime.Sql.Heartbeat, null, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            Interlocked.CompareExchange(ref _lastWrite, last, now);
            throw;
        }
    }

    public async Task ReportProgressAsync(double percent, string? message, CancellationToken cancellationToken)
    {
        await WriteAsync(runtime.Sql.ReportProgress, command =>
        {
            command.Parameters.AddWithValue("percent", percent);
            command.Parameters.Add(new NpgsqlParameter("message", NpgsqlDbType.Text) { Value = (object?)message ?? DBNull.Value });
        }, cancellationToken).ConfigureAwait(false);
        Interlocked.Exchange(ref _lastWrite, timeProvider.GetTimestamp());
    }

    private async Task WriteAsync(string sql, Action<NpgsqlCommand>? addParameters, CancellationToken cancellationToken)
    {
        if (row.AttemptedAt is not { } attemptedAt)
        {
            return;
        }

        var command = runtime.DataSource.CreateCommand(sql);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("id", row.Id);
            command.Parameters.AddWithValue("attempted_at", attemptedAt);
            addParameters?.Invoke(command);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Npgsql;
using Oxbow.Internal;

namespace Oxbow.Processing;

/// <summary>River's leader election: one row in the leader table, held by re-electing before its TTL runs out.</summary>
internal sealed class Elector(OxbowRuntime runtime, ILogger logger)
{
    private readonly TimeSpan _electInterval = runtime.Options.ElectInterval;
    private static readonly TimeSpan TrustMargin = TimeSpan.FromSeconds(1);

    private TimeSpan Ttl => _electInterval + TimeSpan.FromSeconds(10);

    private readonly Channel<bool> _wake = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    public bool IsLeader { get; private set; }

    /// <summary>A leader resigned; try to take over now rather than at the next interval.</summary>
    public void OnLeadershipNotification(string payload)
    {
        var notification = JsonSerializer.Deserialize(payload, OxbowJsonContext.Default.ControlPayload);
        if (notification?.Action == "resigned")
        {
            _wake.Writer.TryWrite(true);
        }
    }

    public async Task RunAsync(Func<CancellationToken, Task> leaderWork, CancellationToken cancellationToken)
    {
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            DateTimeOffset? electedAt;
            var attemptStarted = Stopwatch.GetTimestamp();
            try
            {
                electedAt = await AttemptElectAsync(cancellationToken).ConfigureAwait(false);
                failures = 0;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Log.ElectionFailed(logger, ex);
                await Backoff.DelayAsync(Backoff.For(++failures), cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (electedAt is null)
            {
                await WaitForNextAttemptAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            await LeadAsync(electedAt.Value, attemptStarted, leaderWork, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task LeadAsync(
        DateTimeOffset electedAt,
        long attemptStarted,
        Func<CancellationToken, Task> leaderWork,
        CancellationToken cancellationToken)
    {
        IsLeader = true;
        Log.GainedLeadership(logger, runtime.ClientId);
        using var leaderSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var work = Task.Run(() => leaderWork(leaderSource.Token), CancellationToken.None);
        var trustedUntil = attemptStarted + ToTimestamp(Ttl - TrustMargin);
        try
        {
            while (true)
            {
                await Task.Delay(_electInterval, cancellationToken).ConfigureAwait(false);
                var reelectStarted = Stopwatch.GetTimestamp();
                try
                {
                    if (!await AttemptReelectAsync(electedAt, cancellationToken).ConfigureAwait(false))
                    {
                        break;
                    }

                    trustedUntil = reelectStarted + ToTimestamp(Ttl - TrustMargin);
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    Log.ElectionFailed(logger, ex);
                    if (Stopwatch.GetTimestamp() >= trustedUntil)
                    {
                        break;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }

        await leaderSource.CancelAsync().ConfigureAwait(false);
        try
        {
            await work.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
#pragma warning disable CA1031 // A failed term must not stop the elector; the next term restarts maintenance.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            Log.MaintenanceFailed(logger, ex, "leader");
        }

        IsLeader = false;
        Log.LostLeadership(logger, runtime.ClientId);
        if (cancellationToken.IsCancellationRequested)
        {
            await ResignAsync(electedAt).ConfigureAwait(false);
        }
    }

    private async Task WaitForNextAttemptAsync(CancellationToken cancellationToken)
    {
        using var delay = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        delay.CancelAfter(Backoff.Jitter(_electInterval, 0.2));
        try
        {
            await _wake.Reader.ReadAsync(delay.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task<DateTimeOffset?> AttemptElectAsync(CancellationToken cancellationToken)
    {
        var connection = await runtime.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                var deleteExpired = new NpgsqlCommand(runtime.Sql.LeaderDeleteExpired, connection, transaction);
                await using (deleteExpired.ConfigureAwait(false))
                {
                    await deleteExpired.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                var elect = new NpgsqlCommand(runtime.Sql.LeaderAttemptElect, connection, transaction);
                await using (elect.ConfigureAwait(false))
                {
                    elect.Parameters.AddWithValue("leader_id", runtime.ClientId);
                    elect.Parameters.AddWithValue("ttl", Ttl.TotalSeconds);
                    var electedAt = await elect.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                    return electedAt is DateTime at ? new DateTimeOffset(at, TimeSpan.Zero) : null;
                }
            }
        }
    }

    private async Task<bool> AttemptReelectAsync(DateTimeOffset electedAt, CancellationToken cancellationToken)
    {
        var command = runtime.DataSource.CreateCommand(runtime.Sql.LeaderAttemptReelect);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddWithValue("leader_id", runtime.ClientId);
            command.Parameters.AddWithValue("elected_at", electedAt);
            command.Parameters.AddWithValue("ttl", Ttl.TotalSeconds);
            return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not null;
        }
    }

    private async Task ResignAsync(DateTimeOffset electedAt)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            var command = runtime.DataSource.CreateCommand(runtime.Sql.LeaderResign);
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.AddWithValue("leader_id", runtime.ClientId);
                command.Parameters.AddWithValue("elected_at", electedAt);
                command.Parameters.AddWithValue("channel", runtime.Sql.LeadershipChannel);
                await command.ExecuteNonQueryAsync(timeout.Token).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is NpgsqlException or OperationCanceledException)
        {
            // The row expires on its own; resigning only speeds up the handover.
            Log.ElectionFailed(logger, ex);
        }
    }

    private static long ToTimestamp(TimeSpan duration) => (long)(duration.TotalSeconds * Stopwatch.Frequency);
}

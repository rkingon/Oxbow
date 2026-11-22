using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Npgsql;
using Oxbow.Internal;

namespace Oxbow.Processing;

/// <summary>One dedicated connection LISTENing on Oxbow's channels, reconnecting with backoff.</summary>
internal sealed class Notifier(OxbowRuntime runtime, ILogger logger)
{
    private static readonly TimeSpan PingInterval = TimeSpan.FromSeconds(30);
    private readonly ConcurrentDictionary<string, Action<string>> _handlers = new(StringComparer.Ordinal);

    /// <summary>Raised after every (re)connect, since notifications sent while disconnected are lost.</summary>
    public event Action? Connected;

    public void Subscribe(string channel, Action<string> handler) => _handlers[channel] = handler;

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var connection = await runtime.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                await using (connection.ConfigureAwait(false))
                {
                    connection.Notification += (_, e) =>
                    {
                        if (_handlers.TryGetValue(e.Channel, out var handler))
                        {
                            handler(e.Payload);
                        }
                    };

                    foreach (var channel in _handlers.Keys)
                    {
                        var listen = new NpgsqlCommand($"LISTEN {Identifiers.Quote(channel)}", connection);
                        await using (listen.ConfigureAwait(false))
                        {
                            await listen.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                        }
                    }

                    failures = 0;
                    Connected?.Invoke();
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        if (!await connection.WaitAsync(PingInterval, cancellationToken).ConfigureAwait(false))
                        {
                            var ping = new NpgsqlCommand("SELECT 1", connection);
                            await using (ping.ConfigureAwait(false))
                            {
                                await ping.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                var delay = Backoff.For(++failures);
                Log.NotifierFailed(logger, ex, delay);
                await Backoff.DelayAsync(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}

using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Oxbow.Internal;

/// <summary>The validated, resolved configuration every component shares.</summary>
internal sealed class OxbowRuntime
{
    public OxbowRuntime(IOptions<OxbowOptions> options, IServiceProvider services)
    {
        Options = options.Value;
        Sql = new OxbowSql(Options.Schema, Options.TablePrefix);
        DataSource = (Options.DataSourceServiceKey is null
                ? services.GetService<NpgsqlDataSource>()
                : services.GetKeyedService<NpgsqlDataSource>(Options.DataSourceServiceKey))
            ?? throw new InvalidOperationException(
                "Oxbow needs an NpgsqlDataSource in DI. Register one, e.g. services.AddNpgsqlDataSource(connectionString), " +
                "or set OxbowOptions.DataSourceServiceKey to a keyed registration.");

        var json = new JsonSerializerOptions(Options.JsonSerializerOptions);
        json.TypeInfoResolver ??= new DefaultJsonTypeInfoResolver();
        json.MakeReadOnly();
        Json = json;

        var clientId = Options.ClientId ?? $"{Environment.MachineName}_{Guid.NewGuid():N}";
        ClientId = clientId.Length > 120 ? clientId[..120] : clientId;
    }

    public OxbowOptions Options { get; }

    public OxbowSql Sql { get; }

    public NpgsqlDataSource DataSource { get; }

    public JsonSerializerOptions Json { get; }

    public string ClientId { get; }

    public static bool IsInfinite(TimeSpan value) => value == Timeout.InfiniteTimeSpan;
}

using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Oxbow;
using Oxbow.DataProtection;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers args encryption on ASP.NET Core Data Protection.</summary>
public static class OxbowDataProtectionBuilderExtensions
{
    /// <summary>
    /// Encrypts the args of kinds marked <see cref="EncryptArgsAttribute"/>, or of every kind with
    /// <see cref="OxbowOptions.EncryptAllArgs"/>, with ASP.NET Core Data Protection. Needs an
    /// <see cref="IDataProtectionProvider"/> in DI (<c>services.AddDataProtection()</c>) whose key ring every node
    /// shares.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="configure">Configures the options.</param>
    /// <returns>The builder.</returns>
    public static IOxbowBuilder AddArgsEncryption(this IOxbowBuilder builder, Action<ArgsEncryptionOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var options = builder.Services.AddOptions<ArgsEncryptionOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        builder.Services.TryAddSingleton<IJobArgsProtector>(sp => new DataProtectionJobArgsProtector(
            sp.GetService<IDataProtectionProvider>() ?? throw new InvalidOperationException(
                "Oxbow args encryption needs Data Protection. Call services.AddDataProtection() and persist the key ring " +
                "where every node can read it."),
            sp.GetRequiredService<IOptions<ArgsEncryptionOptions>>().Value.UniqueKeySecret));
        return builder;
    }
}

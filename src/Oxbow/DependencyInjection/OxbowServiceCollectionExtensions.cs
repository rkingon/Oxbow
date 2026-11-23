using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Oxbow;
using Oxbow.Internal;
using Oxbow.Processing;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers Oxbow.</summary>
public static class OxbowServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Oxbow client and, if queues are configured, the processor that works them. Needs an
    /// <c>NpgsqlDataSource</c> in DI.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configures the options.</param>
    /// <returns>A builder for registering workers and middleware.</returns>
    public static IOxbowBuilder AddOxbow(this IServiceCollection services, Action<OxbowOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = services.AddOptions<OxbowOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<OxbowOptions>, OxbowOptionsValidator>());
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IRetryPolicy, DefaultRetryPolicy>();
        services.TryAddSingleton<OxbowRuntime>();
        services.TryAddSingleton<WorkerRegistry>();
        services.TryAddSingleton<OxbowClient>();
        services.TryAddSingleton<IOxbowClient>(sp => sp.GetRequiredService<OxbowClient>());
        services.AddHostedService<OxbowHostedService>();
        return new OxbowBuilder(services);
    }

    private sealed class OxbowBuilder(IServiceCollection services) : IOxbowBuilder
    {
        public IServiceCollection Services { get; } = services;
    }
}

/// <summary>Registers Oxbow workers and middleware.</summary>
public interface IOxbowBuilder
{
    /// <summary>The service collection.</summary>
    IServiceCollection Services { get; }
}

/// <summary>Worker and middleware registration for <see cref="IOxbowBuilder"/>.</summary>
public static class OxbowBuilderExtensions
{
    /// <summary>Registers a scoped worker for <typeparamref name="TArgs"/>, resolved in a new scope for each job.</summary>
    /// <typeparam name="TArgs">The args type, which must carry a <see cref="JobKindAttribute"/>.</typeparam>
    /// <typeparam name="TWorker">The worker.</typeparam>
    /// <param name="builder">The builder.</param>
    /// <returns>The builder.</returns>
    public static IOxbowBuilder AddWorker<TArgs, TWorker>(this IOxbowBuilder builder)
        where TArgs : IJobArgs
        where TWorker : class, IWorker<TArgs>
    {
        ArgumentNullException.ThrowIfNull(builder);
        _ = JobKinds.Get(typeof(TArgs));
        builder.Services.TryAddScoped<TWorker>();
        builder.Services.AddSingleton<WorkerRegistration>(new WorkerRegistration<TArgs, TWorker>());
        return builder;
    }

    /// <summary>Adds scoped work middleware. Middleware runs in registration order, outermost first.</summary>
    /// <typeparam name="TMiddleware">The middleware.</typeparam>
    /// <param name="builder">The builder.</param>
    /// <returns>The builder.</returns>
    public static IOxbowBuilder AddWorkMiddleware<TMiddleware>(this IOxbowBuilder builder)
        where TMiddleware : class, IJobWorkMiddleware
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddScoped<IJobWorkMiddleware, TMiddleware>();
        return builder;
    }

    /// <summary>
    /// Adds a scoped handler called after a job is discarded or cancelled, in a new scope for each job. Handlers run in
    /// registration order, on a background loop of the node that wrote the outcome.
    /// </summary>
    /// <typeparam name="THandler">The handler.</typeparam>
    /// <param name="builder">The builder.</param>
    /// <returns>The builder.</returns>
    public static IOxbowBuilder AddDiscardHandler<THandler>(this IOxbowBuilder builder)
        where THandler : class, IJobDiscardHandler
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddScoped<THandler>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<DiscardHandlerRegistration, DiscardHandlerRegistration<THandler>>());
        return builder;
    }

    /// <summary>Adds singleton insert middleware. Middleware runs in registration order, outermost first.</summary>
    /// <typeparam name="TMiddleware">The middleware.</typeparam>
    /// <param name="builder">The builder.</param>
    /// <returns>The builder.</returns>
    public static IOxbowBuilder AddInsertMiddleware<TMiddleware>(this IOxbowBuilder builder)
        where TMiddleware : class, IJobInsertMiddleware
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddSingleton<IJobInsertMiddleware, TMiddleware>();
        return builder;
    }
}

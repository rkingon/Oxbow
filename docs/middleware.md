# Middleware

Middleware wraps inserts and work the way ASP.NET Core middleware wraps requests. Logging, tenancy and trace
propagation belong here, in your code.

## Work middleware

```csharp
public sealed class TenantMiddleware(TenantContext tenant) : IJobWorkMiddleware
{
    public async Task InvokeAsync(JobContext context, JobWorkDelegate next)
    {
        tenant.Id = context.Job.Metadata.GetProperty("tenant").GetString();
        await next(context);
    }
}

services.AddOxbow(...).AddWorkMiddleware<TenantMiddleware>();
```

Work middleware is scoped and resolved from the job's scope, so it shares scoped services (such as `TenantContext` or
a `DbContext`) with the worker. Exceptions it throws count as the attempt's outcome.

## Insert middleware

```csharp
public sealed class StampTenant(ITenantAccessor tenants) : IJobInsertMiddleware
{
    public Task<IReadOnlyList<InsertResult>> InvokeAsync(JobInsertContext context, JobInsertDelegate next)
    {
        foreach (var job in context.Jobs)
            job.Metadata["tenant"] = tenants.Current;
        return next(context);
    }
}

services.AddOxbow(...).AddInsertMiddleware<StampTenant>();
```

Insert middleware is a singleton by default. To depend on scoped services, register it with a lifetime:
`AddInsertMiddleware<StampTenant>(ServiceLifetime.Scoped)`. It's then resolved from the inserting scope when inserting
through the scoped `IOxbowClient` (see [abstractions](abstractions.md#scoped-insert-middleware)), and from a scope of
its own otherwise, such as the leader's periodic runs.

Insert middleware sees each batch once. It may change metadata, tags, queue and priority. The
unique key is computed before middleware runs.

Both kinds run in registration order, the first registered outermost.

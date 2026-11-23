# Oxbow.Abstractions

The contracts of [Oxbow](https://github.com/rkingon/Oxbow) with no dependencies beyond the BCL: job args, workers,
`IOxbowClient` and the read models. Reference it from domain and application projects, and `Oxbow` from the host.

```csharp
[JobKind("ship_order")]
public sealed record ShipOrder(long OrderId) : IJobArgs;

public sealed class ShipOrderWorker : IWorker<ShipOrder>
{
    public Task WorkAsync(Job<ShipOrder> job, CancellationToken cancellationToken) => ShipAsync(job.Args.OrderId, cancellationToken);
}

public sealed class PlaceOrder(IOxbowClient oxbow)
{
    public Task ExecuteAsync(long orderId) => oxbow.InsertAsync(new ShipOrder(orderId));
}
```

In the host, `AddDbContextTransaction<TContext>()` from `Oxbow.EntityFrameworkCore` makes `InsertAsync` join the
unit of work's transaction. See [layering](https://github.com/rkingon/Oxbow/blob/main/docs/abstractions.md).

using System.Collections.Concurrent;

namespace Oxbow.Abstractions.Sample;

[JobKind("sample_ship_order")]
public sealed record ShipOrder(long OrderId) : IJobArgs;

public sealed class ShippedOrders
{
    public ConcurrentQueue<long> Ids { get; } = new();
}

public sealed class ShipOrderWorker(ShippedOrders shipped) : IWorker<ShipOrder>
{
    public async Task WorkAsync(Job<ShipOrder> job, CancellationToken cancellationToken)
    {
        await job.HeartbeatAsync(cancellationToken);
        shipped.Ids.Enqueue(job.Args.OrderId);
        await job.ReportProgressAsync(100, "shipped", cancellationToken);
        job.SetOutput(new { job.Args.OrderId });
    }
}

/// <summary>A use case: enqueues through the port, in whatever unit of work the host provides.</summary>
public sealed class PlaceOrder(IOxbowClient oxbow)
{
    public async Task<long> ExecuteAsync(long orderId, CancellationToken cancellationToken = default)
    {
        var result = await oxbow.InsertAsync(new ShipOrder(orderId), new InsertOptions { Tags = ["orders"] }, cancellationToken);
        return result.Job.Id;
    }
}

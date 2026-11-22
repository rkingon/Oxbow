namespace Oxbow;

/// <summary>One job in a bulk insert.</summary>
/// <param name="Args">The job's args.</param>
/// <param name="Options">Insert options for this job.</param>
public sealed record InsertManyItem(IJobArgs Args, InsertOptions? Options = null);

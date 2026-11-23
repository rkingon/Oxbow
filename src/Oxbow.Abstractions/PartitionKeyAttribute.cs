namespace Oxbow;

/// <summary>
/// Includes a property in the job's partition key when <see cref="InsertOptions.PartitionKey"/> isn't set. The key
/// is the marked top-level properties as a JSON object, sorted by name, so jobs whose marked values are equal share a
/// partition. On a positional record, apply it as <c>[property: PartitionKey]</c>.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class PartitionKeyAttribute : Attribute;

namespace Oxbow;

/// <summary>
/// Puts the job in a sequence keyed by the kind and the values of the properties carrying this attribute, unless
/// <see cref="InsertOptions.SequenceKey"/> is set. Only top-level properties count. On a positional record, apply it as
/// <c>[property: SequenceKey]</c>.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class SequenceKeyAttribute : Attribute;

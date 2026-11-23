namespace Oxbow;

/// <summary>
/// Includes a property in the unique key when <see cref="UniqueOptions.ByArgs"/> is set. If no property of the args
/// carries it, the whole args object is used. On a positional record, apply it as <c>[property: UniqueKey]</c>.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class UniqueKeyAttribute : Attribute;

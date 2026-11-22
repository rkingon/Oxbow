namespace Oxbow;

/// <summary>
/// Declares the job kind stored for an <see cref="IJobArgs"/> type. Kinds are stable identifiers: renaming the class
/// doesn't change them, so queued jobs are never orphaned.
/// </summary>
/// <param name="kind">The kind, 1 to 127 characters.</param>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class JobKindAttribute(string kind) : Attribute
{
    /// <summary>The job kind.</summary>
    public string Kind { get; } = kind;

    /// <summary>
    /// Earlier kinds this type also answers to, so jobs inserted before a rename are still worked.
    /// </summary>
    public string[] Aliases { get; init; } = [];
}

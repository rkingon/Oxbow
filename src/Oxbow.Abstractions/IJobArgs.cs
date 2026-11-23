namespace Oxbow;

/// <summary>
/// Marks a type as the arguments of a job. Implementations must carry a <see cref="JobKindAttribute"/>.
/// </summary>
/// <remarks>
/// Args are serialized with System.Text.Json using <c>OxbowOptions.JsonSerializerOptions</c>.
/// </remarks>
public interface IJobArgs
{
    /// <summary>
    /// Per-kind insert defaults. Options passed to an insert call take precedence; client defaults apply last.
    /// </summary>
    /// <returns>The defaults, or <see langword="null"/> for none.</returns>
    InsertOptions? GetInsertOptions() => null;
}

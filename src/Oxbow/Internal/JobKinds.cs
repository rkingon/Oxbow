using System.Collections.Concurrent;
using System.Reflection;

namespace Oxbow.Internal;

internal static class JobKinds
{
    private static readonly ConcurrentDictionary<Type, JobKindAttribute> Cache = new();

    public static JobKindAttribute Get(Type argsType) => Cache.GetOrAdd(argsType, static type =>
    {
        var attribute = type.GetCustomAttribute<JobKindAttribute>(inherit: false)
            ?? throw new InvalidOperationException(
                $"Job args type '{type.FullName}' has no [JobKind] attribute. Add one, e.g. [JobKind(\"send_email\")].");
        Validate(attribute.Kind, type);
        foreach (var alias in attribute.Aliases)
        {
            Validate(alias, type);
        }

        return attribute;
    });

    private static void Validate(string kind, Type type)
    {
        if (string.IsNullOrWhiteSpace(kind) || kind.Length >= 128)
        {
            throw new InvalidOperationException(
                $"Job kind '{kind}' on '{type.FullName}' must be 1 to 127 characters and not blank.");
        }
    }
}

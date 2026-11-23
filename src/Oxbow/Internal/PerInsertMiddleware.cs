namespace Oxbow.Internal;

/// <summary>Registered when any insert middleware isn't a singleton, so the list is resolved per insert.</summary>
internal sealed class PerInsertMiddleware;

namespace Oxbow;

/// <summary>A job's kind has no registered worker.</summary>
public sealed class UnknownJobKindException : Exception
{
    /// <summary>Creates the exception for <paramref name="kind"/>.</summary>
    /// <param name="kind">The unknown kind.</param>
    public UnknownJobKindException(string kind)
        : base($"No worker is registered for job kind '{kind}'.")
    {
        Kind = kind;
    }

    /// <summary>The unknown kind.</summary>
    public string Kind { get; }
}

/// <summary>A periodic job with the name already exists, created at runtime or registered in code.</summary>
public sealed class PeriodicJobExistsException : Exception
{
    /// <summary>Creates the exception for <paramref name="name"/>.</summary>
    /// <param name="name">The periodic job's name.</param>
    public PeriodicJobExistsException(string name)
        : base($"A periodic job named '{name}' already exists. Replace it with UpsertPeriodicJobAsync, or choose another name.")
    {
        Name = name;
    }

    /// <summary>The periodic job's name.</summary>
    public string Name { get; }
}

/// <summary>
/// A job's args are encrypted and can't be decrypted: no <see cref="IJobArgsProtector"/> is registered, the key is
/// gone or revoked, or the payload was tampered with. Worked like args that fail to deserialize: retried, then discarded.
/// </summary>
public sealed class JobArgsDecryptionException : Exception
{
    /// <summary>Creates the exception.</summary>
    public JobArgsDecryptionException()
    {
    }

    /// <summary>Creates the exception with a message.</summary>
    /// <param name="message">What failed.</param>
    public JobArgsDecryptionException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and a cause.</summary>
    /// <param name="message">What failed.</param>
    /// <param name="innerException">The protector's error.</param>
    public JobArgsDecryptionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

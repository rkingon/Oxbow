using Npgsql;

namespace Oxbow;

/// <summary>
/// The app's current connection and transaction, for the scoped <see cref="IOxbowClient"/> to insert in. Register one with
/// <c>AddTransactionAccessor&lt;T&gt;()</c>, or <c>AddDbContextTransaction&lt;TContext&gt;()</c> from
/// <c>Oxbow.EntityFrameworkCore</c>. Resolved from the inserting scope on every insert.
/// </summary>
public interface IOxbowTransactionAccessor
{
    /// <summary>
    /// The open connection and its current transaction, or <see langword="null"/> when there is none: the insert then
    /// uses a transaction of its own, or throws if <see cref="OxbowOptions.RequireAmbientTransaction"/> is set.
    /// </summary>
    (NpgsqlConnection Connection, NpgsqlTransaction Transaction)? Current { get; }
}

using Microsoft.EntityFrameworkCore;
using Npgsql;
using Oxbow;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Joins inserts to a scoped <see cref="DbContext"/>'s transaction.</summary>
public static class OxbowDbContextTransactionExtensions
{
    /// <summary>
    /// Makes the scoped <see cref="IOxbowClient"/> insert in <typeparamref name="TContext"/>'s current transaction
    /// (<c>Database.CurrentTransaction</c>), so jobs commit or roll back with the scope's unit of work. With no transaction
    /// begun, inserts use their own, or throw if <see cref="OxbowOptions.RequireAmbientTransaction"/> is set. See
    /// <see cref="OxbowBuilderExtensions.AddTransactionAccessor{TAccessor}"/> for what making the client scoped means.
    /// </summary>
    /// <typeparam name="TContext">A context registered as scoped and configured with Npgsql.</typeparam>
    /// <param name="builder">The builder.</param>
    /// <returns>The builder.</returns>
    public static IOxbowBuilder AddDbContextTransaction<TContext>(this IOxbowBuilder builder)
        where TContext : DbContext =>
        builder.AddTransactionAccessor<DbContextTransactionAccessor<TContext>>();

    private sealed class DbContextTransactionAccessor<TContext>(TContext dbContext) : IOxbowTransactionAccessor
        where TContext : DbContext
    {
        public (NpgsqlConnection Connection, NpgsqlTransaction Transaction)? Current => OxbowDbContextExtensions.Current(dbContext);
    }
}

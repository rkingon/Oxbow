# Oxbow.EntityFrameworkCore

Entity Framework Core integration for [Oxbow](https://github.com/rkingon/Oxbow).

## Insert in the context's transaction

```csharp
await using var transaction = await db.Database.BeginTransactionAsync();
db.Users.Add(user);
await db.SaveChangesAsync();
await oxbow.InsertAsync(db, new SendWelcomeEmail(user.Id));
await transaction.CommitAsync();
```

Without a current transaction, `InsertAsync(db, …)` throws rather than committing the job on its own.

## Join the scoped context's transaction without passing it

```csharp
services.AddOxbow(o => o.RequireAmbientTransaction = true).AddDbContextTransaction<AppDbContext>();
```

`IOxbowClient` becomes scoped, and `InsertAsync(args)` inserts in the scope's `AppDbContext` transaction, so code that
references only `Oxbow.Abstractions` enqueues transactionally.

## Add the schema to your migrations

```csharp
public partial class AddOxbow : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddOxbowSchema(version: 1);
        migrationBuilder.GrantOxbowAccess("app_role", version: 1);
    }

    protected override void Down(MigrationBuilder migrationBuilder) =>
        migrationBuilder.RemoveOxbowSchema(version: 1);
}

public partial class UpgradeOxbowToV2 : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddOxbowSchema(version: 2);
        migrationBuilder.GrantOxbowAccess("app_role", version: 2);
    }

    protected override void Down(MigrationBuilder migrationBuilder) =>
        migrationBuilder.RemoveOxbowSchema(version: 2);
}
```

One EF migration per Oxbow schema version, each with that version's grants. Versions are always explicit, so a
migration replays the same SQL after you upgrade the package.

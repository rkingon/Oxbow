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

## Add the schema to your migrations

```csharp
public partial class AddOxbow : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddOxbowSchema(version: 1);
        migrationBuilder.GrantOxbowAccess("app_role");
    }

    protected override void Down(MigrationBuilder migrationBuilder) =>
        migrationBuilder.RemoveOxbowSchema(version: 1);
}
```

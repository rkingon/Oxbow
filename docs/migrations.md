# Migrations

Oxbow never runs DDL at runtime. You add its schema to your own migrations, and the client checks on startup that the
version it needs is installed. If it isn't, startup fails with the exact call that produces the missing SQL.

Every object Oxbow creates (tables, enum, function, indexes, constraints, NOTIFY channels) uses a configurable
**schema** (default `public`) and **prefix** (default `oxbow_`). Configure the same values in `AddOxbow`:

```csharp
services.AddOxbow(o => { o.Schema = "jobs"; o.TablePrefix = "oxbow_"; });
```

## Plain SQL

```csharp
foreach (var migration in OxbowSchema.GetMigrations(fromVersion: 0, toVersion: OxbowSchema.LatestVersion))
    File.WriteAllText($"V{migration.Version}__oxbow.sql", migration.Sql);
```

`GetMigrations(1, 0)` returns the down scripts. Each script records its version in `{prefix}migration`.

## EF Core

One EF migration per Oxbow schema version:

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.AddOxbowSchema(version: 1);
    migrationBuilder.GrantOxbowAccess("app_role");
}

protected override void Down(MigrationBuilder migrationBuilder) => migrationBuilder.RemoveOxbowSchema(version: 1);
```

## CLI

```shell
dotnet tool install --global Oxbow.Cli --prerelease
oxbow sql --from 0 --to 1 > oxbow_v1.sql
OXBOW_CONNECTION_STRING="Host=...;Username=owner;..." oxbow migrate
```

`oxbow migrate` reads the installed version and applies what's missing in one transaction. Run it as the schema
owner, not the runtime role.

## Upgrading Oxbow

When a release raises `OxbowSchema.LatestVersion`, its changelog says so. Add a migration with the new version's
script before deploying the release; the previous release keeps working against the newer schema.

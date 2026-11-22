# Oxbow.Cli

The `oxbow` command for [Oxbow](https://github.com/rkingon/Oxbow), for projects that don't use EF Core migrations.

```shell
dotnet tool install --global Oxbow.Cli --prerelease

oxbow sql > 001_oxbow.sql                         # print the schema SQL for your migration tool
oxbow grants --role app_role                      # print the least-privilege grants
OXBOW_CONNECTION_STRING="..." oxbow migrate       # or apply it directly, as the schema owner
```

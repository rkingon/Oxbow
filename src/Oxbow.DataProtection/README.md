# Oxbow.DataProtection

Encrypts [Oxbow](https://github.com/rkingon/Oxbow) job args at rest with ASP.NET Core Data Protection.

```csharp
[JobKind("send_statement"), EncryptArgs]
public sealed record SendStatement(string Email, string AccountNumber) : IJobArgs;

builder.Services.AddDataProtection().PersistKeysToDbContext<KeysContext>(); // a key ring every node shares
builder.Services.AddOxbow(o => o.Queues["default"] = new QueueOptions())
    .AddWorker<SendStatement, SendStatementWorker>()
    .AddArgsEncryption(o => o.UniqueKeySecret = Convert.FromBase64String(config["Oxbow:UniqueKeySecret"]!));
```

See [docs/encryption.md](https://github.com/rkingon/Oxbow/blob/main/docs/encryption.md).

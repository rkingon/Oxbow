# Args encryption

Args can hold an email address, an account number or a token. Encrypting them at rest keeps them out of anything that
reads the job table: a read replica, a backup, a support query, a leaked dump. The design follows Sidekiq Enterprise's
encryption: opt in per job type, decrypt only when the job is worked.

## Setup

```shell
dotnet add package Oxbow.DataProtection --prerelease
```

```csharp
[JobKind("send_statement"), EncryptArgs]
public sealed record SendStatement(string Email, string AccountNumber) : IJobArgs;

builder.Services.AddDataProtection()
    .SetApplicationName("billing")
    .PersistKeysToDbContext<KeysContext>();  // any key store every node shares

builder.Services.AddOxbow(o => o.Queues["default"] = new QueueOptions())
    .AddWorker<SendStatement, SendStatementWorker>()
    .AddArgsEncryption(o => o.UniqueKeySecret = Convert.FromBase64String(config["Oxbow:UniqueKeySecret"]!));
```

`[EncryptArgs]` encrypts one kind. `OxbowOptions.EncryptAllArgs = true` encrypts every kind. Inserting an encrypted kind
with no protector registered throws.

Every node that inserts or works encrypted kinds needs the same Data Protection key ring and application name. The
default key store is per-machine, so a job inserted on one host would fail to decrypt on another. Configure a shared one:
the database (`PersistKeysToDbContext`), a file share, Azure Blob Storage, Redis. Protect keys at rest too
(`ProtectKeysWith…`), or the keys sit next to the data they protect.

`UniqueKeySecret` is only needed for encrypted kinds that are [unique by args](#unique-jobs).

## What's stored

```json
"oxbow:enc:v1:CfDJ8…"
```

A JSON string in the `jsonb` args column, so no schema change is needed. A string rather than an object so that code
expecting args can't mistake it for them (see [upgrading a fleet](#upgrading-a-fleet)). The payload is a Data Protection payload: it carries the
ID of the key that made it, and is authenticated. The purpose is `Oxbow.Args` with the job kind as sub-purpose, so a
payload copied onto a job of another kind fails to decrypt.

Only args are encrypted. Kind, queue, tags, metadata and `errors` stay plaintext. Keep secrets out of tags and metadata,
and out of the messages of exceptions your workers throw: the message is recorded in `errors`.

## Where args are plaintext

| Where | Sees |
|---|---|
| Insert middleware (`JobInsertParams.Args`) | Plaintext args object |
| Worker (`Job<TArgs>.Args`), work middleware (`JobContext.Args`) | Plaintext args object |
| `Job<TArgs>.Row.Args`, `JobContext.Job.Args` | The envelope |
| `GetAsync`, `ListAsync`, `CancelAsync`, … (`JobRow.Args`) | The envelope |

Args are encrypted after the unique key is computed, on every insert path: `InsertAsync`, `InsertManyAsync`,
`InsertManyFastAsync` and the periodic enqueuer. To read a stored job's args, decrypt them with the protector:

```csharp
var protector = services.GetRequiredService<IJobArgsProtector>();
var job = await oxbow.GetAsync(id);
JsonElement args = protector.UnprotectArgs(job!);   // unencrypted args come back as they are
```

## Key rotation

Data Protection rotates its default key every 90 days by default. New jobs use the new key, and jobs encrypted with an
older key still decrypt, as long as that key stays in the ring. Don't delete keys while jobs that use them can still
run. A revoked or deleted key makes its jobs fail to decrypt.

## Decryption failures

A job whose args can't be decrypted (missing key, tampered payload, no protector registered on the worker) fails its
attempt with `JobArgsDecryptionException`, like args that fail to deserialize. It's retried with the retry policy and
discarded after its max attempts. The rescuer treats it the same way.

Decryption is chosen by the stored shape, not by the kind's current setting. Turning encryption off for a kind leaves
its encrypted jobs workable as long as a protector is still registered, and turning it on leaves its plaintext jobs
workable.

## Unique jobs

A unique key is normally a SHA-256 of the kind and args. For low-entropy args (an email, an invoice ID), anyone who can
read the table could confirm a guess by hashing it. So when an encrypted kind is unique `ByArgs`, the key is an
HMAC-SHA256 keyed with `UniqueKeySecret` instead. Keys without `ByArgs` stay SHA-256, as they hold nothing secret.

The secret is separate from Data Protection, which doesn't expose key material and rotates its default key. Unique keys
must be stable for deduplication to work, so a key that changed every 90 days would let a duplicate through at each
rotation. Generate at least 32 random bytes once, store them like any other secret, and use the same value on every node:

```shell
openssl rand -base64 32
```

Changing the secret, or turning encryption on for a kind that's unique by args, changes that kind's keys. A job
inserted before the change doesn't block a duplicate inserted after it. Make the change when no such job is in flight,
or accept one window in which duplicates can be inserted. For the same reason, an `ExcludeKind` key shared between an
encrypted and an unencrypted kind doesn't match across them.

## Querying args

The `args` GIN index is no use for encrypted kinds: `args @> '{"email": …}'` can't match ciphertext. Query by kind,
queue, tags or metadata instead.

## Upgrading a fleet

Older Oxbow versions don't know the envelope. They fail to deserialize it into the args type, so the attempt fails and
is retried, and it succeeds once a node that can decrypt picks it up; the worker never runs with wrong args. Upgrade
every node that works an encrypted kind, with the protector registered, before inserting encrypted jobs, or those jobs
spend attempts on old nodes and can be discarded.

## Testing

`OxbowAssert` decrypts with the protector you give it:

```csharp
var assert = new OxbowAssert { ArgsProtector = services.GetRequiredService<IJobArgsProtector>() };
var job = await assert.InsertedAsync<SendStatement>(connection, transaction, new() { Args = a => a.Email == "ada@example.com" });
```

Without one, asserting on an encrypted job throws `OxbowAssertionException`. `WorkerHarness` takes args directly, so
it's unaffected. For unit tests, `new DataProtectionJobArgsProtector(new EphemeralDataProtectionProvider())` works
without a key store.

## Another key service

`IJobArgsProtector` is the seam: `Protect(kind, json)`, `Unprotect(kind, payload)` and `HashUniqueKey(bytes)`.
Register your own singleton (on a KMS, say) instead of calling `AddArgsEncryption()`.

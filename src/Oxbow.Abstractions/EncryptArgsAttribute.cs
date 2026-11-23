namespace Oxbow;

/// <summary>
/// Encrypts this kind's args at rest. Needs an <c>IJobArgsProtector</c>, e.g. <c>AddArgsEncryption()</c> from
/// <c>Oxbow.DataProtection</c>. <c>OxbowOptions.EncryptAllArgs</c> encrypts every kind.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class EncryptArgsAttribute : Attribute;

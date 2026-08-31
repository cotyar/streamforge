namespace StreamsForge.Abstractions;

/// <summary>
/// Plan 010: marks a config property whose VALUE is a secret under the secrets-lite convention (D-H) —
/// masked as <see cref="SourceKinds.SecretMask"/> on every read path, and restored from the stored value
/// when a client writes the mask back.
///
/// <para><b>Why an attribute instead of a list in SecretsMasker.</b> Before this, every secret slot was
/// named three times in <c>SecretsMasker</c> (mask, merge, has-masked) plus twice more for the sink half —
/// so a new transport meant eight hand-written blocks in a file its author would never open, and forgetting
/// one leaked a plaintext credential through an export. Declaring the fact next to the field it describes
/// makes that failure mode structurally impossible: <c>SecretWalk</c> finds every annotated property by
/// reflection, so the count of hand-written places is zero regardless of how many transports exist.</para>
///
/// <para><b>Scope: string properties only.</b> The walker ignores this attribute anywhere else. Collection-
/// shaped secrets (<see cref="UrlPollConfig.Headers"/> values, <c>IngestConfig.Keys[].Hash/.Salt</c>) keep
/// their hand-written treatment in <c>SecretsMasker</c> — they need key/id matching rules that are specific
/// to each and are not going to multiply the way transport credentials do.</para>
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class SecretAttribute : Attribute;

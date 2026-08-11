namespace StreamForge.AppCore.Sinks;

/// <summary>
/// Plan 009 B2: an immutable snapshot of one <see cref="NatsSinkClient"/>'s lifetime publish counters.
/// "Lifetime" means since that CLIENT instance was created — a sink recreated because its config
/// changed (URL/subject/credentials edited) starts back at zero, exactly like re-pointing a connector
/// at a new endpoint would reset its own runtime-status counters. <see cref="LastError"/>/
/// <see cref="LastFailureAtMs"/> are null/0 until the first publish failure; a sink that has published
/// successfully only ever leaves them alone (a later success does not clear the memory of an earlier
/// failure — that mirrors <c>ConnectorRuntimeStatus.LastError</c>, which also isn't cleared by a
/// subsequent ok run except through <see cref="Published"/>/<see cref="Failed"/> moving forward around
/// it).
/// </summary>
public sealed record SinkPublishCounters(long Published, long Failed, string? LastError, long LastFailureAtMs);

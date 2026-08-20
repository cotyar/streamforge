namespace StreamForge.Abstractions;

/// <summary>Plan 020 wave B-2: key = <c>EnvKeys.Qualify(def.Environment, def.Name)</c> — the same
/// per-source qualified key every connector-driven grain in this repo uses (see
/// <see cref="IConnectorGrain"/>/<see cref="IGeneratorGrain"/>). Drives one <see cref="SourceKinds.Crdt"/>
/// document. Dispatched to from <c>SourceKindDispatch.ActorKind.Crdt</c> (plan 020 D3) — never from
/// <see cref="IConnectorGrain"/>'s own driver, and never looked up by <see cref="IRegistryGrain"/> mid-merge
/// (D5: config is carried on <see cref="StartAsync"/>, not fetched from inside the merge path — see
/// <c>CrdtDocGrain</c>'s own class doc for why that rule exists and what it would deadlock if broken).
///
/// <para>Shape mirrors <see cref="IConnectorGrain"/> deliberately (D4): <see cref="StartAsync"/>/
/// <see cref="StopAsync"/>/<see cref="PingAsync"/> for the same keep-alive/self-resume reasons — a CRDT
/// document has no polling timer of its own, but it still wants to survive a silo recycle and stay warm
/// under <c>GeneratorSupervisorService</c>'s ping sweep exactly like every other per-source grain
/// does.</para></summary>
public interface ICrdtDocGrain : IGrainWithStringKey
{
    Task StartAsync(SourceDefinition def);
    Task StopAsync();

    /// <summary>Keep-alive; mirrors <see cref="IConnectorGrain.PingAsync"/> — a grain call alone extends
    /// activation lifetime, a held reference does not.</summary>
    Task PingAsync();

    /// <summary>Merge Yjs v1 update bytes into the document, in order, and emit whatever rows changed.
    /// Never null — the "no such source" / "not crdt-kind" 404 distinction
    /// <see cref="ICrdtFacade.MergeAsync"/>'s own doc names lives at the FACADE, which resolves the
    /// source before ever reaching this grain; once a caller is here, the document exists and is
    /// running. Returns an empty (zero-applied, zero-emitted) result if called before
    /// <see cref="StartAsync"/> or after <see cref="StopAsync"/> — a defensive floor, not a path any
    /// correctly-wired caller should reach.</summary>
    Task<CrdtMergeResult> MergeAsync(IReadOnlyList<byte[]> updates);

    /// <summary>Never null, for the same reason as <see cref="MergeAsync"/> — see
    /// <see cref="CrdtDocStatus.Error"/> for how "not currently running" is represented instead.</summary>
    Task<CrdtDocStatus> GetStatusAsync();
}

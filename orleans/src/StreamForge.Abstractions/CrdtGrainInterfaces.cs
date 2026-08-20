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

    /// <summary>Plan 020 wave D, finding 3 — <see cref="MergeAsync"/> plus attribution. A separate method,
    /// not an added parameter on <see cref="MergeAsync"/>, so that method's signature — and every existing
    /// caller/test pinned against it — stays untouched. See <see cref="ICrdtFacade.MergeAttributedAsync"/>
    /// and <see cref="CrdtSourceConfig.AttributeChanges"/> for what <paramref name="actor"/> is used for
    /// and its documented boundary (writes who touched an entity, not who deleted one).</summary>
    Task<CrdtMergeResult> MergeAttributedAsync(IReadOnlyList<byte[]> updates, string actor);

    /// <summary>Plan 020 wave C — re-emit the document's ENTIRE current projection as create rows, without
    /// merging anything. The recovery action for the one thing a CRDT cannot recover from on its own:
    /// D7 makes replaying an edge's update history a no-op, so a consumer that lost its rows can never be
    /// refilled by re-sending updates — only by re-asserting the document's current state, which is what
    /// this does.
    ///
    /// <para><b>Why it is needed at all.</b> <c>TableGrain</c>'s RESTART-RESUME LIMITATION (its class doc)
    /// resets a resuming table's rows to empty and marks it Rebuilding — it rebuilds "purely from live
    /// traffic going forward". For a generator or a broker that is fine, because traffic keeps arriving.
    /// A document is not a stream of new events: its VALUE is its current state, and re-delivering the
    /// history that produced that state emits nothing. Without this call a twin's table is empty after
    /// every silo recycle, and a table created over an already-populated document starts empty too.</para>
    ///
    /// <para>Emits one <c>_op = c</c>, <c>_weight = +1</c> row per live entity — a re-assert, not a delta,
    /// which is why a <c>LATEST BY</c> consumer converges on it and why calling it twice is harmless.
    /// Tombstoned keys do not enumerate, so a deleted entity is not resurrected. Counts toward
    /// <see cref="CrdtDocStatus.RowsEmitted"/> like any other emission; merges nothing, so
    /// <see cref="CrdtDocStatus.UpdatesMerged"/> is untouched.</para></summary>
    Task<CrdtMergeResult> ReplayAsync();

    /// <summary>Never null, for the same reason as <see cref="MergeAsync"/> — see
    /// <see cref="CrdtDocStatus.Error"/> for how "not currently running" is represented instead.</summary>
    Task<CrdtDocStatus> GetStatusAsync();
}

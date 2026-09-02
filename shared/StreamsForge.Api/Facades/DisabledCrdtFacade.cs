using StreamsForge.Abstractions;

namespace StreamsForge.Api.Facades;

/// <summary>The "no CRDT document runtime in this build" default, shared by both flavors (moved here from
/// <c>dapr/src/StreamsForge.Dapr.Host/Facades/StubFacades.cs</c> when CRDT support became an install-time
/// Orleans plugin, <c>plugins/StreamsForge.Plugins.Crdt</c>). Lives in <c>StreamsForge.Api</c>, not
/// AppCore, because both hosts already reference this project and it is the natural home for a
/// facade-shaped default — no ASP.NET dependency is needed here, just <c>StreamsForge.Abstractions</c>
/// (StreamsForge.Contracts).
///
/// <para><b>On the Dapr flavor</b> this is still the permanent answer — partitioned execution's sibling
/// story, decision D9: the CRDT document runtime is Orleans-only. <b>On the Orleans flavor</b> it is now
/// the DEFAULT ONLY: <c>OrleansFacadesExtensions.AddOrleansFacades</c> registers this first, and if the
/// <c>crdt</c> plugin is loaded, <c>CrdtPlugin.ConfigureServices</c> registers the real
/// <c>OrleansCrdtFacade</c> afterward — "last registration wins" for singleton resolution — so a host
/// with the plugin absent behaves exactly like this class describes: <see cref="Enabled"/> false,
/// <c>CrdtEndpoints</c> answers 501 before ever calling <see cref="MergeAsync"/>/
/// <see cref="GetStatusAsync"/>, so every member below is defensive-only and should never actually
/// run.</para></summary>
public sealed class DisabledCrdtFacade : ICrdtFacade
{
    public bool Enabled => false;

    public Task<CrdtMergeResult?> MergeAsync(string sourceName, IReadOnlyList<byte[]> updates) =>
        Task.FromResult<CrdtMergeResult?>(null);

    // Plan 020 wave D, finding 3 — same "no runtime here" null as every other member; Enabled already
    // false is what makes CrdtEndpoints answer 501 before this is ever reached.
    public Task<CrdtMergeResult?> MergeAttributedAsync(string sourceName, IReadOnlyList<byte[]> updates, string actor) =>
        Task.FromResult<CrdtMergeResult?>(null);

    public Task<CrdtDocStatus?> GetStatusAsync(string sourceName) =>
        Task.FromResult<CrdtDocStatus?>(null);

    public Task<CrdtMergeResult?> ReplayAsync(string sourceName) =>
        Task.FromResult<CrdtMergeResult?>(null);

    // Undecidable, not "no touches": an empty Touches list reads as "nothing to authorize" and would
    // AUTHORIZE the update. Unreachable today (CrdtEndpoints answers 501 on Enabled == false before
    // asking), so this is purely about which way a future wiring mistake falls.
    public CrdtUpdateInspection Inspect(SourceDefinition source, byte[] update) =>
        new() { Undecidable = true, UndecidableReason = "this build has no CRDT document runtime" };

    // Plan 020 wave F — same "no runtime here" null as every other member above.
    public Task<EscrowRebalanceResult?> RebalanceAsync(string sourceName, string from, string to, long amount) =>
        Task.FromResult<EscrowRebalanceResult?>(null);
}

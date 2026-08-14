using StreamForge.Abstractions;

namespace StreamForge.AppCore.Transports;

/// <summary>One poll's worth of result set: the rows as the transport read them, the cursor to persist,
/// and whether there is more waiting right now.
///
/// <para><see cref="Cursor"/> is <b>opaque</b> — the transport mints it, the driver stores it, and nothing
/// in between parses it. An LSN, a composite <c>(ts,id)</c> string and a plain bigint therefore all fit
/// without the platform learning what any of them mean. <c>null</c> means "leave the persisted cursor
/// exactly as it was", which is what an empty poll returns; it is NOT "reset to the beginning".</para>
///
/// <para><see cref="HasMore"/> means "re-arm immediately instead of waiting for the schedule". That single
/// bit is what makes an initial snapshot resumable: a large table pages through in successive <i>driver</i>
/// cycles, each one persisting its own cursor, so a restart halfway through resumes where it stopped
/// instead of starting the snapshot over. Paging inside one <see cref="IPolledTransport.PollAsync"/> call
/// would put those intermediate cursors back in memory, which is the failure this whole seam exists to
/// avoid.</para></summary>
public sealed record PolledBatch(
    IReadOnlyList<Dictionary<string, object?>> Rows,
    string? Cursor,
    bool HasMore);

/// <summary>
/// Plan 014: everything the platform needs to know about a <b>pull</b>-shaped source kind — the sibling of
/// <see cref="IInboundTransport"/>, not a generalization of it.
///
/// <para><b>Why a sibling.</b> <see cref="IInboundTransport.Open"/> hands back an async enumerable that
/// yields until it throws. A database source driven through that shape would have to run its polling loop
/// <i>inside the subscription instance</i>, which means its cursor — the one piece of state that must
/// survive anything — lives in memory and is lost on every silo recycle, actor deactivation and rebalance.
/// That is precisely the thing that must not happen. <c>PollAsync(def, cursor, ct) -&gt; PolledBatch</c>
/// puts the loop in the driver instead, and the driver already persists its state once per cycle, so the
/// cursor rides along for free.</para>
///
/// <para><b>Three deliberate divergences from <see cref="IInboundTransport"/>:</b></para>
/// <list type="bullet">
/// <item><b>No <c>FormatOf</c>.</b> A result set is already structured. Serializing a <c>numeric</c> or a
/// <c>timestamptz</c> to JSON purely so the shared parse path can re-parse it would lose fidelity in
/// exchange for nothing — rows go straight into <c>ConnectorPollCycle.ExecuteRows</c>, which skips the
/// parse step and keeps coercion, dedup and stamping byte-identical.</item>
/// <item><b>No ledger.</b> <c>FileLedger</c>'s name → mtime map answers "which file changed", a question a
/// query has no version of. The cursor replaces it outright.</item>
/// <item><b>No mapping.</b> For a row source the SELECT list <i>is</i> the mapping; a JSONPath layer on top
/// would be a second way to say the same thing, disagreeing with the first the moment a column is renamed
/// in one and not the other. Hence <see cref="TransportDescriptor.Mapping"/> exists, so a transport can say
/// so and the console stops offering an editor for it.</item>
/// </list>
///
/// <para><b>The cursor is never advanced on a failed cycle</b> — see <see cref="PolledSourceCore"/>. That
/// belongs to the driver core rather than to each transport, because a transport bug is exactly the case
/// the rule protects against, and a rule enforced by the code that might be buggy protects nothing.</para>
/// </summary>
public interface IPolledTransport
{
    /// <summary>The <see cref="SourceDefinition.Kind"/> value this transport serves, e.g.
    /// <see cref="SourceKinds.Postgres"/>. Compared ordinally, and must be unique across the registry.</summary>
    string Kind { get; }

    /// <summary>Appends a human-readable message per problem with this source's transport config — the
    /// per-kind half of <c>SourceValidation.Validate</c>, exactly as <see cref="IInboundTransport.Validate"/>
    /// is for the message family. Never throws; an empty <paramref name="errors"/> on return means accepted.</summary>
    void Validate(SourceDefinition def, List<string> errors);

    /// <summary>Reads the next batch at or after <paramref name="cursor"/> (null = no cursor persisted yet,
    /// i.e. this source's first ever cycle). Throwing is a normal, expected outcome — a database is down far
    /// more often than a config is wrong — and <see cref="PolledSourceCore"/> turns it into a reported error
    /// with the cursor left untouched, so the batch is re-read rather than skipped.</summary>
    Task<PolledBatch> PollAsync(SourceDefinition def, string? cursor, CancellationToken ct);

    /// <summary>What the console needs to render this transport's config form — see
    /// <see cref="TransportDescriptor"/>, and set <see cref="TransportDescriptor.Polled"/> so it renders a
    /// schedule editor.</summary>
    TransportDescriptor Describe();
}

/// <summary>What a probe found: the fields it could infer, and everything it wants the operator to know
/// before accepting them. <see cref="Diagnostics"/> is not an error channel — a probe that cannot connect
/// throws. It carries the losses a successful probe still incurs, the canonical one being
/// <c>numeric</c>/<c>decimal</c> → <see cref="FieldType.Double"/>: the platform has no exact decimal type,
/// so the honest response is to say so at discovery time rather than to round silently forever after.</summary>
public sealed record SchemaProbeResult(List<FieldDef> Fields, List<string> Diagnostics);

/// <summary>
/// Plan 014: an OPTIONAL capability a transport may implement alongside <see cref="IPolledTransport"/> —
/// "I can discover my own schema". <c>POST /api/transports/{kind}/probe</c> looks for it on the registered
/// transport and 400s when it is absent, which is how schema discovery reaches the console without
/// <c>StreamForge.Api</c> learning what a database is: it knows a probe returns fields and notes, and
/// nothing further.
///
/// <para>Separate from <see cref="IPolledTransport"/> rather than an always-present method returning null,
/// so that "can this kind be discovered" is a type test the descriptor can honestly report through
/// <see cref="TransportDescriptor.CanProbe"/>, instead of a button the console renders hopefully and the
/// operator discovers is inert.</para>
/// </summary>
public interface ISchemaProbe
{
    /// <summary>Infers the field list <paramref name="def"/>'s configured table/query would produce.
    /// Throwing means "could not look" (unreachable, denied, no such table) and is surfaced verbatim;
    /// a successful probe that lost precision reports it in <see cref="SchemaProbeResult.Diagnostics"/>.</summary>
    Task<SchemaProbeResult> ProbeAsync(SourceDefinition def, CancellationToken ct);
}

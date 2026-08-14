using System.Text.Json;
using StreamForge.Abstractions;

namespace StreamForge.AppCore.Connectors.Mapping;

/// <summary>
/// Unwraps one Debezium change event into the row the platform should ingest (plan 014,
/// <see cref="MappingSpec.Envelope"/> / <see cref="CdcEnvelopes.Debezium"/>). A pure function — no
/// I/O, no state — so it composes ahead of <see cref="RecordExtractor"/> exactly where the doc comment
/// on <see cref="MappingSpec.Envelope"/> says it must: BEFORE <see cref="MappingSpec.ItemsPath"/>
/// applies, because the envelope wraps the row, it is not part of the row's own shape.
///
/// <para><b>Op → row → stamps</b>, per Debezium's own vocabulary: <c>c</c> (create) and <c>r</c>
/// (snapshot read) take the row from <c>payload.after</c>; <c>u</c> (update) likewise from
/// <c>payload.after</c>; <c>d</c> (delete) takes it from <c>payload.before</c> instead — the row no
/// longer exists in <c>after</c> to take it from. Every case stamps <c>_op</c> with the literal op
/// letter and <c>_weight</c> with <c>-1</c> for <c>d</c>, <c>+1</c> for everything else (an op this
/// unwrapper has never seen collapses to the "everything else" branch — one honest guess is better
/// than a thrown exception at ingest time over a Debezium version we don't recognize yet).
/// <c>payload.ts_ms</c>, when present, becomes <c>_ts</c> — Debezium's own commit-time clock, more
/// trustworthy than connector arrival time.</para>
///
/// <para><b>All three shapes a Debezium deployment can actually emit</b> are accepted, because which
/// one a given install produces is a connector-config decision StreamForge does not control: the raw
/// <c>{"schema": …, "payload": {"op": …}}</c> form; the same thing with the <c>schema</c> wrapper
/// stripped (some Debezium Server sink configs do this); and a message with NO <c>op</c> key at all —
/// the <c>ExtractNewRecordState</c> single-message-transform has already flattened it down to a plain
/// row before it reached us. That third shape is passed through UNTOUCHED: an operator who configured
/// the SMT and set <c>envelope: debezium</c> anyway (belt and suspenders, or just not sure which one
/// they need) must still get their rows, not an empty batch because we went looking for a wrapper that
/// was already stripped.</para>
///
/// <para><b>What can't produce a row, and is made visible rather than silently dropped</b>: a <c>d</c>
/// event whose <c>before</c> is absent — Debezium emits exactly this when the source table has no
/// <c>REPLICA IDENTITY FULL</c>, so there is genuinely no "old row" to hand over — and a tombstone
/// message (Debezium's null/empty payload emitted as the second half of a delete, so consumers doing
/// Kafka-style log compaction can drop the key). Both come back as <see cref="Unwrapped.Skip"/> = true
/// with a <see cref="Unwrapped.SkipReason"/> naming which case it was, rather than a fabricated empty
/// row or a swallowed message; <see cref="ConnectorPollCycle"/> counts these instead of failing the
/// whole poll cycle over one unrepresentable event among many good ones.</para>
///
/// <para><b>The honest limit</b> (stated here, not left for a reader to infer): a StreamForge source is
/// an append-only <c>EventRecord</c> stream — <c>_weight</c> on an INBOUND row is just a column, a
/// value like any other. The Engine's Z-set weights that make a table a genuine multiset are computed
/// FROM table SQL, not carried in from ingress. So a Debezium delete does not retract the row it
/// deleted; it arrives as one more event stamped <c>_op = "d"</c>, <c>_weight = -1</c>, sitting in the
/// stream next to every insert and update that came before it. The working pattern this enables is
/// <c>LATEST BY &lt;key&gt;</c> + <c>WHERE _op &lt;&gt; 'd'</c> on the downstream table — which HIDES a
/// deleted key from query results but does not FREE it: the tombstone event, and everything before it,
/// is still sitting in the source's history. This is not mirror-perfect replicated state; it is a
/// change log an operator can query around. Threading a real ingress retraction into the Engine's
/// Z-sets is out of scope here — see plan 014's "cut, ranked" list.</para>
/// </summary>
public static class CdcEnvelope
{
    /// <summary>One unwrap outcome. <see cref="Row"/> is what <see cref="RecordExtractor"/> should
    /// extract from — the original message, untouched, for every case that isn't a recognized
    /// Debezium change event (<see cref="MappingSpec.Envelope"/> is <see cref="CdcEnvelopes.None"/>,
    /// or the message has no <c>op</c> key). <see cref="Op"/>/<see cref="Weight"/>/<see cref="TsMs"/>
    /// are null exactly when there is nothing to stamp — the pre-014 path never sees anything but
    /// null here, which is what makes <see cref="CdcEnvelopes.None"/> byte-identical. <see cref="Skip"/>
    /// true means the event carries no extractable row at all (see the class doc's "what can't
    /// produce a row" section); <see cref="Row"/> is still the original message in that case, but the
    /// caller must not extract from it as a data row — it exists only so <see cref="SkipReason"/> has
    /// something to reference.</summary>
    public readonly record struct Unwrapped(
        JsonElement Row,
        string? Op,
        int? Weight,
        long? TsMs,
        bool Skip,
        string? SkipReason);

    /// <summary>Unwraps <paramref name="message"/> per <paramref name="envelope"/>
    /// (<see cref="MappingSpec.Envelope"/>). Anything other than <see cref="CdcEnvelopes.Debezium"/> —
    /// today only <see cref="CdcEnvelopes.None"/>, additively-typed as a string so a future envelope
    /// kind doesn't need a new call site here — is a straight pass-through: the message, unexamined,
    /// with nothing stamped. This is the entire proof that <see cref="CdcEnvelopes.None"/> changes
    /// nothing about the pre-014 pipeline: this method never even looks at the JSON when it's called.</summary>
    public static Unwrapped Unwrap(JsonElement message, string envelope)
        => envelope == CdcEnvelopes.Debezium
            ? UnwrapDebezium(message)
            : new Unwrapped(message, null, null, null, false, null);

    private static Unwrapped UnwrapDebezium(JsonElement message)
    {
        if (IsTombstone(message))
        {
            return new Unwrapped(message, null, null, null, true, "tombstone (null/empty Debezium payload)");
        }

        // Shape 1: {"schema": …, "payload": {…}}. Shape 2: the payload fields at the top level
        // already (some Debezium Server sink configs strip the schema wrapper). Try "payload" first;
        // if it's not there, treat the message itself as the payload.
        var payload = message;
        if (message.ValueKind == JsonValueKind.Object && message.TryGetProperty("payload", out var inner))
        {
            payload = inner;
            if (IsTombstone(payload))
            {
                return new Unwrapped(message, null, null, null, true, "tombstone (null/empty Debezium payload)");
            }
        }

        // Shape 3: no "op" key — the ExtractNewRecordState SMT already flattened this to a plain row.
        // Pass the ORIGINAL message through untouched; nothing here was a change-event wrapper at all.
        if (payload.ValueKind != JsonValueKind.Object
            || !payload.TryGetProperty("op", out var opEl)
            || opEl.ValueKind != JsonValueKind.String)
        {
            return new Unwrapped(message, null, null, null, false, null);
        }

        var op = opEl.GetString() ?? "";
        var weight = op == "d" ? -1 : 1;
        long? tsMs = payload.TryGetProperty("ts_ms", out var tsEl) && tsEl.ValueKind == JsonValueKind.Number
            ? tsEl.GetInt64()
            : null;

        var sourceField = op == "d" ? "before" : "after";
        if (!payload.TryGetProperty(sourceField, out var row) || row.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            var reason = op == "d"
                ? "delete event has no 'before' — the source table has no REPLICA IDENTITY FULL, so Debezium sent no old row to retract"
                : $"op '{op}' event has no '{sourceField}'";
            return new Unwrapped(message, op, weight, tsMs, true, reason);
        }

        return new Unwrapped(row, op, weight, tsMs, false, null);
    }

    /// <summary>A JSON <c>null</c> (the whole message, or its <c>payload</c> property), or a payload
    /// object with no properties at all — both are how a Debezium tombstone shows up depending on how
    /// far downstream of Kafka the JSON got re-encoded.</summary>
    private static bool IsTombstone(JsonElement value)
        => value.ValueKind == JsonValueKind.Null
            || (value.ValueKind == JsonValueKind.Object && !value.EnumerateObject().MoveNext());
}

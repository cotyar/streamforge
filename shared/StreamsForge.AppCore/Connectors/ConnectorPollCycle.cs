using System.Text.Json;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors.Formats;
using StreamsForge.AppCore.Connectors.Mapping;
using StreamsForge.AppCore.Connectors.Polling;

namespace StreamsForge.AppCore.Connectors;

/// <summary>Result of one poll cycle: rows ready to emit (already `_ts`/`_source`-stamped, deduped,
/// and — plan 009 C2 — coerced to each field's declared type) plus the error channel. State mutations
/// happen on the trackers the caller passed in — persist them after a successful emit.
/// <see cref="CoercionFailures"/> counts field-level coercion failures encountered while producing
/// <see cref="Rows"/> (0 for pre-009 callers/records — additive, default parameter) — see
/// <see cref="ConnectorRowCoercion"/> for what happens to a failing field/row per
/// <see cref="SourceDefinition.OnCoercionFailure"/>; a <see cref="CoercionFailurePolicy.RejectBatch"/>
/// rejection surfaces as a non-null <see cref="Error"/> instead (same "coerce before admission, nothing
/// left behind" shape as every other Error case here). <see cref="EnvelopeSkipped"/> (plan 014, 0 for
/// every pre-014 caller/record — additive, default parameter) counts messages
/// <see cref="Mapping.CdcEnvelope"/> could not turn into a row (a delete with no `before`, a tombstone) —
/// same shape as <see cref="CoercionFailures"/>: counted and visible, NOT folded into <see cref="Error"/>,
/// because one unrepresentable CDC event must not drop every other row this cycle DID produce.
/// <see cref="Note"/> (additive, default parameter — every pre-existing caller/record reads null) is the
/// "clean cycle, something to say" channel: a cycle that SUCCEEDED and whose rows must be emitted, but
/// which nevertheless has something an operator needs to read. It is deliberately NOT an
/// <see cref="Error"/> — the driver's emit policy drops every row of a failed cycle, so folding a
/// partial-success note into Error is exactly how good rows get lost (see
/// <see cref="ConnectorPollCycle.ExecuteFolder"/>, the reason this field exists). Drivers surface it on
/// the same status line coercion failures and envelope skips already use.</summary>
public sealed record PollCycleResult(List<Dictionary<string, object?>> Rows, string? Error, int CoercionFailures = 0, int EnvelopeSkipped = 0, string? Note = null);

/// <summary>One poll execution for url/file/folder connector kinds, composing the W2 cores
/// (FormatParsers → RecordExtractor → DedupTracker/FileLedger) identically on both runtimes, plus
/// (plan 009 C2) declared-type coercion via <see cref="ConnectorRowCoercion"/>. The drivers own
/// scheduling, persistence, and emission; this owns the cycle semantics. HTTP is injected; file/folder
/// I/O is BCL. gRPC kind is NOT handled here — it decodes already-typed rows off the wire and applies
/// its own coercion pass (ConnectorGrain/ConnectorActor). NATS (plan 009 B1) IS handled here —
/// <see cref="ExecuteNatsMessage"/> — because it shares the exact same format/mapping path a polled
/// body uses; only its transport (a persistent subscription) is different. Plan 014 adds
/// <see cref="ExecuteRows"/> for sources whose rows arrive already structured (a database result set):
/// same coercion, dedup and stamping, minus the parse step there is nothing to apply.
///
/// <para>Plan 014 also wires <see cref="Mapping.CdcEnvelope"/> into <see cref="ParseAndExtract"/> — the
/// one place every format/message-based kind funnels through (url-with-a-declared-format, file, folder,
/// NATS, and any future message transport) — so a Debezium envelope unwraps identically no matter which
/// transport carried it in. It runs on each parsed item BEFORE <see cref="RecordExtractor.Extract"/>
/// applies <see cref="MappingSpec.ItemsPath"/>/Fields, per that field's own doc comment. It does NOT run
/// in <see cref="ExecuteRows"/> — a database result row is not wrapped in anything to unwrap.</para></summary>
public static class ConnectorPollCycle
{
    /// <summary>Effective mapping: the configured one, else a pass-through built from the source's
    /// declared fields (ItemsPath "$", SourcePath = field name) — the OpenAPI-derived-schema path
    /// where no hand-written mapping document exists.</summary>
    public static MappingSpec EffectiveMapping(SourceDefinition def)
        => def.Connector?.Mapping ?? new MappingSpec
        {
            ItemsPath = "$",
            Fields = def.Fields.Select(f => new FieldMapEntry { Field = f }).ToList(),
        };

    /// <summary>URL kind: body → JSON → extract → dedup → stamp.
    ///
    /// <para>Plan 012: a url source may declare a <see cref="UrlPollConfig.Format"/> other than JSON, in
    /// which case the body goes through the SAME <see cref="ParseAndExtract"/> path a file/folder/message
    /// payload uses — an endpoint that serves text/csv no longer needs a file in between. The JSON branch
    /// below is left exactly as it was rather than folded into that path: it hands the parsed ROOT to the
    /// extractor, so an ItemsPath rooted at a top-level array (<c>$.data[*]</c> against an array body)
    /// resolves against the whole document, where ParseAndExtract would have already split it into items
    /// and re-rooted each one. Identical for every mapping that works today, different for some that
    /// don't — not a distinction worth risking on a default-valued field.</para></summary>
    public static PollCycleResult ExecuteUrl(SourceDefinition def, string responseBody, DedupTracker dedup, long nowMs)
    {
        var format = def.Connector?.Url?.Format;
        if (!string.IsNullOrEmpty(format) && format != FileFormats.JsonArray)
        {
            return ParseAndExtract(def, format, responseBody, dedup, nowMs);
        }

        try
        {
            using var doc = JsonDocument.Parse(responseBody);
            return Emit(def, ExtractRows(def, doc.RootElement, nowMs), dedup, nowMs);
        }
        catch (JsonException e)
        {
            return new PollCycleResult([], $"response is not valid JSON: {e.Message}");
        }
    }

    /// <summary>File kind: re-parse the whole file when the ledger says it changed (mtime); the
    /// dedup key (when configured) is what keeps re-parses from re-emitting seen items — without
    /// one, every content change re-emits the full file (documented at-least-once semantics).</summary>
    public static PollCycleResult ExecuteFile(SourceDefinition def, FileLedger ledger, DedupTracker dedup, long nowMs)
    {
        var cfg = def.Connector?.File ?? throw new InvalidOperationException($"source '{def.Name}' has kind 'file' but no file config");
        if (!File.Exists(cfg.Path)) return new PollCycleResult([], $"file not found: {cfg.Path}");
        var mtimeMs = new DateTimeOffset(File.GetLastWriteTimeUtc(cfg.Path)).ToUnixTimeMilliseconds();
        if (!ledger.IsNewOrChanged(cfg.Path, mtimeMs)) return new PollCycleResult([], null);
        var result = ParseAndExtract(def, cfg.Format, File.ReadAllText(cfg.Path), dedup, nowMs);
        if (result.Error is null) ledger.Record(cfg.Path, mtimeMs);
        return result;
    }

    /// <summary>Folder kind: each NEW/changed file (name+mtime ledger, optional glob on names,
    /// no recursion) is parsed once and remembered.
    ///
    /// <para>PER-FILE ISOLATION: one unparseable file no longer costs this cycle the rows every OTHER file
    /// produced. A failed file is skipped BEFORE <c>ledger.Record</c> (so it is re-read, and re-attempted,
    /// on the next cycle — a half-written file that gets its second chunk simply lands then), and the good
    /// files' rows come back with <see cref="PollCycleResult.Error"/> NULL plus a
    /// <see cref="PollCycleResult.Note"/> naming what failed. This is not cosmetic: the driver's emit
    /// policy is "a failed cycle emits nothing", so returning an aggregate Error here — which is what this
    /// method used to do — meant the good files were ledgered as read AND their rows dropped, i.e.
    /// permanently lost. "folder not found" stays an Error: nothing was read, nothing is being hidden, and
    /// the failure streak/backoff it drives is the right response to a path that is not there.</para></summary>
    public static PollCycleResult ExecuteFolder(SourceDefinition def, FileLedger ledger, DedupTracker dedup, long nowMs)
    {
        var cfg = def.Connector?.Folder ?? throw new InvalidOperationException($"source '{def.Name}' has kind 'folder' but no folder config");
        if (!Directory.Exists(cfg.Path)) return new PollCycleResult([], $"folder not found: {cfg.Path}");
        var rows = new List<Dictionary<string, object?>>();
        var errors = new List<string>();
        foreach (var file in Directory.EnumerateFiles(cfg.Path, cfg.Glob ?? "*", SearchOption.TopDirectoryOnly).OrderBy(f => f, StringComparer.Ordinal))
        {
            var mtimeMs = new DateTimeOffset(File.GetLastWriteTimeUtc(file)).ToUnixTimeMilliseconds();
            if (!ledger.IsNewOrChanged(file, mtimeMs)) continue;
            var one = ParseAndExtract(def, cfg.Format, File.ReadAllText(file), dedup, nowMs);
            if (one.Error is not null) { errors.Add($"{Path.GetFileName(file)}: {one.Error}"); continue; }
            rows.AddRange(one.Rows);
            ledger.Record(file, mtimeMs);
        }
        return new PollCycleResult(
            rows,
            null,
            Note: errors.Count == 0
                ? null
                : $"{errors.Count} file(s) failed to parse and will be retried next cycle: {string.Join("; ", errors)}");
    }

    /// <summary>NATS kind (plan 009 B1): one message payload → parse (<see cref="NatsSubConfig.Format"/>)
    /// → extract (Mapping) → coerce/dedup/stamp — the EXACT SAME pipeline url/file/folder connectors use
    /// for a fetched body (the class doc's "do not invent a second extraction path" rule), so a NATS
    /// message becomes a row exactly the way a polled HTTP body does. <paramref name="dedup"/> is the
    /// subscription-lifetime tracker the caller persists — <c>MappingSpec.DedupKeyField</c> applies here
    /// exactly as it does for a poll cycle, which matters most for JetStream redelivery.</summary>
    public static PollCycleResult ExecuteNatsMessage(SourceDefinition def, string format, string payloadText, DedupTracker dedup, long nowMs)
        => ParseAndExtract(def, format, payloadText, dedup, nowMs);

    /// <summary>Plan 010: the transport-neutral name for the call above — this path never depended on NATS
    /// for anything, only on "a payload, in a known format", which is exactly what every message transport
    /// hands over. <see cref="SubscriberCore"/> calls this one; <see cref="ExecuteNatsMessage"/> remains as
    /// the original name it shipped under.</summary>
    public static PollCycleResult ExecuteMessage(SourceDefinition def, string format, string payloadText, DedupTracker dedup, long nowMs)
        => ParseAndExtract(def, format, payloadText, dedup, nowMs);

    /// <summary>Plan 014: rows that arrive ALREADY STRUCTURED — a database result set, driven through
    /// <c>IPolledTransport</c>/<c>PolledSourceCore</c>. It shares the exact <see cref="Emit"/> the url and
    /// message paths use, so coercion, dedup and the "_source"/"_ts" stamps are byte-identical; the only
    /// step it skips is the parse, because there is nothing to parse. Serializing a <c>numeric</c> or a
    /// <c>timestamptz</c> to JSON purely to re-enter <see cref="ParseAndExtract"/> would lose fidelity for
    /// nothing, which is why this entry point exists at all rather than the transport handing over bytes.
    ///
    /// <para>It also skips <c>MappingSpec</c> extraction: for a row source the SELECT list IS the mapping,
    /// and a JSONPath layer on top would be a second way to say the same thing — one that starts disagreeing
    /// with the first the moment a column is renamed in only one of them. Consequently the dedup key comes
    /// from <paramref name="dedupKeyField"/> (the transport's own config, via the driver) and NOT from
    /// <c>MappingSpec.DedupKeyField</c>, which such a source never populates. Null disables dedup.</para></summary>
    public static PollCycleResult ExecuteRows(
        SourceDefinition def,
        IReadOnlyList<Dictionary<string, object?>> rows,
        string? dedupKeyField,
        DedupTracker dedup,
        long nowMs)
        => EmitCore(def, [.. rows], dedup, nowMs, dedupKeyField);

    private static PollCycleResult ParseAndExtract(SourceDefinition def, string format, string text, DedupTracker dedup, long nowMs)
    {
        List<JsonElement> items;
        try
        {
            items = format switch
            {
                FileFormats.Ndjson => FormatParsers.ParseNdjson(text),
                FileFormats.JsonArray => FormatParsers.ParseJsonArray(text),
                FileFormats.Csv => FormatParsers.ParseCsv(text),
                FileFormats.Fix => FixParser.Parse(text),
                _ => throw new FormatException($"unknown format '{format}'"),
            };
        }
        catch (FormatException e)
        {
            return new PollCycleResult([], e.Message);
        }
        var mapping = EffectiveMapping(def);
        var rows = new List<Dictionary<string, object?>>();
        var envelopeSkipped = 0;
        foreach (var item in items)
        {
            // Plan 014: unwrap BEFORE ItemsPath/Fields apply — see the class doc and
            // Mapping.CdcEnvelope's own doc for why. `mapping.Envelope` defaults to
            // CdcEnvelopes.None, for which Unwrap() hands `item` straight back with nothing stamped,
            // which is what keeps every pre-014 mapping byte-identical.
            var unwrapped = CdcEnvelope.Unwrap(item, mapping.Envelope);
            if (unwrapped.Skip)
            {
                envelopeSkipped++;
                continue;
            }

            foreach (var row in RecordExtractor.Extract(unwrapped.Row, mapping, nowMs))
            {
                // These three land directly on the row, bypassing Fields, the same way "_source" and
                // "_ts" already do in EmitCore/ResolveTimestamp below — a CDC stamp is metadata about
                // the event, not a column the operator's mapping document would ever declare.
                if (unwrapped.Op is not null) row["_op"] = unwrapped.Op;
                if (unwrapped.Weight is not null) row["_weight"] = unwrapped.Weight.Value;
                if (unwrapped.TsMs is not null) row["_ts"] = unwrapped.TsMs.Value;
                rows.Add(row);
            }
        }

        var result = Emit(def, rows, dedup, nowMs);
        return envelopeSkipped == 0 ? result : result with { EnvelopeSkipped = envelopeSkipped };
    }

    private static List<Dictionary<string, object?>> ExtractRows(SourceDefinition def, JsonElement root, long nowMs)
        => RecordExtractor.Extract(root, EffectiveMapping(def), nowMs);

    private static PollCycleResult Emit(SourceDefinition def, List<Dictionary<string, object?>> rows, DedupTracker dedup, long nowMs)
        => EmitCore(def, rows, dedup, nowMs, def.Connector?.Mapping?.DedupKeyField);

    /// <summary>Plan 014: <see cref="Emit"/> with the dedup field passed in rather than read off the
    /// mapping document, so <see cref="ExecuteRows"/> — whose sources have no mapping document — reaches the
    /// same coerce/dedup/stamp code instead of growing its own near-copy of it. Every pre-014 path goes
    /// through <see cref="Emit"/> and is unchanged.</summary>
    private static PollCycleResult EmitCore(SourceDefinition def, List<Dictionary<string, object?>> rows, DedupTracker dedup, long nowMs, string? dedupField)
    {
        // Plan 009 C2: coerce every declared field to its type BEFORE dedup/admission — a RejectBatch
        // rejection must leave nothing behind, same rule A1.1 states for push ingress.
        var coercion = ConnectorRowCoercion.Apply(def.Fields, rows, def.OnCoercionFailure);
        if (coercion.BatchRejected)
        {
            return new PollCycleResult([], $"coercion rejected batch: {coercion.RejectReason}", coercion.FailureCount);
        }

        var emitted = new List<Dictionary<string, object?>>(coercion.Rows.Count);
        foreach (var row in coercion.Rows)
        {
            if (dedupField is not null
                && row.TryGetValue(dedupField, out var key) && key is not null
                && dedup.Seen(FormattableString.Invariant($"{key}")))
            {
                continue;
            }
            row["_source"] = def.Name;
            if (!row.ContainsKey("_ts")) row["_ts"] = nowMs;
            emitted.Add(row);
        }
        return new PollCycleResult(emitted, null, coercion.FailureCount);
    }
}

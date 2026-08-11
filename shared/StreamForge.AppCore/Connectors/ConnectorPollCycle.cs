using System.Text.Json;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Formats;
using StreamForge.AppCore.Connectors.Mapping;
using StreamForge.AppCore.Connectors.Polling;

namespace StreamForge.AppCore.Connectors;

/// <summary>Result of one poll cycle: rows ready to emit (already `_ts`/`_source`-stamped, deduped,
/// and — plan 009 C2 — coerced to each field's declared type) plus the error channel. State mutations
/// happen on the trackers the caller passed in — persist them after a successful emit.
/// <see cref="CoercionFailures"/> counts field-level coercion failures encountered while producing
/// <see cref="Rows"/> (0 for pre-009 callers/records — additive, default parameter) — see
/// <see cref="ConnectorRowCoercion"/> for what happens to a failing field/row per
/// <see cref="SourceDefinition.OnCoercionFailure"/>; a <see cref="CoercionFailurePolicy.RejectBatch"/>
/// rejection surfaces as a non-null <see cref="Error"/> instead (same "coerce before admission, nothing
/// left behind" shape as every other Error case here).</summary>
public sealed record PollCycleResult(List<Dictionary<string, object?>> Rows, string? Error, int CoercionFailures = 0);

/// <summary>One poll execution for url/file/folder connector kinds, composing the W2 cores
/// (FormatParsers → RecordExtractor → DedupTracker/FileLedger) identically on both runtimes, plus
/// (plan 009 C2) declared-type coercion via <see cref="ConnectorRowCoercion"/>. The drivers own
/// scheduling, persistence, and emission; this owns the cycle semantics. HTTP is injected; file/folder
/// I/O is BCL. gRPC kind is NOT handled here — it decodes already-typed rows off the wire and applies
/// its own coercion pass (ConnectorGrain/ConnectorActor). NATS (plan 009 B1) IS handled here —
/// <see cref="ExecuteNatsMessage"/> — because it shares the exact same format/mapping path a polled
/// body uses; only its transport (a persistent subscription) is different.</summary>
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

    /// <summary>URL kind: body → JSON → extract → dedup → stamp.</summary>
    public static PollCycleResult ExecuteUrl(SourceDefinition def, string responseBody, DedupTracker dedup, long nowMs)
    {
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
    /// no recursion) is parsed once and remembered.</summary>
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
        return new PollCycleResult(rows, errors.Count == 0 ? null : string.Join("; ", errors));
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
                _ => throw new FormatException($"unknown format '{format}'"),
            };
        }
        catch (FormatException e)
        {
            return new PollCycleResult([], e.Message);
        }
        var mapping = EffectiveMapping(def);
        var rows = new List<Dictionary<string, object?>>();
        foreach (var item in items) rows.AddRange(RecordExtractor.Extract(item, mapping, nowMs));
        return Emit(def, rows, dedup, nowMs);
    }

    private static List<Dictionary<string, object?>> ExtractRows(SourceDefinition def, JsonElement root, long nowMs)
        => RecordExtractor.Extract(root, EffectiveMapping(def), nowMs);

    private static PollCycleResult Emit(SourceDefinition def, List<Dictionary<string, object?>> rows, DedupTracker dedup, long nowMs)
    {
        // Plan 009 C2: coerce every declared field to its type BEFORE dedup/admission — a RejectBatch
        // rejection must leave nothing behind, same rule A1.1 states for push ingress.
        var coercion = ConnectorRowCoercion.Apply(def.Fields, rows, def.OnCoercionFailure);
        if (coercion.BatchRejected)
        {
            return new PollCycleResult([], $"coercion rejected batch: {coercion.RejectReason}", coercion.FailureCount);
        }

        var dedupField = def.Connector?.Mapping?.DedupKeyField;
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

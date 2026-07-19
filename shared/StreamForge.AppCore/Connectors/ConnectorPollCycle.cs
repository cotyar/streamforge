using System.Text.Json;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Formats;
using StreamForge.AppCore.Connectors.Mapping;
using StreamForge.AppCore.Connectors.Polling;

namespace StreamForge.AppCore.Connectors;

/// <summary>Result of one poll cycle: rows ready to emit (already `_ts`/`_source`-stamped, deduped)
/// plus the error channel. State mutations happen on the trackers the caller passed in — persist
/// them after a successful emit.</summary>
public sealed record PollCycleResult(List<Dictionary<string, object?>> Rows, string? Error);

/// <summary>One poll execution for url/file/folder connector kinds, composing the W2 cores
/// (FormatParsers → RecordExtractor → DedupTracker/FileLedger) identically on both runtimes.
/// The drivers own scheduling, persistence, and emission; this owns the cycle semantics.
/// HTTP is injected; file/folder I/O is BCL. gRPC kind is NOT handled here — it is a persistent
/// subscription (GrpcSubscriberCore), not a poll.</summary>
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
        var dedupField = def.Connector?.Mapping?.DedupKeyField;
        var emitted = new List<Dictionary<string, object?>>(rows.Count);
        foreach (var row in rows)
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
        return new PollCycleResult(emitted, null);
    }
}

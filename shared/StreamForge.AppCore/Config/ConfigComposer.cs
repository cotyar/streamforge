using System.Text.Json.Nodes;

namespace StreamForge.AppCore.Config;

/// <summary>
/// Plan 006 (D-I): ordered multi-document composition — later documents win per entity (matched by
/// kind+name) with SHALLOW field override at the JSON level: a property present in the later
/// document's entity replaces the earlier value ENTIRELY (arrays/objects replace whole, never
/// element-merged); an explicit JSON <c>null</c> clears an optional field (the property is set to
/// null in the merged node — <see cref="ConfigJsonMapper.NodeToDocument"/> then normalizes any
/// resulting null collection back to empty, since a merge-cleared collection means "no items", not
/// "absent"). Merging happens on <see cref="JsonNode"/>s BEFORE deserialization so "absent vs
/// present-null" is honored — deserializing first and merging POCOs would lose that distinction
/// (a POCO can't tell "field was never set" apart from "field was explicitly cleared").
/// </summary>
public static class ConfigComposer
{
    /// <summary>Composes documents in order — later wins. Each document's own parse diagnostics are
    /// prefixed with its index (<c>"doc[i]: ..."</c>); a document whose root isn't even a JSON
    /// object (or that fails to parse) contributes no entities but doesn't abort the others. Returns
    /// (null, diagnostics) only when there's nothing left to compose (every document failed to
    /// parse and at least one was given) — an empty input list composes to an empty document.</summary>
    public static (ConfigDocument? Doc, IReadOnlyList<string> Diagnostics) Compose(IReadOnlyList<string> orderedDocTexts)
    {
        var diagnostics = new List<string>();
        var docs = new List<(string Label, JsonNode? Node)>();

        for (var i = 0; i < orderedDocTexts.Count; i++)
        {
            var (node, nodeDiagnostics) = ConfigJsonMapper.TextToNode(orderedDocTexts[i]);
            foreach (var d in nodeDiagnostics)
            {
                diagnostics.Add($"doc[{i}]: {d}");
            }

            docs.Add(($"doc[{i}]", node));
        }

        var (doc, mergeDiagnostics) = MergeDocs(docs);
        diagnostics.AddRange(mergeDiagnostics);
        return (doc, diagnostics);
    }

    /// <summary>Resolves <paramref name="rootPath"/>'s <c>include</c> list depth-first, in order,
    /// via <paramref name="resolver"/> (relative path -&gt; text; null means missing -&gt; a fatal
    /// diagnostic naming the path). Each document's OWN includes are composed BEFORE that document
    /// itself (so a document always wins over everything it includes — "the includer overrides its
    /// includes"); nested includes are resolved the same way, recursively. A cycle (a path already
    /// on the current resolution stack) is fatal — diagnostic names the full cycle chain, returns
    /// (null, diagnostics). A missing include is likewise fatal (there is no well-defined "partial"
    /// composition when a document's base is absent).</summary>
    public static (ConfigDocument? Doc, IReadOnlyList<string> Diagnostics) ComposeWithIncludes(
        string rootPath, Func<string, string?> resolver)
    {
        var diagnostics = new List<string>();
        var docs = new List<(string Label, JsonNode? Node)>();
        var stack = new List<string>();

        if (!CollectIncludes(rootPath, resolver, stack, docs, diagnostics))
        {
            return (null, diagnostics);
        }

        var (doc, mergeDiagnostics) = MergeDocs(docs);
        diagnostics.AddRange(mergeDiagnostics);
        return (doc, diagnostics);
    }

    private static bool CollectIncludes(
        string path, Func<string, string?> resolver, List<string> stack,
        List<(string Label, JsonNode? Node)> docs, List<string> diagnostics)
    {
        if (stack.Contains(path, StringComparer.Ordinal))
        {
            diagnostics.Add($"include cycle detected: {string.Join(" -> ", stack)} -> {path}");
            return false;
        }

        var text = resolver(path);
        if (text is null)
        {
            diagnostics.Add($"missing include: {path}");
            return false;
        }

        stack.Add(path);

        var (node, nodeDiagnostics) = ConfigJsonMapper.TextToNode(text);
        foreach (var d in nodeDiagnostics)
        {
            diagnostics.Add($"{path}: {d}");
        }

        if (node is JsonObject obj &&
            obj.TryGetPropertyValue("include", out var includeNode) &&
            includeNode is JsonArray includeArr)
        {
            foreach (var item in includeArr)
            {
                if (item is JsonValue iv && iv.TryGetValue<string>(out var childPath) && !string.IsNullOrWhiteSpace(childPath))
                {
                    if (!CollectIncludes(childPath, resolver, stack, docs, diagnostics))
                    {
                        stack.RemoveAt(stack.Count - 1);
                        return false;
                    }
                }
            }
        }

        stack.RemoveAt(stack.Count - 1);
        docs.Add((path, node));
        return true;
    }

    /// <summary>The shared merge core for both <see cref="Compose"/> and
    /// <see cref="ComposeWithIncludes"/>: walks the already-parsed (label, node) pairs in order,
    /// folding each document's sources/pipelines/tables into name-keyed maps (later document's
    /// entity of the same kind+name replaces the earlier one via <see cref="ShallowMergeEntity"/>),
    /// then deserializes the merged result through the normal <see cref="ConfigJsonMapper.NodeToDocument"/>
    /// path (so entity-level diagnostics — missing name, etc. — apply uniformly).</summary>
    private static (ConfigDocument? Doc, List<string> Diagnostics) MergeDocs(List<(string Label, JsonNode? Node)> docs)
    {
        var diagnostics = new List<string>();
        var sources = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
        var pipelines = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
        var tables = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
        var version = 1;
        string? schemaPolicy = null;
        var anySucceeded = false;

        foreach (var (label, node) in docs)
        {
            if (node is null)
            {
                // Already diagnosed by the caller (parse failure / missing include).
                continue;
            }

            if (node is not JsonObject obj)
            {
                diagnostics.Add($"{label}: root must be a JSON object");
                continue;
            }

            if (obj.TryGetPropertyValue("version", out var versionNode) && versionNode is not null)
            {
                if (versionNode is JsonValue vv && ConfigJsonMapper.TryGetInt32(vv, out var v))
                {
                    if (v != 1)
                    {
                        diagnostics.Add($"{label}: unsupported version: {v} (expected 1)");
                        continue;
                    }

                    version = v;
                }
                else
                {
                    diagnostics.Add($"{label}: 'version' must be an integer");
                    continue;
                }
            }

            MergeEntitiesInto(sources, obj, "sources", label, diagnostics);
            MergeEntitiesInto(pipelines, obj, "pipelines", label, diagnostics);
            MergeEntitiesInto(tables, obj, "tables", label, diagnostics);

            // Plan 016 wave 3 (orchestrator fix): schemaPolicy is a document-level SCALAR, and this
            // method rebuilds the merged root from scratch — so before this line every non-entity
            // top-level property was dropped, and `schemaPolicy: "any"` could never reach the import
            // gate that reads it. Found live by wave 3-C, which owns the gate but not this file.
            //
            // Assigned unconditionally (absent overwrites present), which makes it the ROOT document's
            // property rather than a merged one. Includes are collected depth-first BEFORE the document
            // that includes them (see CollectIncludes), so the root is last and therefore always wins —
            // consistent with this file's stated "later wins" rule, and the safe direction besides: an
            // included fragment must not be able to switch off a schema gate the document being imported
            // never asked to switch off. Only a document someone deliberately imports can relax its own
            // promotion gate.
            schemaPolicy = obj.TryGetPropertyValue("schemaPolicy", out var policyNode) &&
                policyNode is JsonValue policyValue && policyValue.TryGetValue<string>(out var policyText)
                    ? policyText
                    : null;
            anySucceeded = true;
        }

        if (!anySucceeded && docs.Count > 0)
        {
            diagnostics.Insert(0, "no document composed successfully");
            return (null, diagnostics);
        }

        var root = new JsonObject { ["version"] = version };
        if (!string.IsNullOrWhiteSpace(schemaPolicy))
        {
            root["schemaPolicy"] = schemaPolicy;
        }

        if (sources.Count > 0)
        {
            root["sources"] = new JsonArray([.. sources.Values.Select(v => (JsonNode?)v.DeepClone())]);
        }

        if (pipelines.Count > 0)
        {
            root["pipelines"] = new JsonArray([.. pipelines.Values.Select(v => (JsonNode?)v.DeepClone())]);
        }

        if (tables.Count > 0)
        {
            root["tables"] = new JsonArray([.. tables.Values.Select(v => (JsonNode?)v.DeepClone())]);
        }

        var (composedDoc, docDiagnostics) = ConfigJsonMapper.NodeToDocument(root);
        diagnostics.AddRange(docDiagnostics);
        return (composedDoc, diagnostics);
    }

    private static void MergeEntitiesInto(
        Dictionary<string, JsonObject> target, JsonObject doc, string arrayKey, string label, List<string> diagnostics)
    {
        if (!doc.TryGetPropertyValue(arrayKey, out var arrNode) || arrNode is null)
        {
            return;
        }

        if (arrNode is not JsonArray arr)
        {
            diagnostics.Add($"{label}: '{arrayKey}' must be an array");
            return;
        }

        for (var i = 0; i < arr.Count; i++)
        {
            if (arr[i] is not JsonObject entity)
            {
                diagnostics.Add($"{label}: {arrayKey}[{i}] must be an object");
                continue;
            }

            if (!entity.TryGetPropertyValue("name", out var nameNode) ||
                nameNode is not JsonValue nv ||
                !nv.TryGetValue<string>(out var name) ||
                string.IsNullOrWhiteSpace(name))
            {
                diagnostics.Add($"{label}: {arrayKey}[{i}] missing name");
                continue;
            }

            target[name] = target.TryGetValue(name, out var existing)
                ? ShallowMergeEntity(existing, entity)
                : (JsonObject)entity.DeepClone();
        }
    }

    /// <summary>The D-I merge rule for one entity: clone <paramref name="existing"/>, then for every
    /// property present in <paramref name="incoming"/> (INCLUDING an explicit JSON null) overwrite
    /// the clone's value for that key entirely — never recurse into matching sub-objects/arrays. A
    /// property absent from <paramref name="incoming"/> leaves the clone's existing value untouched.
    /// This one loop body is simultaneously: scalar override, whole-list replace, whole-object
    /// replace, explicit-null clear, and absent-keeps — the "shallow" in "shallow field merge" is
    /// exactly "one JsonObject.Remove-then-set per top-level property, no deeper".</summary>
    private static JsonObject ShallowMergeEntity(JsonObject existing, JsonObject incoming)
    {
        var merged = (JsonObject)existing.DeepClone();
        foreach (var (key, value) in incoming)
        {
            merged[key] = value?.DeepClone();
        }

        return merged;
    }
}

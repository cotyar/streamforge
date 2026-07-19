using System.Text.Json;
using System.Text.Json.Nodes;
using StreamForge.Abstractions;
using YamlDotNet.Serialization;

namespace StreamForge.AppCore.Connectors.OpenApi;

/// <summary>
/// Pure OpenAPI v3 → <see cref="FieldDef"/> schema derivation (plan 006, D-F). No network access:
/// callers hand this the document text (already fetched by URL or supplied inline via
/// <see cref="OpenApiRef.DocInline"/>). Hand-rolled walker over <see cref="JsonNode"/> — no
/// Microsoft.OpenApi dependency, per D-A. YAML input is bridged to the same JSON node model via
/// YamlDotNet before walking; the walker itself only ever sees <see cref="JsonNode"/>.
///
/// <para>Selection: <see cref="OpenApiRef.SchemaPointer"/> (a "#/..." JSON pointer) wins when set;
/// otherwise <see cref="OpenApiRef.OperationId"/> selects an operation across every path/method, and
/// its response is chosen by trying "200", "201", "2XX", "default" in that order, then the first
/// "application/json*" media type within that response.</para>
///
/// <para>Resolution: internal "#/" $refs only, cycle-safe (a $ref already on the current resolution
/// path becomes a schemaless <see cref="FieldType.Json"/> field + a diagnostic). Any other $ref
/// (external or relative) is likewise a schemaless field + a diagnostic — external refs are never
/// followed (D-F, honest ceiling).</para>
///
/// <para>This type never throws for a malformed document or an unresolved reference — diagnostics
/// are the error channel. The sole exception is <see cref="FormatException"/> for a null/empty
/// <paramref name="docText"/>-equivalent input to <see cref="Derive"/>.</para>
/// </summary>
public static class OpenApiSchemaDeriver
{
    private static readonly string[] HttpMethods = ["get", "put", "post", "delete", "options", "head", "patch", "trace"];
    private static readonly string[] ResponsePreference = ["200", "201", "2XX", "default"];

    /// <summary>Derives a <see cref="SchemaDeriveResult"/> from an OpenAPI v3 document (JSON or YAML)
    /// per <paramref name="reference"/>. See the type doc comment for selection/resolution/type-map
    /// rules. Never throws for a bad document — diagnostics carry the error; <see cref="FormatException"/>
    /// is thrown only when <paramref name="docText"/> is null or empty.</summary>
    public static SchemaDeriveResult Derive(string docText, OpenApiRef reference)
    {
        if (string.IsNullOrEmpty(docText))
        {
            throw new FormatException("OpenAPI document text must not be null or empty.");
        }

        var diagnostics = new List<string>();

        if (reference is null)
        {
            diagnostics.Add("OpenApiRef is null; nothing to derive.");
            return new SchemaDeriveResult { Fields = [], Diagnostics = diagnostics };
        }

        JsonObject root;
        try
        {
            root = ParseDocument(docText);
        }
        catch (Exception ex)
        {
            diagnostics.Add($"Document could not be parsed as JSON or YAML: {ex.Message}");
            return new SchemaDeriveResult { Fields = [], Diagnostics = diagnostics };
        }

        JsonNode? selectedSchema;
        if (!string.IsNullOrEmpty(reference.SchemaPointer))
        {
            selectedSchema = ResolvePointer(root, reference.SchemaPointer);
            if (selectedSchema is null)
            {
                diagnostics.Add($"Schema pointer '{reference.SchemaPointer}' could not be resolved in the document.");
                return new SchemaDeriveResult { Fields = [], Diagnostics = diagnostics };
            }
        }
        else if (!string.IsNullOrEmpty(reference.OperationId))
        {
            selectedSchema = SelectOperationResponseSchema(root, reference.OperationId, diagnostics);
            if (selectedSchema is null)
            {
                return new SchemaDeriveResult { Fields = [], Diagnostics = diagnostics }; // diagnostic already added
            }
        }
        else
        {
            diagnostics.Add("OpenApiRef has neither SchemaPointer nor OperationId set; nothing to derive.");
            return new SchemaDeriveResult { Fields = [], Diagnostics = diagnostics };
        }

        var fields = DeriveRootFields(selectedSchema, root, diagnostics);
        return new SchemaDeriveResult { Fields = fields, Diagnostics = diagnostics };
    }

    // ---- Operation/response selection ----

    private static JsonNode? SelectOperationResponseSchema(JsonObject root, string operationId, List<string> diagnostics)
    {
        if (!root.TryGetPropertyValue("paths", out var pathsNode) || pathsNode is not JsonObject pathsObj)
        {
            diagnostics.Add($"Document has no 'paths'; operationId '{operationId}' not found.");
            return null;
        }

        foreach (var pathEntry in pathsObj)
        {
            if (pathEntry.Value is not JsonObject pathItem)
            {
                continue;
            }

            foreach (var method in HttpMethods)
            {
                if (!pathItem.TryGetPropertyValue(method, out var opNode) || opNode is not JsonObject opObj)
                {
                    continue;
                }

                if (!TryGetString(opObj, "operationId", out var opId) || opId != operationId)
                {
                    continue;
                }

                // Found the operation — select its response.
                if (!opObj.TryGetPropertyValue("responses", out var responsesNode) || responsesNode is not JsonObject responsesObj)
                {
                    diagnostics.Add($"Operation '{operationId}' has no 'responses'.");
                    return null;
                }

                JsonObject? chosenResponse = null;
                string? chosenStatus = null;
                foreach (var statusKey in ResponsePreference)
                {
                    if (responsesObj.TryGetPropertyValue(statusKey, out var responseNode) && responseNode is JsonObject respObj)
                    {
                        chosenResponse = respObj;
                        chosenStatus = statusKey;
                        break;
                    }
                }

                if (chosenResponse is null)
                {
                    diagnostics.Add($"Operation '{operationId}': no response among 200/201/2XX/default was found.");
                    return null;
                }

                if (!chosenResponse.TryGetPropertyValue("content", out var contentNode) || contentNode is not JsonObject contentObj)
                {
                    diagnostics.Add($"Operation '{operationId}': response '{chosenStatus}' has no content.");
                    return null;
                }

                foreach (var mediaEntry in contentObj)
                {
                    var mediaType = mediaEntry.Key.Split(';')[0].Trim();
                    if (!mediaType.StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (mediaEntry.Value is JsonObject mediaObj && mediaObj.TryGetPropertyValue("schema", out var schemaNode))
                    {
                        return schemaNode;
                    }
                }

                diagnostics.Add($"Operation '{operationId}': response '{chosenStatus}' has no application/json media type with a schema.");
                return null;
            }
        }

        diagnostics.Add($"operationId '{operationId}' was not found in the document.");
        return null;
    }

    // ---- Root-level derivation (the poller treats an array root as the items list) ----

    private static List<FieldDef> DeriveRootFields(JsonNode? schemaNode, JsonObject root, List<string> diagnostics)
    {
        var stack = new HashSet<string>();
        return ResolveAndUse(schemaNode, root, stack, "$root", diagnostics,
            (JsonObject? resolved) => DeriveRootFieldsFromResolved(resolved, root, stack, diagnostics));
    }

    private static List<FieldDef> DeriveRootFieldsFromResolved(JsonObject? schemaObj, JsonObject root, HashSet<string> stack, List<string> diagnostics)
    {
        if (schemaObj is null)
        {
            return []; // failure (external/cycle/unresolved) already diagnosed by ResolveAndUse
        }

        var type = TryGetString(schemaObj, "type", out var t) ? t : InferType(schemaObj);

        if (type == "array")
        {
            diagnostics.Add("Selected schema is an array; derived fields from its item schema (the response root is treated as the items list).");
            if (!schemaObj.TryGetPropertyValue("items", out var itemsNode) || itemsNode is null)
            {
                diagnostics.Add("Array schema has no 'items'; no fields derived.");
                return [];
            }

            return ResolveAndUse(itemsNode, root, stack, "$root item", diagnostics,
                (JsonObject? itemResolved) => DeriveRootFieldsFromResolved(itemResolved, root, stack, diagnostics));
        }

        if (schemaObj.TryGetPropertyValue("allOf", out var allOfNode) && allOfNode is JsonArray allOfArr)
        {
            var merged = MergeAllOf("$root", allOfArr, root, stack, diagnostics);
            diagnostics.Add($"Root schema: allOf merged across {allOfArr.Count} subschema(s) (later entries override earlier property definitions).");
            return BuildFieldList(merged, root, stack, diagnostics);
        }

        if (schemaObj.ContainsKey("oneOf") || schemaObj.ContainsKey("anyOf") || schemaObj.ContainsKey("not"))
        {
            diagnostics.Add("Root schema uses oneOf/anyOf/not, which is not supported for derivation; no fields derived.");
            return [];
        }

        if (schemaObj.TryGetPropertyValue("properties", out var propsNode) && propsNode is JsonObject propsObj && propsObj.Count > 0)
        {
            return BuildFieldList(propsObj, root, stack, diagnostics);
        }

        diagnostics.Add("Selected schema has no properties; no fields derived.");
        return [];
    }

    // ---- Field-level mapping (D-F type map) ----

    private static FieldDef MapSchema(string fieldName, JsonNode? schemaNode, JsonObject root, HashSet<string> stack, List<string> diagnostics)
        => ResolveAndUse(schemaNode, root, stack, fieldName, diagnostics,
            (JsonObject? resolved) => MapResolvedSchema(fieldName, resolved, root, stack, diagnostics));

    private static FieldDef MapResolvedSchema(string fieldName, JsonObject? schemaObj, JsonObject root, HashSet<string> stack, List<string> diagnostics)
    {
        if (schemaObj is null)
        {
            return new FieldDef(fieldName, FieldType.Json); // failure (external/cycle/unresolved) already diagnosed
        }

        if (schemaObj.TryGetPropertyValue("allOf", out var allOfNode) && allOfNode is JsonArray allOfArr)
        {
            var merged = MergeAllOf(fieldName, allOfArr, root, stack, diagnostics);
            diagnostics.Add($"Field '{fieldName}': allOf merged across {allOfArr.Count} subschema(s) (later entries override earlier property definitions).");
            return BuildObjectField(fieldName, merged, root, stack, diagnostics);
        }

        if (schemaObj.ContainsKey("oneOf"))
        {
            diagnostics.Add($"Field '{fieldName}': 'oneOf' is not supported for schema derivation; derived as schemaless Json.");
            return new FieldDef(fieldName, FieldType.Json);
        }

        if (schemaObj.ContainsKey("anyOf"))
        {
            diagnostics.Add($"Field '{fieldName}': 'anyOf' is not supported for schema derivation; derived as schemaless Json.");
            return new FieldDef(fieldName, FieldType.Json);
        }

        if (schemaObj.ContainsKey("not"))
        {
            diagnostics.Add($"Field '{fieldName}': 'not' is not supported for schema derivation; derived as schemaless Json.");
            return new FieldDef(fieldName, FieldType.Json);
        }

        var type = TryGetString(schemaObj, "type", out var t) ? t : InferType(schemaObj);

        switch (type)
        {
            case "string":
                var format = TryGetString(schemaObj, "format", out var f) ? f : null;
                return new FieldDef(fieldName, format is "date-time" or "date" ? FieldType.Timestamp : FieldType.String);

            case "number":
                return new FieldDef(fieldName, FieldType.Double);

            case "integer":
                return new FieldDef(fieldName, FieldType.Long);

            case "boolean":
                return new FieldDef(fieldName, FieldType.Bool);

            case "object":
                return BuildObjectField(fieldName, schemaObj.TryGetPropertyValue("properties", out var p) ? p as JsonObject : null, root, stack, diagnostics);

            case "array":
                if (!schemaObj.TryGetPropertyValue("items", out var itemsNode) || itemsNode is null)
                {
                    diagnostics.Add($"Field '{fieldName}': array schema has no 'items'; derived as schemaless Json.");
                    return new FieldDef(fieldName, FieldType.Json, IsArray: true);
                }

                var itemField = MapSchema(fieldName, itemsNode, root, stack, diagnostics);
                return itemField with { IsArray = true };

            default:
                if (type is not null)
                {
                    diagnostics.Add($"Field '{fieldName}': unrecognized schema type '{type}'; derived as schemaless Json.");
                }

                return new FieldDef(fieldName, FieldType.Json); // schemaless: no explicit/inferrable type, or object w/o properties (free-form)
        }
    }

    private static string? InferType(JsonObject schemaObj)
    {
        if (schemaObj.TryGetPropertyValue("properties", out var props) && props is JsonObject)
        {
            return "object";
        }

        if (schemaObj.TryGetPropertyValue("items", out _))
        {
            return "array";
        }

        return null;
    }

    private static FieldDef BuildObjectField(string fieldName, JsonObject? properties, JsonObject root, HashSet<string> stack, List<string> diagnostics)
    {
        if (properties is null || properties.Count == 0)
        {
            return new FieldDef(fieldName, FieldType.Json); // schemaless: no properties / free-form additionalProperties
        }

        return new FieldDef(fieldName, FieldType.Json, Children: BuildFieldList(properties, root, stack, diagnostics));
    }

    private static List<FieldDef> BuildFieldList(JsonObject properties, JsonObject root, HashSet<string> stack, List<string> diagnostics)
    {
        var fields = new List<FieldDef>();
        foreach (var kv in properties)
        {
            fields.Add(MapSchema(kv.Key, kv.Value, root, stack, diagnostics));
        }

        return fields;
    }

    // ---- allOf (best-effort shallow property merge, D-F) ----

    private static JsonObject MergeAllOf(string fieldName, JsonArray allOf, JsonObject root, HashSet<string> stack, List<string> diagnostics)
    {
        var merged = new JsonObject();
        foreach (var sub in allOf)
        {
            MergeSubschemaInto(merged, sub, root, stack, fieldName, diagnostics);
        }

        return merged;
    }

    private static void MergeSubschemaInto(JsonObject merged, JsonNode? sub, JsonObject root, HashSet<string> stack, string fieldName, List<string> diagnostics)
    {
        ResolveAndUse(sub, root, stack, fieldName, diagnostics, (JsonObject? resolved) =>
        {
            if (resolved is not null && resolved.TryGetPropertyValue("properties", out var propsNode) && propsNode is JsonObject propsObj)
            {
                foreach (var kv in propsObj)
                {
                    // Later allOf entries win (dictionary indexer overwrite); DeepClone since a JsonNode
                    // can only ever be attached under one parent.
                    merged[kv.Key] = kv.Value?.DeepClone();
                }
            }

            return true; // no meaningful result — MergeSubschemaInto mutates `merged` as its side effect
        });
    }

    // ---- Internal "#/" $ref resolution, cycle-safe (D-F) ----

    /// <summary>If <paramref name="node"/> is a "$ref" object, resolves it (following chains, tracking
    /// the resolution path in <paramref name="stack"/> for cycle detection) and invokes
    /// <paramref name="use"/> with the final non-$ref object — bracketing the push/pop around the
    /// *entire* downstream processing done inside <paramref name="use"/>, so a cycle several levels
    /// deep is still caught. External refs, cycles, and unresolved pointers add a diagnostic and invoke
    /// <paramref name="use"/> with <c>null</c> (never throws).</summary>
    private static TResult ResolveAndUse<TResult>(
        JsonNode? node,
        JsonObject root,
        HashSet<string> stack,
        string fieldName,
        List<string> diagnostics,
        Func<JsonObject?, TResult> use)
    {
        if (node is JsonObject obj && TryGetString(obj, "$ref", out var refValue))
        {
            if (!refValue.StartsWith("#/", StringComparison.Ordinal))
            {
                diagnostics.Add($"Field '{fieldName}': external $ref '{refValue}' is not supported; derived as schemaless Json.");
                return use(null);
            }

            if (!stack.Add(refValue))
            {
                diagnostics.Add($"Field '{fieldName}': cycle detected resolving $ref '{refValue}'; derived as schemaless Json.");
                return use(null);
            }

            try
            {
                var target = ResolvePointer(root, refValue);
                if (target is not JsonObject)
                {
                    diagnostics.Add($"Field '{fieldName}': $ref '{refValue}' could not be resolved; derived as schemaless Json.");
                    return use(null);
                }

                return ResolveAndUse(target, root, stack, fieldName, diagnostics, use); // follow chains
            }
            finally
            {
                stack.Remove(refValue);
            }
        }

        return use(node as JsonObject);
    }

    private static JsonNode? ResolvePointer(JsonObject root, string pointer)
    {
        if (string.IsNullOrEmpty(pointer))
        {
            return null;
        }

        var raw = pointer.StartsWith('#') ? pointer[1..] : pointer;
        if (raw.Length == 0)
        {
            return root;
        }

        if (!raw.StartsWith('/'))
        {
            return null; // only "#/..." pointers are supported
        }

        JsonNode? current = root;
        foreach (var rawSegment in raw[1..].Split('/'))
        {
            var segment = UnescapePointerSegment(rawSegment);
            switch (current)
            {
                case JsonObject o when o.TryGetPropertyValue(segment, out var next):
                    current = next;
                    break;
                case JsonArray a when int.TryParse(segment, out var idx) && idx >= 0 && idx < a.Count:
                    current = a[idx];
                    break;
                default:
                    return null;
            }
        }

        return current;
    }

    private static string UnescapePointerSegment(string segment) => segment.Replace("~1", "/").Replace("~0", "~");

    private static bool TryGetString(JsonObject obj, string key, out string value)
    {
        if (obj.TryGetPropertyValue(key, out var node) && node is JsonValue jv && jv.TryGetValue<string>(out var s))
        {
            value = s;
            return true;
        }

        value = "";
        return false;
    }

    // ---- JSON/YAML document loading (D-A: YAML bridged to the same JSON node model at the boundary) ----

    private static JsonObject ParseDocument(string docText)
    {
        JsonNode? jsonNode = null;
        try
        {
            jsonNode = JsonNode.Parse(docText);
        }
        catch (JsonException)
        {
            // Not JSON — fall through to YAML.
        }

        if (jsonNode is JsonObject jsonObj)
        {
            return jsonObj;
        }

        var yamlObj = ParseYamlDocument(docText);
        if (yamlObj is not null)
        {
            return yamlObj;
        }

        throw new InvalidOperationException("Document is neither a valid JSON object nor a valid YAML mapping.");
    }

    private static JsonObject? ParseYamlDocument(string docText)
    {
        var deserializer = new DeserializerBuilder().Build();
        var yamlValue = deserializer.Deserialize<object?>(docText);
        return YamlToJson(yamlValue) as JsonObject;
    }

    private static JsonNode? YamlToJson(object? value) => value switch
    {
        null => null,
        string s => JsonValue.Create(s),
        bool b => JsonValue.Create(b),
        int i => JsonValue.Create(i),
        long l => JsonValue.Create(l),
        double d => JsonValue.Create(d),
        decimal m => JsonValue.Create((double)m),
        IDictionary<object, object> map => YamlMapToJsonObject(map),
        IEnumerable<object> seq => YamlSeqToJsonArray(seq),
        _ => JsonValue.Create(value.ToString()),
    };

    private static JsonObject YamlMapToJsonObject(IDictionary<object, object> map)
    {
        var obj = new JsonObject();
        foreach (var kv in map)
        {
            obj[kv.Key?.ToString() ?? ""] = YamlToJson(kv.Value);
        }

        return obj;
    }

    private static JsonArray YamlSeqToJsonArray(IEnumerable<object> seq)
    {
        var arr = new JsonArray();
        foreach (var item in seq)
        {
            arr.Add(YamlToJson(item));
        }

        return arr;
    }
}

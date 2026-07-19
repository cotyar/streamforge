using System.Text.Json;
using StreamForge.Abstractions;
using YamlDotNet.Serialization;

namespace StreamForge.AppCore.Connectors.Mapping;

/// <summary>
/// Parses a user-supplied "mapping document" (JSON or YAML text, plan 006 D-B) into a
/// <see cref="MappingSpec"/>. One canonical model, two accepted encodings: YAML is converted to its
/// JSON-equivalent representation first (via YamlDotNet's default-to-<see cref="object"/> scalar type
/// inference + its JSON-compatible serializer), and everything downstream of that point — validation,
/// diagnostics, <see cref="MappingSpec"/> construction — runs against a single <see cref="JsonElement"/>
/// tree, regardless of which encoding the caller used.
///
/// <para>Document shape (camelCase keys):</para>
/// <code>
/// {
///   "itemsPath": "$.data.trades[*]",       // optional, default "$"
///   "dedupKeyField": "id",                 // optional; must name a field below
///   "timestampField": "ts",                // optional; must name a field below
///   "fields": [
///     { "sourcePath": "price", "field": { "name": "price", "type": "Double" } },
///     { "field": { "name": "id", "type": "String" } }               // sourcePath defaults to Field.Name
///   ]
/// }
/// </code>
///
/// <para><b>Diagnostics, not exceptions</b>: anything short of a fatally unparseable document (not
/// valid JSON and not valid YAML, or a root that isn't an object) still returns a best-effort
/// <see cref="MappingSpec"/> alongside a list of human-readable diagnostics — unknown top-level keys,
/// an empty/duplicate field name, an invalid JSONPath-lite path (syntax-checked via
/// <see cref="JsonPathLite"/>, not evaluated against data), or a <c>dedupKeyField</c>/
/// <c>timestampField</c> that doesn't name any field in <c>fields</c>. <see cref="Parse"/>'s
/// <c>Spec</c> result is <c>null</c> if and only if the document could not be parsed at all.</para>
/// </summary>
public static class MappingLoader
{
    private static readonly HashSet<string> KnownTopLevelKeys =
        new(StringComparer.Ordinal) { "itemsPath", "dedupKeyField", "timestampField", "fields" };

    /// <summary>Parses a mapping document (JSON or YAML). See type-level doc for shape and
    /// diagnostics semantics.</summary>
    public static (MappingSpec? Spec, IReadOnlyList<string> Diagnostics) Parse(string document)
    {
        var diagnostics = new List<string>();

        if (string.IsNullOrWhiteSpace(document))
        {
            diagnostics.Add("mapping document is empty.");
            return (null, diagnostics);
        }

        JsonDocument? jsonDoc;
        try
        {
            jsonDoc = JsonDocument.Parse(document);
        }
        catch (JsonException)
        {
            var json = TryConvertYamlToJson(document, out var yamlError);
            if (json is null)
            {
                diagnostics.Add($"mapping document is neither valid JSON nor valid YAML: {yamlError}");
                return (null, diagnostics);
            }

            try
            {
                jsonDoc = JsonDocument.Parse(json);
            }
            catch (JsonException ex)
            {
                diagnostics.Add($"mapping document YAML converted to invalid JSON internally: {ex.Message}");
                return (null, diagnostics);
            }
        }

        using (jsonDoc)
        {
            var root = jsonDoc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                diagnostics.Add($"mapping document root must be an object, got {root.ValueKind}.");
                return (null, diagnostics);
            }

            var spec = ParseSpec(root, diagnostics);
            return (spec, diagnostics);
        }
    }

    /// <summary>Converts YAML text to a JSON string via YamlDotNet's default object-graph
    /// deserialization (which infers scalar types — bool/int/double/null/string) followed by its
    /// JSON-compatible serializer. Returns null (with <paramref name="error"/> set) if the text isn't
    /// valid YAML either.</summary>
    private static string? TryConvertYamlToJson(string document, out string? error)
    {
        try
        {
            var deserializer = new Deserializer();
            var yamlObject = deserializer.Deserialize(new StringReader(document));
            if (yamlObject is null)
            {
                error = null;
                return "null";
            }

            var serializer = new SerializerBuilder().JsonCompatible().Build();
            error = null;
            return serializer.Serialize(yamlObject);
        }
        catch (Exception ex)
        {
            error = ex.Message;
            return null;
        }
    }

    private static MappingSpec ParseSpec(JsonElement root, List<string> diagnostics)
    {
        foreach (var property in root.EnumerateObject())
        {
            if (!KnownTopLevelKeys.Contains(property.Name))
            {
                diagnostics.Add($"unknown top-level key '{property.Name}'.");
            }
        }

        var spec = new MappingSpec();

        if (root.TryGetProperty("itemsPath", out var itemsPathEl))
        {
            if (itemsPathEl.ValueKind == JsonValueKind.String)
            {
                var path = itemsPathEl.GetString()!;
                ValidatePathSyntax(path, "itemsPath", diagnostics);
                spec.ItemsPath = path;
            }
            else
            {
                diagnostics.Add($"itemsPath must be a string, got {itemsPathEl.ValueKind}.");
            }
        }

        if (root.TryGetProperty("dedupKeyField", out var dedupEl))
        {
            if (dedupEl.ValueKind == JsonValueKind.String)
            {
                spec.DedupKeyField = dedupEl.GetString();
            }
            else if (dedupEl.ValueKind != JsonValueKind.Null)
            {
                diagnostics.Add($"dedupKeyField must be a string, got {dedupEl.ValueKind}.");
            }
        }

        if (root.TryGetProperty("timestampField", out var tsEl))
        {
            if (tsEl.ValueKind == JsonValueKind.String)
            {
                spec.TimestampField = tsEl.GetString();
            }
            else if (tsEl.ValueKind != JsonValueKind.Null)
            {
                diagnostics.Add($"timestampField must be a string, got {tsEl.ValueKind}.");
            }
        }

        if (root.TryGetProperty("fields", out var fieldsEl))
        {
            if (fieldsEl.ValueKind == JsonValueKind.Array)
            {
                ParseFields(fieldsEl, spec, diagnostics);
            }
            else
            {
                diagnostics.Add($"fields must be an array, got {fieldsEl.ValueKind}.");
            }
        }

        if (spec.Fields.Count == 0)
        {
            diagnostics.Add("mapping document defines no fields.");
        }

        var fieldNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var f in spec.Fields)
        {
            fieldNames.Add(f.Field.Name);
        }

        if (spec.DedupKeyField is { Length: > 0 } dedupKey && !fieldNames.Contains(dedupKey))
        {
            diagnostics.Add($"dedupKeyField '{dedupKey}' is not among the mapped fields.");
        }

        if (spec.TimestampField is { Length: > 0 } tsKey && !fieldNames.Contains(tsKey))
        {
            diagnostics.Add($"timestampField '{tsKey}' is not among the mapped fields.");
        }

        return spec;
    }

    private static void ParseFields(JsonElement fieldsEl, MappingSpec spec, List<string> diagnostics)
    {
        var seenNames = new HashSet<string>(StringComparer.Ordinal);
        var duplicateNames = new List<string>();
        var index = 0;

        foreach (var fieldEl in fieldsEl.EnumerateArray())
        {
            var entry = ParseFieldMapEntry(fieldEl, index, diagnostics);
            if (entry is not null)
            {
                var name = entry.Field.Name;
                if (name.Length == 0)
                {
                    diagnostics.Add($"fields[{index}] has an empty field name.");
                }
                else if (!seenNames.Add(name))
                {
                    duplicateNames.Add(name);
                }

                spec.Fields.Add(entry);
            }

            index++;
        }

        foreach (var dup in duplicateNames)
        {
            diagnostics.Add($"duplicate field name '{dup}'.");
        }
    }

    private static FieldMapEntry? ParseFieldMapEntry(JsonElement fieldEl, int index, List<string> diagnostics)
    {
        if (fieldEl.ValueKind != JsonValueKind.Object)
        {
            diagnostics.Add($"fields[{index}] must be an object, got {fieldEl.ValueKind}.");
            return null;
        }

        string? sourcePath = null;
        if (fieldEl.TryGetProperty("sourcePath", out var sp))
        {
            if (sp.ValueKind == JsonValueKind.String)
            {
                sourcePath = sp.GetString();
                if (sourcePath is not null)
                {
                    ValidatePathSyntax(sourcePath, $"fields[{index}].sourcePath", diagnostics);
                }
            }
            else if (sp.ValueKind != JsonValueKind.Null)
            {
                diagnostics.Add($"fields[{index}].sourcePath must be a string, got {sp.ValueKind}.");
            }
        }

        if (!fieldEl.TryGetProperty("field", out var fieldDefEl) || fieldDefEl.ValueKind != JsonValueKind.Object)
        {
            diagnostics.Add($"fields[{index}].field is required and must be an object.");
            return new FieldMapEntry { SourcePath = sourcePath, Field = new FieldDef("", FieldType.String) };
        }

        var fieldDef = ParseFieldDef(fieldDefEl, $"fields[{index}].field", diagnostics);
        return new FieldMapEntry { SourcePath = sourcePath, Field = fieldDef };
    }

    private static FieldDef ParseFieldDef(JsonElement el, string context, List<string> diagnostics)
    {
        var name = "";
        if (el.TryGetProperty("name", out var nameEl) && nameEl.ValueKind == JsonValueKind.String)
        {
            name = nameEl.GetString() ?? "";
        }
        else
        {
            diagnostics.Add($"{context}.name is required and must be a string.");
        }

        var type = FieldType.String;
        if (el.TryGetProperty("type", out var typeEl))
        {
            if (typeEl.ValueKind == JsonValueKind.String)
            {
                var typeText = typeEl.GetString() ?? "";
                if (!Enum.TryParse(typeText, ignoreCase: true, out type))
                {
                    diagnostics.Add($"{context}.type '{typeText}' is not a recognized field type; defaulting to String.");
                    type = FieldType.String;
                }
            }
            else
            {
                diagnostics.Add($"{context}.type must be a string, got {typeEl.ValueKind}.");
            }
        }

        var isArray = false;
        if (el.TryGetProperty("isArray", out var isArrayEl))
        {
            if (isArrayEl.ValueKind is JsonValueKind.True or JsonValueKind.False)
            {
                isArray = isArrayEl.GetBoolean();
            }
            else
            {
                diagnostics.Add($"{context}.isArray must be a boolean, got {isArrayEl.ValueKind}.");
            }
        }

        List<FieldDef>? children = null;
        if (el.TryGetProperty("children", out var childrenEl))
        {
            if (childrenEl.ValueKind == JsonValueKind.Array)
            {
                children = [];
                var childIndex = 0;
                foreach (var childEl in childrenEl.EnumerateArray())
                {
                    if (childEl.ValueKind == JsonValueKind.Object)
                    {
                        children.Add(ParseFieldDef(childEl, $"{context}.children[{childIndex}]", diagnostics));
                    }
                    else
                    {
                        diagnostics.Add($"{context}.children[{childIndex}] must be an object, got {childEl.ValueKind}.");
                    }

                    childIndex++;
                }
            }
            else
            {
                diagnostics.Add($"{context}.children must be an array, got {childrenEl.ValueKind}.");
            }
        }

        return new FieldDef(name, type, children, isArray);
    }

    /// <summary>Checks a path string against the JSONPath-lite grammar only (no data to evaluate
    /// against yet at load time) by running it against an empty dummy object and catching the
    /// resulting <see cref="FormatException"/>, if any.</summary>
    private static void ValidatePathSyntax(string path, string context, List<string> diagnostics)
    {
        try
        {
            using var dummy = JsonDocument.Parse("{}");
            JsonPathLite.Select(dummy.RootElement, path);
        }
        catch (FormatException ex)
        {
            diagnostics.Add($"{context}: invalid path '{path}': {ex.Message}");
        }
    }
}

using System.Text.Json.Nodes;
using StreamsForge.Abstractions;

namespace StreamsForge.Api;

/// <summary>
/// Derives a <em>per-entity</em> OpenAPI document from the application's own generated one — the REST
/// twin of the per-entity <c>.proto</c> downloads (<c>/api/tables/{id}/proto</c> and friends).
///
/// <para>Why derive rather than hand-author: the application document is produced by
/// <c>Microsoft.AspNetCore.OpenApi</c> from the real endpoint metadata, so it stays correct as routes,
/// parameters and DTOs change. This class only ever <em>subsets and rewrites</em> that document —
/// it never invents an operation. Consequently a new route under <c>/api/tables/{id}/…</c> shows up in
/// every table's document for free.</para>
///
/// <para>The three rewrites that make the result directly usable:</para>
/// <list type="number">
/// <item>paths are filtered to the ones that belong to this one entity, and the concrete id/name is
/// substituted for the <c>{id}</c>/<c>{name}</c> template so every path in the document is a URL you
/// can call as-is (the now-redundant path parameter is dropped from the operations);</item>
/// <item>the entity's real output schema replaces the free-form <c>row</c> / <c>events</c> objects that
/// <c>Dictionary&lt;string, object?&gt;</c> otherwise generates — most of the value of a per-entity
/// spec, and the reason this file knows about <see cref="FieldDef"/>;</item>
/// <item>component schemas unreachable from the surviving paths are pruned, so Scalar's "Models"
/// section describes this entity rather than the whole catalog.</item>
/// </list>
///
/// <para>Everything here is pure <see cref="JsonNode"/> manipulation on the <em>serialized</em>
/// document rather than surgery on <c>OpenApiDocument</c> objects: Microsoft.OpenApi v2 reference
/// objects are bound to their host document, so moving paths between documents is a footgun, while
/// re-parsing the serialized form is trivially correct and keeps this class host-free and testable.</para>
/// </summary>
public static class EntityOpenApiLogic
{
    /// <summary>Kept alongside the entity's own routes so the document is usable end-to-end: without a
    /// token none of the operations below it can be tried, and this is the route that mints one.</summary>
    public const string LoginPath = "/api/auth/login";

    private const string SchemaRefPrefix = "#/components/schemas/";

    /// <summary>
    /// Subsets <paramref name="appDocument"/> (the serialized application OpenAPI document) down to one
    /// entity. <paramref name="pathPrefix"/>/<paramref name="parameterName"/> select the routes
    /// (<c>"/api/tables"</c> + <c>"id"</c> keeps <c>/api/tables/{id}</c> and everything under it);
    /// <paramref name="parameterValue"/> is substituted for the template. <paramref name="rowSchema"/>,
    /// when supplied, is installed as <c>components/schemas/{rowSchemaName}</c> and referenced from the
    /// row-shaped payloads. The input node is never mutated.
    /// </summary>
    public static JsonObject BuildEntityDocument(
        JsonNode appDocument,
        string pathPrefix,
        string parameterName,
        string parameterValue,
        string title,
        string description,
        string tag,
        JsonObject? rowSchema = null,
        string? rowSchemaName = null)
    {
        var doc = appDocument.DeepClone().AsObject();

        if (doc["info"] is JsonObject info)
        {
            info["title"] = title;
            info["description"] = description;
        }

        var template = $"{pathPrefix}/{{{parameterName}}}";
        var concrete = $"{pathPrefix}/{parameterValue}";
        var sourcePaths = doc["paths"] as JsonObject ?? [];
        var keptPaths = new JsonObject();

        foreach (var (path, item) in sourcePaths.ToList())
        {
            if (item is not JsonObject pathItem)
            {
                continue;
            }

            bool isEntityPath = path == template || path.StartsWith(template + "/", StringComparison.Ordinal);
            if (!isEntityPath && path != LoginPath)
            {
                continue;
            }

            sourcePaths.Remove(path);

            if (isEntityPath)
            {
                DropPathParameter(pathItem, parameterName);
            }

            Retag(pathItem, isEntityPath ? tag : "auth");
            keptPaths[isEntityPath ? concrete + path[template.Length..] : path] = pathItem;
        }

        doc["paths"] = keptPaths;

        // Tag list mirrors what the operations actually carry now — otherwise Scalar's sidebar shows the
        // generator's default assembly-name tag with nothing under it.
        var tags = new JsonArray(new JsonObject { ["name"] = tag, ["description"] = description });
        if (keptPaths.ContainsKey(LoginPath))
        {
            tags.Add(new JsonObject
            {
                ["name"] = "auth",
                ["description"] = "Exchange credentials for the Bearer token every operation above requires.",
            });
        }

        doc["tags"] = tags;

        if (rowSchema is not null && rowSchemaName is not null)
        {
            InstallRowSchema(doc, rowSchema, rowSchemaName);
        }

        PruneUnreachableSchemas(doc);
        return doc;
    }

    /// <summary>The id/name is baked into the path now, so the parameter would render as an editable box
    /// that can only break the request. Removed at both the path-item and the operation level.</summary>
    private static void DropPathParameter(JsonObject pathItem, string parameterName)
    {
        RemoveFrom(pathItem);
        foreach (var (_, op) in pathItem)
        {
            if (op is JsonObject operation)
            {
                RemoveFrom(operation);
            }
        }

        void RemoveFrom(JsonObject holder)
        {
            if (holder["parameters"] is not JsonArray parameters)
            {
                return;
            }

            for (var i = parameters.Count - 1; i >= 0; i--)
            {
                if (parameters[i] is JsonObject p &&
                    (string?)p["name"] == parameterName &&
                    (string?)p["in"] == "path")
                {
                    parameters.RemoveAt(i);
                }
            }

            if (parameters.Count == 0)
            {
                holder.Remove("parameters");
            }
        }
    }

    private static void Retag(JsonObject pathItem, string tag)
    {
        foreach (var (key, op) in pathItem)
        {
            // Path items also carry non-operation members (summary/description/parameters/servers).
            if (op is JsonObject operation && IsHttpMethod(key))
            {
                operation["tags"] = new JsonArray(tag);
            }
        }
    }

    private static bool IsHttpMethod(string key) => key is
        "get" or "put" or "post" or "delete" or "options" or "head" or "patch" or "trace";

    /// <summary>
    /// Installs the entity's real row schema and points the free-form row payloads at it. The targets are
    /// matched structurally rather than by component name: every <c>row</c> property and every
    /// <c>events</c> array item that the generator emitted as a schemaless object (which is all
    /// <c>Dictionary&lt;string, object?&gt;</c> ever produces) becomes a reference to
    /// <paramref name="rowSchemaName"/>. That covers the table rows/search/history payloads, a pipeline's
    /// results and a source's ingest body without this file hard-coding any DTO's generated name.
    /// </summary>
    private static void InstallRowSchema(JsonObject doc, JsonObject rowSchema, string rowSchemaName)
    {
        var components = doc["components"] as JsonObject;
        if (components is null)
        {
            components = [];
            doc["components"] = components;
        }

        var schemas = components["schemas"] as JsonObject;
        if (schemas is null)
        {
            schemas = [];
            components["schemas"] = schemas;
        }

        schemas[rowSchemaName] = rowSchema;
        var reference = SchemaRefPrefix + rowSchemaName;

        foreach (var (name, schema) in schemas.ToList())
        {
            if (name == rowSchemaName || schema is not JsonObject { } obj ||
                obj["properties"] is not JsonObject properties)
            {
                continue;
            }

            if (IsSchemalessObject(properties["row"]))
            {
                properties["row"] = Ref(reference);
            }

            if (properties["events"] is JsonObject { } events &&
                (string?)events["type"] == "array" && IsSchemalessObject(events["items"]))
            {
                events["items"] = Ref(reference);
            }
        }
    }

    /// <summary>An <c>object</c> with no declared properties — what a <c>Dictionary&lt;string, object?&gt;</c>
    /// becomes, and the only thing worth replacing with a real schema.</summary>
    private static bool IsSchemalessObject(JsonNode? node) =>
        node is JsonObject o && o["properties"] is null && o["$ref"] is null &&
        (o["type"] is null || (string?)o["type"] == "object");

    private static JsonObject Ref(string reference) => new() { ["$ref"] = reference };

    /// <summary>Drops component schemas no surviving path can reach. Walks <c>$ref</c>s transitively from
    /// the paths so a kept schema keeps everything it points at.</summary>
    private static void PruneUnreachableSchemas(JsonObject doc)
    {
        if (doc["components"] is not JsonObject components ||
            components["schemas"] is not JsonObject schemas)
        {
            return;
        }

        var reachable = new HashSet<string>(StringComparer.Ordinal);
        var queue = new Queue<string>();

        foreach (var name in CollectRefs(doc["paths"]))
        {
            if (reachable.Add(name))
            {
                queue.Enqueue(name);
            }
        }

        while (queue.Count > 0)
        {
            foreach (var name in CollectRefs(schemas[queue.Dequeue()]))
            {
                if (reachable.Add(name))
                {
                    queue.Enqueue(name);
                }
            }
        }

        foreach (var (name, _) in schemas.ToList())
        {
            if (!reachable.Contains(name))
            {
                schemas.Remove(name);
            }
        }
    }

    private static IEnumerable<string> CollectRefs(JsonNode? node)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var (key, value) in obj)
                {
                    if (key == "$ref" && value is JsonValue &&
                        (string?)value is { } r && r.StartsWith(SchemaRefPrefix, StringComparison.Ordinal))
                    {
                        yield return r[SchemaRefPrefix.Length..];
                        continue;
                    }

                    foreach (var nested in CollectRefs(value))
                    {
                        yield return nested;
                    }
                }

                break;
            case JsonArray array:
                foreach (var item in array)
                {
                    foreach (var nested in CollectRefs(item))
                    {
                        yield return nested;
                    }
                }

                break;
        }
    }

    /// <summary>
    /// JSON Schema for one row of an entity's output, from the same <see cref="FieldDef"/> list that
    /// drives the .proto download — so the two per-entity contracts describe the same shape. Returns null
    /// for an entity with no compiled schema (the document is still served, just with the generator's
    /// free-form row objects).
    /// </summary>
    public static JsonObject? RowSchemaFromFields(IReadOnlyList<FieldDef> fields, string? description = null)
    {
        if (fields.Count == 0)
        {
            return null;
        }

        var schema = ObjectSchema(fields);
        if (description is not null)
        {
            schema["description"] = description;
        }

        return schema;
    }

    private static JsonObject ObjectSchema(IReadOnlyList<FieldDef> fields)
    {
        var properties = new JsonObject();
        foreach (var field in fields)
        {
            properties[field.Name] = FieldSchema(field);
        }

        return new JsonObject { ["type"] = "object", ["properties"] = properties };
    }

    private static JsonNode FieldSchema(FieldDef field)
    {
        JsonObject scalar = field.Type switch
        {
            FieldType.String => new JsonObject { ["type"] = "string" },
            FieldType.Double => new JsonObject { ["type"] = "number", ["format"] = "double" },
            FieldType.Long => new JsonObject { ["type"] = "integer", ["format"] = "int64" },
            FieldType.Bool => new JsonObject { ["type"] = "boolean" },
            // Timestamps travel as epoch milliseconds everywhere in StreamsForge — the .proto download
            // types them int64 with the same note (DescriptorFactory), so don't claim date-time here.
            FieldType.Timestamp => new JsonObject
            {
                ["type"] = "integer",
                ["format"] = "int64",
                ["description"] = "epoch milliseconds",
            },
            FieldType.Json when field.Children is { Count: > 0 } => ObjectSchema(field.Children),
            // Schemaless JSON: anything goes, and saying so is more honest than guessing "object".
            _ => [],
        };

        return field.IsArray
            ? new JsonObject { ["type"] = "array", ["items"] = scalar }
            : scalar;
    }
}

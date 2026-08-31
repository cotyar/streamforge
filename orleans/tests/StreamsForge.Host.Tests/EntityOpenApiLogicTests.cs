using System.Text.Json.Nodes;
using StreamsForge.Abstractions;
using StreamsForge.Api;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Unit tests for <see cref="EntityOpenApiLogic"/> — the pure document-subsetting logic behind
/// GET /api/{tables|pipelines|sources}/{id-or-name}/openapi.json and the per-entity Scalar pages.
/// No HTTP harness and no OpenAPI generator: the input is a hand-written stand-in for the application
/// document with exactly the shapes the real generator produces (templated paths, path parameters,
/// <c>Dictionary&lt;string, object?&gt;</c> rendered as a schemaless object, cross-referencing component
/// schemas), which is what lets these assertions be exact. Mirrors PlanEndpointsLogicTests' "logic lives
/// in a pure static class, test it directly" convention.
/// </summary>
public class EntityOpenApiLogicTests
{
    /// <summary>Stand-in for /openapi/v1.json: one entity family with a sub-route and a second path
    /// parameter, one unrelated family, the login route, and a component graph deep enough to prove
    /// transitive reachability (RowsResponse -> RowDto -> the injected row schema).</summary>
    private static JsonNode AppDocument() => JsonNode.Parse(
        """
        {
          "openapi": "3.1.1",
          "info": { "title": "StreamsForge API", "version": "1.0.0", "description": "whole API" },
          "paths": {
            "/api/tables": { "get": { "tags": ["StreamsForge.Host"], "responses": { "200": {} } } },
            "/api/tables/{id}": {
              "get": {
                "tags": ["StreamsForge.Host"],
                "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
                "responses": { "200": { "content": { "application/json": {
                  "schema": { "$ref": "#/components/schemas/RowsResponse" } } } } }
              }
            },
            "/api/tables/{id}/history/lookup": {
              "post": {
                "tags": ["StreamsForge.Host"],
                "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
                "requestBody": { "content": { "application/json": {
                  "schema": { "$ref": "#/components/schemas/HistoryLookupRequest" } } } },
                "responses": { "200": {} }
              }
            },
            "/api/tables/{id}/keys/{keyId}": {
              "delete": {
                "tags": ["StreamsForge.Host"],
                "parameters": [
                  { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } },
                  { "name": "keyId", "in": "path", "required": true, "schema": { "type": "string" } }
                ],
                "responses": { "204": {} }
              }
            },
            "/api/pipelines/{id}": { "get": { "tags": ["StreamsForge.Host"], "responses": { "200": {} } } },
            "/api/auth/login": {
              "post": {
                "tags": ["StreamsForge.Host"],
                "requestBody": { "content": { "application/json": {
                  "schema": { "$ref": "#/components/schemas/LoginRequest" } } } },
                "responses": { "200": {} }
              }
            }
          },
          "components": {
            "schemas": {
              "RowsResponse": { "type": "object", "properties": {
                "rows": { "type": "array", "items": { "$ref": "#/components/schemas/RowDto" } } } },
              "RowDto": { "type": "object", "properties": {
                "row": { "type": "object" },
                "weight": { "type": "integer", "format": "int64" } } },
              "HistoryLookupRequest": { "type": "object", "properties": { "row": { "type": "object" } } },
              "IngestRequest": { "type": "object", "properties": {
                "events": { "type": "array", "items": { "type": "object" } } } },
              "LoginRequest": { "type": "object", "properties": { "username": { "type": "string" } } },
              "UnrelatedDto": { "type": "object", "properties": { "x": { "type": "string" } } }
            },
            "securitySchemes": { "Bearer": { "type": "http", "scheme": "bearer" } }
          },
          "security": [{ "Bearer": [] }]
        }
        """)!;

    private static JsonObject BuildTableDocument(JsonObject? rowSchema = null, string? rowSchemaName = null) =>
        EntityOpenApiLogic.BuildEntityDocument(
            AppDocument(), "/api/tables", "id", "tbl-42",
            "StreamsForge table \"positions\"", "one table", "positions", rowSchema, rowSchemaName);

    [Fact]
    public void KeepsOnlyTheEntitysPathsPlusLogin()
    {
        var paths = BuildTableDocument()["paths"]!.AsObject().Select(p => p.Key).ToList();

        Assert.Equal(
            [
                "/api/auth/login",
                "/api/tables/tbl-42",
                "/api/tables/tbl-42/history/lookup",
                "/api/tables/tbl-42/keys/{keyId}",
            ],
            paths.Order().ToList());
    }

    [Fact]
    public void SubstitutesTheEntityKeyAndLeavesOtherParametersTemplated()
    {
        var paths = BuildTableDocument()["paths"]!.AsObject();

        // The entity's own {id} is gone from every path...
        Assert.DoesNotContain(paths, p => p.Key.Contains("{id}"));
        // ...but an unrelated path parameter is still a parameter, not something to substitute.
        Assert.True(paths.ContainsKey("/api/tables/tbl-42/keys/{keyId}"));
    }

    [Fact]
    public void DropsTheEntityPathParameterButKeepsTheOthers()
    {
        var paths = BuildTableDocument()["paths"]!.AsObject();

        // Sole path parameter removed => the parameters array goes away entirely.
        Assert.Null(paths["/api/tables/tbl-42"]!["get"]!["parameters"]);

        var remaining = paths["/api/tables/tbl-42/keys/{keyId}"]!["delete"]!["parameters"]!.AsArray();
        Assert.Equal("keyId", (string?)Assert.Single(remaining)!["name"]);
    }

    [Fact]
    public void RetitlesAndRetagsSoTheReferenceReadsAsOneEntity()
    {
        var doc = BuildTableDocument();

        Assert.Equal("StreamsForge table \"positions\"", (string?)doc["info"]!["title"]);
        Assert.Equal("one table", (string?)doc["info"]!["description"]);
        Assert.Equal("1.0.0", (string?)doc["info"]!["version"]);

        var paths = doc["paths"]!.AsObject();
        Assert.Equal("positions", (string?)paths["/api/tables/tbl-42"]!["get"]!["tags"]!.AsArray()[0]);
        Assert.Equal("auth", (string?)paths["/api/auth/login"]!["post"]!["tags"]!.AsArray()[0]);
        Assert.Equal(["positions", "auth"], doc["tags"]!.AsArray().Select(t => (string?)t!["name"]).ToList());
    }

    [Fact]
    public void CarriesTheBearerSecuritySchemeThroughUntouched()
    {
        var doc = BuildTableDocument();

        Assert.Equal("bearer", (string?)doc["components"]!["securitySchemes"]!["Bearer"]!["scheme"]);
        Assert.NotNull(doc["security"]);
        Assert.Equal("3.1.1", (string?)doc["openapi"]);
    }

    [Fact]
    public void PrunesSchemasUnreachableFromTheSurvivingPaths()
    {
        var schemas = BuildTableDocument()["components"]!["schemas"]!.AsObject()
            .Select(s => s.Key).Order().ToList();

        // RowDto survives only because RowsResponse (referenced by a kept path) points at it.
        Assert.Equal(["HistoryLookupRequest", "LoginRequest", "RowDto", "RowsResponse"], schemas);
        Assert.DoesNotContain("IngestRequest", schemas);
        Assert.DoesNotContain("UnrelatedDto", schemas);
    }

    [Fact]
    public void PointsRowShapedPayloadsAtTheEntitysOwnSchema()
    {
        var rowSchema = EntityOpenApiLogic.RowSchemaFromFields(
            [new FieldDef("symbol", FieldType.String), new FieldDef("qty", FieldType.Long)]);
        var schemas = BuildTableDocument(rowSchema, "positions_row")["components"]!["schemas"]!.AsObject();

        Assert.Equal("#/components/schemas/positions_row", (string?)schemas["RowDto"]!["properties"]!["row"]!["$ref"]);
        Assert.Equal(
            "#/components/schemas/positions_row",
            (string?)schemas["HistoryLookupRequest"]!["properties"]!["row"]!["$ref"]);
        // Untouched neighbours stay themselves.
        Assert.Equal("int64", (string?)schemas["RowDto"]!["properties"]!["weight"]!["format"]);
        Assert.Equal("string", (string?)schemas["LoginRequest"]!["properties"]!["username"]!["type"]);
        Assert.Equal("string", (string?)schemas["positions_row"]!["properties"]!["symbol"]!["type"]);
    }

    [Fact]
    public void TypesTheItemsOfAnEventsArray()
    {
        // A source has no REST rows endpoint — its typed shape lands on the ingest push body instead.
        var withIngest = AppDocument();
        withIngest["paths"]!["/api/tables/{id}/events"] = JsonNode.Parse(
            """
            { "post": { "requestBody": { "content": { "application/json": {
              "schema": { "$ref": "#/components/schemas/IngestRequest" } } } }, "responses": { "202": {} } } }
            """);

        var built = EntityOpenApiLogic.BuildEntityDocument(
            withIngest, "/api/tables", "id", "tbl-42", "t", "d", "tag",
            EntityOpenApiLogic.RowSchemaFromFields([new FieldDef("symbol", FieldType.String)]),
            "trades_row");

        Assert.Equal(
            "#/components/schemas/trades_row",
            (string?)built["components"]!["schemas"]!["IngestRequest"]!["properties"]!["events"]!["items"]!["$ref"]);
    }

    [Fact]
    public void LeavesTheApplicationDocumentUntouched()
    {
        var app = AppDocument();
        var before = app.ToJsonString();

        EntityOpenApiLogic.BuildEntityDocument(
            app, "/api/tables", "id", "tbl-42", "t", "d", "tag",
            EntityOpenApiLogic.RowSchemaFromFields([new FieldDef("symbol", FieldType.String)]), "r");

        Assert.Equal(before, app.ToJsonString());
    }

    [Fact]
    public void RowSchemaMapsEveryFieldTypeTheWayTheProtoDownloadDoes()
    {
        var schema = EntityOpenApiLogic.RowSchemaFromFields(
        [
            new FieldDef("s", FieldType.String),
            new FieldDef("d", FieldType.Double),
            new FieldDef("l", FieldType.Long),
            new FieldDef("b", FieldType.Bool),
            new FieldDef("ts", FieldType.Timestamp),
            new FieldDef("blob", FieldType.Json),
        ], "rows of t")!;

        var p = schema["properties"]!;
        Assert.Equal("string", (string?)p["s"]!["type"]);
        Assert.Equal(("number", "double"), ((string?)p["d"]!["type"], (string?)p["d"]!["format"]));
        Assert.Equal(("integer", "int64"), ((string?)p["l"]!["type"], (string?)p["l"]!["format"]));
        Assert.Equal("boolean", (string?)p["b"]!["type"]);
        // Timestamps are epoch millis on the wire, exactly as DescriptorFactory types them int64.
        Assert.Equal(("integer", "int64"), ((string?)p["ts"]!["type"], (string?)p["ts"]!["format"]));
        Assert.Equal("epoch milliseconds", (string?)p["ts"]!["description"]);
        // Schemaless JSON: no claim at all beats a wrong one.
        Assert.Empty(p["blob"]!.AsObject());
        Assert.Equal("rows of t", (string?)schema["description"]);
    }

    [Fact]
    public void RowSchemaDescribesArraysAndNestedRecords()
    {
        var schema = EntityOpenApiLogic.RowSchemaFromFields(
        [
            new FieldDef("tags", FieldType.String, IsArray: true),
            new FieldDef("legs", FieldType.Json, [new FieldDef("rate", FieldType.Double)], IsArray: true),
            new FieldDef("meta", FieldType.Json, [new FieldDef("venue", FieldType.String)]),
        ])!;

        var p = schema["properties"]!;
        Assert.Equal("array", (string?)p["tags"]!["type"]);
        Assert.Equal("string", (string?)p["tags"]!["items"]!["type"]);

        Assert.Equal("array", (string?)p["legs"]!["type"]);
        Assert.Equal("number", (string?)p["legs"]!["items"]!["properties"]!["rate"]!["type"]);

        Assert.Equal("object", (string?)p["meta"]!["type"]);
        Assert.Equal("string", (string?)p["meta"]!["properties"]!["venue"]!["type"]);
    }

    [Fact]
    public void RowSchemaIsNullWhenTheEntityHasNoCompiledSchema()
    {
        // A table whose SQL has never compiled: serve the document, just without the typed rows.
        Assert.Null(EntityOpenApiLogic.RowSchemaFromFields([]));
    }
}

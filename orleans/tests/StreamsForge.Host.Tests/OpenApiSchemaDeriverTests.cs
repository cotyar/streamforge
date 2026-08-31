using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors.OpenApi;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Plan 006 W2-2C: pure OpenAPI v3 → <see cref="FieldDef"/> schema derivation
/// (<see cref="OpenApiSchemaDeriver"/>). A single petstore-flavored fixture (JSON and an equivalent
/// YAML transcription) covers operationId selection + response preference, SchemaPointer selection,
/// every scalar mapping, nested objects, arrays of scalars/objects, internal $ref chains, a $ref
/// cycle, allOf merge, oneOf, an external $ref, and an array-root response. Field-tree assertions use
/// <see cref="Dump(System.Collections.Generic.IEnumerable{FieldDef})"/> rather than record equality,
/// because <see cref="FieldDef"/>'s auto-generated record Equals compares <c>Children</c>
/// (<c>List&lt;FieldDef&gt;</c>) by reference (List&lt;T&gt; never overrides Equals) — two
/// independently-built trees with identical shape would otherwise compare unequal.</summary>
public class OpenApiSchemaDeriverTests
{
    // ---- Fixture: JSON ----

    private const string PetstoreJson = """
    {
      "openapi": "3.0.0",
      "info": { "title": "Petstore", "version": "1.0.0" },
      "paths": {
        "/pets": {
          "get": {
            "operationId": "listPets",
            "responses": {
              "200": { "content": { "application/json": { "schema": { "type": "array", "items": { "$ref": "#/components/schemas/Pet" } } } } },
              "201": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Tag" } } } },
              "default": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
            }
          }
        },
        "/pets/stream": {
          "get": {
            "operationId": "streamPets",
            "responses": {
              "2XX": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Tag" } } } },
              "default": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
            }
          }
        },
        "/pets/alias": {
          "get": {
            "operationId": "getPetAlias",
            "responses": {
              "200": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/PetAlias" } } } }
            }
          }
        },
        "/category": {
          "get": {
            "operationId": "getCategory",
            "responses": {
              "200": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Category" } } } }
            }
          }
        },
        "/extended": {
          "get": {
            "operationId": "getExtendedPet",
            "responses": {
              "200": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ExtendedPet" } } } }
            }
          }
        },
        "/pet-or-error": {
          "get": {
            "operationId": "getPetOrError",
            "responses": {
              "200": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/PetOrError" } } } }
            }
          }
        },
        "/linked": {
          "get": {
            "operationId": "getLinked",
            "responses": {
              "200": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Linked" } } } }
            }
          }
        }
      },
      "components": {
        "schemas": {
          "Tag": {
            "type": "object",
            "properties": { "id": { "type": "integer" }, "name": { "type": "string" } }
          },
          "Category": {
            "type": "object",
            "properties": {
              "id": { "type": "integer" },
              "name": { "type": "string" },
              "parent": { "$ref": "#/components/schemas/Category" }
            }
          },
          "Error": {
            "type": "object",
            "properties": { "message": { "type": "string" } }
          },
          "Pet": {
            "type": "object",
            "properties": {
              "id": { "type": "integer", "format": "int64" },
              "name": { "type": "string" },
              "category": { "$ref": "#/components/schemas/Category" },
              "tags": { "type": "array", "items": { "$ref": "#/components/schemas/Tag" } },
              "photoUrls": { "type": "array", "items": { "type": "string" } },
              "status": { "type": "string" },
              "createdAt": { "type": "string", "format": "date-time" },
              "bornOn": { "type": "string", "format": "date" },
              "weight": { "type": "number" },
              "vaccinated": { "type": "boolean" }
            }
          },
          "PetRef": { "$ref": "#/components/schemas/Pet" },
          "PetAlias": { "$ref": "#/components/schemas/PetRef" },
          "ExtendedPet": {
            "allOf": [
              { "$ref": "#/components/schemas/Pet" },
              { "type": "object", "properties": { "name": { "type": "integer" }, "nickname": { "type": "string" } } }
            ]
          },
          "PetOrError": {
            "oneOf": [
              { "$ref": "#/components/schemas/Pet" },
              { "$ref": "#/components/schemas/Category" }
            ]
          },
          "Linked": {
            "type": "object",
            "properties": {
              "external": { "$ref": "external.yaml#/components/schemas/Foo" }
            }
          },
          "OneOfHolder": {
            "type": "object",
            "properties": {
              "value": { "oneOf": [ { "type": "string" }, { "type": "integer" } ] },
              "plain": { "type": "string" }
            }
          }
        }
      }
    }
    """;

    // ---- Fixture: YAML (same document, transcribed by hand — proves JSON/YAML equivalence) ----

    private const string PetstoreYaml = """
    openapi: "3.0.0"
    info:
      title: Petstore
      version: "1.0.0"
    paths:
      /pets:
        get:
          operationId: listPets
          responses:
            "200":
              content:
                application/json:
                  schema:
                    type: array
                    items:
                      "$ref": "#/components/schemas/Pet"
            "201":
              content:
                application/json:
                  schema:
                    "$ref": "#/components/schemas/Tag"
            default:
              content:
                application/json:
                  schema:
                    "$ref": "#/components/schemas/Error"
      /pets/stream:
        get:
          operationId: streamPets
          responses:
            "2XX":
              content:
                application/json:
                  schema:
                    "$ref": "#/components/schemas/Tag"
            default:
              content:
                application/json:
                  schema:
                    "$ref": "#/components/schemas/Error"
      /pets/alias:
        get:
          operationId: getPetAlias
          responses:
            "200":
              content:
                application/json:
                  schema:
                    "$ref": "#/components/schemas/PetAlias"
      /category:
        get:
          operationId: getCategory
          responses:
            "200":
              content:
                application/json:
                  schema:
                    "$ref": "#/components/schemas/Category"
      /extended:
        get:
          operationId: getExtendedPet
          responses:
            "200":
              content:
                application/json:
                  schema:
                    "$ref": "#/components/schemas/ExtendedPet"
      /pet-or-error:
        get:
          operationId: getPetOrError
          responses:
            "200":
              content:
                application/json:
                  schema:
                    "$ref": "#/components/schemas/PetOrError"
      /linked:
        get:
          operationId: getLinked
          responses:
            "200":
              content:
                application/json:
                  schema:
                    "$ref": "#/components/schemas/Linked"
    components:
      schemas:
        Tag:
          type: object
          properties:
            id:
              type: integer
            name:
              type: string
        Category:
          type: object
          properties:
            id:
              type: integer
            name:
              type: string
            parent:
              "$ref": "#/components/schemas/Category"
        Error:
          type: object
          properties:
            message:
              type: string
        Pet:
          type: object
          properties:
            id:
              type: integer
              format: int64
            name:
              type: string
            category:
              "$ref": "#/components/schemas/Category"
            tags:
              type: array
              items:
                "$ref": "#/components/schemas/Tag"
            photoUrls:
              type: array
              items:
                type: string
            status:
              type: string
            createdAt:
              type: string
              format: date-time
            bornOn:
              type: string
              format: date
            weight:
              type: number
            vaccinated:
              type: boolean
        PetRef:
          "$ref": "#/components/schemas/Pet"
        PetAlias:
          "$ref": "#/components/schemas/PetRef"
        ExtendedPet:
          allOf:
            - "$ref": "#/components/schemas/Pet"
            - type: object
              properties:
                name:
                  type: integer
                nickname:
                  type: string
        PetOrError:
          oneOf:
            - "$ref": "#/components/schemas/Pet"
            - "$ref": "#/components/schemas/Category"
        Linked:
          type: object
          properties:
            external:
              "$ref": "external.yaml#/components/schemas/Foo"
        OneOfHolder:
          type: object
          properties:
            value:
              oneOf:
                - type: string
                - type: integer
            plain:
              type: string
    """;

    private const string ExpectedPetDump =
        "id:Long;name:String;category:Json{id:Long;name:String;parent:Json};" +
        "tags:Json[]{id:Long;name:String};photoUrls:String[];status:String;" +
        "createdAt:Timestamp;bornOn:Timestamp;weight:Double;vaccinated:Bool";

    private static string Dump(FieldDef field)
    {
        var array = field.IsArray ? "[]" : "";
        var children = field.Children is { Count: > 0 } ? "{" + Dump(field.Children) + "}" : "";
        return $"{field.Name}:{field.Type}{array}{children}";
    }

    private static string Dump(IEnumerable<FieldDef> fields) => string.Join(";", fields.Select(Dump));

    private static void AssertPetShapeWithCycleDiagnostic(SchemaDeriveResult result)
    {
        Assert.Equal(ExpectedPetDump, Dump(result.Fields));
        Assert.Contains(result.Diagnostics, d =>
            d.Contains("parent", StringComparison.Ordinal) && d.Contains("cycle", StringComparison.OrdinalIgnoreCase));
    }

    // ---- SchemaPointer selection + every scalar mapping + nested objects + arrays + $ref + cycle ----

    [Theory]
    [InlineData(PetstoreJsonConstName)]
    [InlineData(PetstoreYamlConstName)]
    public void SchemaPointer_selects_Pet_directly_with_full_type_map_nested_objects_and_arrays(string which)
    {
        var doc = Doc(which);
        var result = OpenApiSchemaDeriver.Derive(doc, new OpenApiRef { SchemaPointer = "#/components/schemas/Pet" });

        AssertPetShapeWithCycleDiagnostic(result);
    }

    // xunit InlineData requires compile-time constants; route through named constants instead of the
    // (much larger) doc text itself.
    private const string PetstoreJsonConstName = "json";
    private const string PetstoreYamlConstName = "yaml";

    private static string Doc(string which) => which == PetstoreJsonConstName ? PetstoreJson : PetstoreYaml;

    [Fact]
    public void SchemaPointer_wins_over_OperationId_when_both_are_set()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef
        {
            OperationId = "listPets", // would select an array-of-Pet root if honored
            SchemaPointer = "#/components/schemas/Tag",
        });

        Assert.Equal("id:Long;name:String", Dump(result.Fields));
        Assert.Empty(result.Diagnostics);
    }

    // ---- operationId selection + response-preference order (200 > 201 > 2XX > default) ----

    [Fact]
    public void OperationId_prefers_200_over_201_and_default_and_unwraps_an_array_root()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef { OperationId = "listPets" });

        AssertPetShapeWithCycleDiagnostic(result);
        Assert.Contains(result.Diagnostics, d => d.Contains("array", StringComparison.OrdinalIgnoreCase) && d.Contains("items list", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void OperationId_prefers_200_over_201_and_default_and_unwraps_an_array_root_Yaml()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreYaml, new OpenApiRef { OperationId = "listPets" });

        AssertPetShapeWithCycleDiagnostic(result);
    }

    [Fact]
    public void OperationId_prefers_2XX_over_default_when_200_and_201_are_absent()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef { OperationId = "streamPets" });

        Assert.Equal("id:Long;name:String", Dump(result.Fields)); // Tag, not Error
        Assert.Empty(result.Diagnostics);
    }

    [Fact]
    public void Missing_operationId_yields_empty_fields_and_one_clear_diagnostic()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef { OperationId = "doesNotExist" });

        Assert.Empty(result.Fields);
        Assert.Single(result.Diagnostics);
        Assert.Contains("doesNotExist", result.Diagnostics[0], StringComparison.Ordinal);
    }

    // ---- internal $ref chains (2 hops) ----

    [Fact]
    public void Internal_ref_chain_of_two_hops_resolves_to_the_terminal_schema()
    {
        // PetAlias -> PetRef -> Pet
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef { OperationId = "getPetAlias" });

        AssertPetShapeWithCycleDiagnostic(result);
    }

    // ---- $ref cycle (dedicated, single-hop self-reference) ----

    [Fact]
    public void Self_referential_ref_cycle_becomes_schemaless_Json_with_a_diagnostic()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef { OperationId = "getCategory" });

        Assert.Equal("id:Long;name:String;parent:Json", Dump(result.Fields));
        Assert.Contains(result.Diagnostics, d =>
            d.Contains("parent", StringComparison.Ordinal) && d.Contains("cycle", StringComparison.OrdinalIgnoreCase));
    }

    // ---- allOf: shallow property merge, later-wins override + diagnostic ----

    [Fact]
    public void AllOf_merges_properties_shallowly_with_later_entries_overriding_earlier_ones()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef { OperationId = "getExtendedPet" });

        var expected =
            "id:Long;name:Long;category:Json{id:Long;name:String;parent:Json};" + // name overridden String -> Long by the 2nd allOf entry
            "tags:Json[]{id:Long;name:String};photoUrls:String[];status:String;" +
            "createdAt:Timestamp;bornOn:Timestamp;weight:Double;vaccinated:Bool;nickname:String";
        Assert.Equal(expected, Dump(result.Fields));
        Assert.Contains(result.Diagnostics, d => d.Contains("allOf", StringComparison.Ordinal) && d.Contains("merged", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void AllOf_merges_properties_shallowly_with_later_entries_overriding_earlier_ones_Yaml()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreYaml, new OpenApiRef { OperationId = "getExtendedPet" });

        Assert.Equal(
            "id:Long;name:Long;category:Json{id:Long;name:String;parent:Json};" +
            "tags:Json[]{id:Long;name:String};photoUrls:String[];status:String;" +
            "createdAt:Timestamp;bornOn:Timestamp;weight:Double;vaccinated:Bool;nickname:String",
            Dump(result.Fields));
    }

    // ---- oneOf → schemaless + diagnostic (here: at the root, so "schemaless" means no fields) ----

    [Fact]
    public void OneOf_is_unsupported_and_yields_empty_fields_with_a_diagnostic()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef { OperationId = "getPetOrError" });

        Assert.Empty(result.Fields);
        Assert.Contains(result.Diagnostics, d => d.Contains("oneOf", StringComparison.Ordinal));
    }

    [Fact]
    public void OneOf_nested_inside_a_property_becomes_a_schemaless_Json_field_with_a_diagnostic()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef { SchemaPointer = "#/components/schemas/OneOfHolder" });

        Assert.Equal("value:Json;plain:String", Dump(result.Fields));
        Assert.Contains(result.Diagnostics, d => d.Contains("value", StringComparison.Ordinal) && d.Contains("oneOf", StringComparison.Ordinal));
    }

    // ---- external $ref → diagnostic, schemaless field ----

    [Fact]
    public void External_ref_is_rejected_with_a_diagnostic_and_the_field_is_schemaless_Json()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef { OperationId = "getLinked" });

        Assert.Equal("external:Json", Dump(result.Fields));
        Assert.Contains(result.Diagnostics, d =>
            d.Contains("external.yaml", StringComparison.Ordinal) && d.Contains("external", StringComparison.OrdinalIgnoreCase));
    }

    // ---- array-root response (array of scalars covered via Pet.photoUrls above; this is array-of-objects at the root) ----

    [Fact]
    public void Array_root_response_derives_fields_from_its_item_schema_with_a_diagnostic()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef { OperationId = "listPets" });

        Assert.Contains(result.Diagnostics, d => d.Contains("array", StringComparison.OrdinalIgnoreCase) && d.Contains("items list", StringComparison.OrdinalIgnoreCase));
        AssertPetShapeWithCycleDiagnostic(result);
    }

    // ---- unresolved pointer / bad reference input ----

    [Fact]
    public void Unresolvable_SchemaPointer_yields_empty_fields_and_a_diagnostic()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef { SchemaPointer = "#/components/schemas/DoesNotExist" });

        Assert.Empty(result.Fields);
        Assert.Single(result.Diagnostics);
        Assert.Contains("DoesNotExist", result.Diagnostics[0], StringComparison.Ordinal);
    }

    [Fact]
    public void Reference_with_neither_pointer_nor_operationId_yields_empty_fields_and_a_diagnostic()
    {
        var result = OpenApiSchemaDeriver.Derive(PetstoreJson, new OpenApiRef());

        Assert.Empty(result.Fields);
        Assert.Single(result.Diagnostics);
    }

    // ---- never throws for bad documents; FormatException only for null/empty docText ----

    [Fact]
    public void Malformed_document_never_throws_diagnostics_carry_the_error()
    {
        var result = OpenApiSchemaDeriver.Derive("not json and not : valid: yaml: : :", new OpenApiRef { OperationId = "x" });

        Assert.Empty(result.Fields);
        Assert.NotEmpty(result.Diagnostics);
    }

    [Fact]
    public void Empty_docText_throws_FormatException()
    {
        Assert.Throws<FormatException>(() => OpenApiSchemaDeriver.Derive("", new OpenApiRef { OperationId = "x" }));
    }

    [Fact]
    public void Null_docText_throws_FormatException()
    {
        Assert.Throws<FormatException>(() => OpenApiSchemaDeriver.Derive(null!, new OpenApiRef { OperationId = "x" }));
    }
}

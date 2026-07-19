namespace StreamForge.Abstractions;

// Plan 006 — connector configuration + runtime-status contracts. Frozen like everything else in
// this assembly: additive evolution only, next free [Id], set-accessors (ORLEANS0101 forbids init
// under cross-assembly codegen). Secret fields (URL header values, gRPC password/token) follow
// D-H secrets-lite: masked as SecretMask in every read path; a written SecretMask value means
// "keep the stored value".

/// <summary>Well-known source kinds (string constants, not an enum — additive like GeneratorProfile).</summary>
public static class SourceKinds
{
    public const string Generator = "generator";
    public const string Url = "url";
    public const string File = "file";
    public const string Folder = "folder";
    public const string Grpc = "grpc";

    /// <summary>The masked placeholder for secrets-lite values (D-H).</summary>
    public const string SecretMask = "***";
}

/// <summary>Per-kind connector config container. Exactly one of Url/File/Folder/Grpc is set,
/// matching <see cref="SourceDefinition.Kind"/>. Schedule applies to url/file/folder kinds (grpc
/// is a persistent subscription — its Schedule is ignored). Mapping applies to url/file/folder.</summary>
[GenerateSerializer]
public sealed class ConnectorConfig
{
    [Id(0)] public ScheduleSpec? Schedule { get; set; }
    [Id(1)] public UrlPollConfig? Url { get; set; }
    [Id(2)] public FilePollConfig? File { get; set; }
    [Id(3)] public FolderPollConfig? Folder { get; set; }
    [Id(4)] public GrpcSubConfig? Grpc { get; set; }
    [Id(5)] public MappingSpec? Mapping { get; set; }
}

/// <summary>Cron (5/6-field, UTC, Cronos) XOR fixed interval; IntervalMs floor is 1000 (D-E).</summary>
[GenerateSerializer]
public sealed class ScheduleSpec
{
    [Id(0)] public string? Cron { get; set; }
    [Id(1)] public int? IntervalMs { get; set; }
}

/// <summary>HTTP(S) GET polling. Header VALUES are secrets-lite (D-H).</summary>
[GenerateSerializer]
public sealed class UrlPollConfig
{
    [Id(0)] public string Url { get; set; } = "";
    [Id(1)] public Dictionary<string, string> Headers { get; set; } = [];
    /// <summary>Optional OpenAPI derivation reference (schema was derived from it; kept for re-derive).</summary>
    [Id(2)] public OpenApiRef? OpenApi { get; set; }
}

/// <summary>Where an OpenAPI-derived schema came from (D-F).</summary>
[GenerateSerializer]
public sealed class OpenApiRef
{
    /// <summary>URL of the OpenAPI v3 document (JSON or YAML). Mutually exclusive with DocInline.</summary>
    [Id(0)] public string? DocUrl { get; set; }
    /// <summary>The document text itself, when supplied inline instead of by URL.</summary>
    [Id(1)] public string? DocInline { get; set; }
    /// <summary>operationId in the doc; response defaults to 200 / first application/json media type.</summary>
    [Id(2)] public string? OperationId { get; set; }
    /// <summary>Explicit JSON pointer to a schema (e.g. "#/components/schemas/Trade"); overrides OperationId.</summary>
    [Id(3)] public string? SchemaPointer { get; set; }
}

/// <summary>Formats for file/folder sources.</summary>
public static class FileFormats
{
    public const string Ndjson = "ndjson";
    public const string JsonArray = "json";
    public const string Csv = "csv";
}

/// <summary>Poll one file; re-parse on content change (hash+mtime). No tailing guarantees.</summary>
[GenerateSerializer]
public sealed class FilePollConfig
{
    [Id(0)] public string Path { get; set; } = "";
    /// <summary>"ndjson" | "json" | "csv" (<see cref="FileFormats"/>).</summary>
    [Id(1)] public string Format { get; set; } = FileFormats.Ndjson;
}

/// <summary>Poll a directory; each NEW file (name+mtime ledger) is parsed once and remembered.</summary>
[GenerateSerializer]
public sealed class FolderPollConfig
{
    [Id(0)] public string Path { get; set; } = "";
    [Id(1)] public string Format { get; set; } = FileFormats.Ndjson;
    /// <summary>Optional glob over file NAMES within the folder (no recursion), e.g. "*.json".</summary>
    [Id(2)] public string? Glob { get; set; }
}

/// <summary>Subscription to a remote StreamForge DynamicStreamService (D-G — federation).
/// Password/Token are secrets-lite (D-H).</summary>
[GenerateSerializer]
public sealed class GrpcSubConfig
{
    /// <summary>Target gRPC address, e.g. "http://localhost:5299" (h2c).</summary>
    [Id(0)] public string Address { get; set; } = "";
    /// <summary>"source:{name}" | "pipeline:{id}" | "table:{id}" on the REMOTE instance.</summary>
    [Id(1)] public string EntityKey { get; set; } = "";
    /// <summary>Login for the remote's /api/auth/login (re-login on expiry). XOR Token.</summary>
    [Id(2)] public string? Username { get; set; }
    [Id(3)] public string? Password { get; set; }
    /// <summary>Static bearer token alternative (no re-login possible — documented).</summary>
    [Id(4)] public string? Token { get; set; }
    /// <summary>"reflection" (default) | "proto". Reflection walks the remote's v1alpha service;
    /// "proto" parses <see cref="ProtoText"/> (StreamForge-generated files only).</summary>
    [Id(5)] public string SchemaSource { get; set; } = "reflection";
    /// <summary>Pasted/downloaded proto text when SchemaSource == "proto".</summary>
    [Id(6)] public string? ProtoText { get; set; }
    /// <summary>Remote REST base for login when it differs from the gRPC address,
    /// e.g. "http://localhost:5199".</summary>
    [Id(7)] public string? RestAddress { get; set; }
}

/// <summary>Response-structure mapping (the "mapping document" deserializes into this; JSON or
/// YAML accepted at the API boundary). Paths use the JSONPath-lite subset:
/// $ .name ['name'] [n] [*] — nothing else (documented, closed).</summary>
[GenerateSerializer]
public sealed class MappingSpec
{
    /// <summary>Where the items live, e.g. "$.data.trades[*]". "$" = the root (single item, or
    /// each element when the root is an array).</summary>
    [Id(0)] public string ItemsPath { get; set; } = "$";
    /// <summary>Emitted FIELD name whose value dedups re-polled items. Null = no dedup.</summary>
    [Id(1)] public string? DedupKeyField { get; set; }
    /// <summary>Emitted FIELD name holding the event timestamp (epoch-ms or ISO-8601) → _ts.
    /// Null = arrival time.</summary>
    [Id(2)] public string? TimestampField { get; set; }
    [Id(3)] public List<FieldMapEntry> Fields { get; set; } = [];
}

/// <summary>One output field: where it comes from (path relative to the item) and its FieldDef
/// (name/type/children/isArray — the existing schema model, reused).</summary>
[GenerateSerializer]
public sealed class FieldMapEntry
{
    /// <summary>JSONPath-lite relative to the item, e.g. "price" or "user.tier". Null = same as Field.Name.</summary>
    [Id(0)] public string? SourcePath { get; set; }
    [Id(1)] public FieldDef Field { get; set; } = new("", FieldType.String);
}

/// <summary>Connector runtime status (D-C). Returned by IConnectorStatusFacade; null for
/// generator-kind sources. LastStatus: "never" | "ok" | "error".</summary>
[GenerateSerializer]
public sealed class ConnectorRuntimeStatus
{
    [Id(0)] public string SourceName { get; set; } = "";
    [Id(1)] public long? NextRunMs { get; set; }
    [Id(2)] public long? LastRunMs { get; set; }
    [Id(3)] public string LastStatus { get; set; } = "never";
    [Id(4)] public string? LastError { get; set; }
    [Id(5)] public int ConsecutiveFailures { get; set; }
    [Id(6)] public long EventsEmittedTotal { get; set; }
    [Id(7)] public int LastBatchCount { get; set; }
}

// ---- REST helper DTOs (cross HTTP only, but follow house serialization style anyway) ----

/// <summary>POST /api/sources/schema/mapping-validate request.</summary>
[GenerateSerializer]
public sealed class MappingValidateRequest
{
    /// <summary>Mapping document text (JSON or YAML).</summary>
    [Id(0)] public string Document { get; set; } = "";
    /// <summary>Optional sample response body to dry-run the mapping against.</summary>
    [Id(1)] public string? Sample { get; set; }
}

[GenerateSerializer]
public sealed class MappingValidateResult
{
    [Id(0)] public bool Ok { get; set; }
    [Id(1)] public MappingSpec? Mapping { get; set; }
    [Id(2)] public List<string> Diagnostics { get; set; } = [];
    /// <summary>Rows extracted from Sample (first 10), for UI preview.</summary>
    [Id(3)] public List<Dictionary<string, object?>> PreviewRows { get; set; } = [];
}

/// <summary>POST /api/sources/schema/derive-openapi request.</summary>
[GenerateSerializer]
public sealed class SchemaDeriveRequest
{
    [Id(0)] public OpenApiRef OpenApi { get; set; } = new();
}

[GenerateSerializer]
public sealed class SchemaDeriveResult
{
    [Id(0)] public List<FieldDef> Fields { get; set; } = [];
    [Id(1)] public List<string> Diagnostics { get; set; } = [];
}

/// <summary>POST /api/sources/schema/from-remote request.</summary>
[GenerateSerializer]
public sealed class RemoteSchemaRequest
{
    [Id(0)] public GrpcSubConfig Grpc { get; set; } = new();
}

[GenerateSerializer]
public sealed class RemoteSchemaResult
{
    [Id(0)] public List<FieldDef> Fields { get; set; } = [];
    /// <summary>FieldNumberMap JSON (EntitySchemas.ParseMap format) captured from the remote.</summary>
    [Id(1)] public string FieldNumbersJson { get; set; } = "";
    [Id(2)] public List<string> Diagnostics { get; set; } = [];
}

/// <summary>One entity's outcome in a config import (D-J).</summary>
[GenerateSerializer]
public sealed class ConfigImportReportEntry
{
    /// <summary>"source" | "pipeline" | "table".</summary>
    [Id(0)] public string Kind { get; set; } = "";
    [Id(1)] public string Name { get; set; } = "";
    /// <summary>"created" | "updated" | "deleted" | "skipped" | "error".</summary>
    [Id(2)] public string Action { get; set; } = "";
    [Id(3)] public List<string> Diagnostics { get; set; } = [];
}

/// <summary>POST /api/config/import response (D-J).</summary>
[GenerateSerializer]
public sealed class ConfigImportReport
{
    /// <summary>"validate" | "merge" | "replace".</summary>
    [Id(0)] public string Mode { get; set; } = "";
    [Id(1)] public List<ConfigImportReportEntry> Entries { get; set; } = [];
    [Id(2)] public bool Ok { get; set; }
}

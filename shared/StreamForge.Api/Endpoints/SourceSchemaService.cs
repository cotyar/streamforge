using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Grpc;
using StreamForge.AppCore.Connectors.Mapping;
using StreamForge.AppCore.Connectors.OpenApi;
using StreamForge.AppCore.Connectors.Scheduling;
using StreamForge.AppCore.Ingest;

namespace StreamForge.Api;

/// <summary>
/// Plan 006 (W4): kind-aware source validation for POST/PUT /api/sources — deliberately a sibling
/// file to SourcesEndpoints.cs (not inlined in the handlers) so the rule evaluation stays
/// unit-testable without an HTTP-level test harness (this repo doesn't have one; see
/// orleans/tests/StreamForge.Host.Tests/SourcesEndpointsLogicTests.cs) — mirrors the
/// ConfigEndpoints/ConfigImportService split from W3C.
/// </summary>
public static class SourceValidation
{
    private static readonly HashSet<string> KnownKinds = new(StringComparer.Ordinal)
    {
        SourceKinds.Generator, SourceKinds.Url, SourceKinds.File, SourceKinds.Folder, SourceKinds.Grpc,
        SourceKinds.Ingest,
    };

    private static readonly HashSet<string> KnownFileFormats = new(StringComparer.Ordinal)
    {
        FileFormats.Ndjson, FileFormats.JsonArray, FileFormats.Csv,
    };

    /// <summary>"source:{id}" | "pipeline:{id}" | "table:{id}" — the shape
    /// <see cref="GrpcSubConfig.EntityKey"/> must match on a grpc-kind source (D-G).</summary>
    public static readonly Regex GrpcEntityKeyPattern = new(@"^(source|pipeline|table):.+$", RegexOptions.Compiled);

    /// <summary>Kind-aware validation for POST/PUT /api/sources (plan 006 W4 item 2). Accumulates
    /// EVERY problem it finds (not just the first) so the 400 response can list them all — the
    /// caller joins them into the single-string <c>ErrorResponse</c> this codebase's endpoints
    /// otherwise use. Never throws.</summary>
    public static List<string> Validate(SourceDefinition def)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(def.Name))
        {
            errors.Add("name is required");
        }

        // Fields non-empty is required for EVERY kind (connectors still need a declared schema —
        // it's what the mapping/OpenAPI-derive flows populate, and what the wire encoder needs).
        if (def.Fields.Count == 0)
        {
            errors.Add("at least one field is required");
        }

        if (!KnownKinds.Contains(def.Kind))
        {
            errors.Add($"kind '{def.Kind}' is not recognized (expected one of: generator, url, file, folder, grpc, ingest)");
            return errors;
        }

        // Plan 008 W4: Ingest is meaningful ONLY on an ingest-kind source — carrying it on any other
        // kind is silent-dead config a client would reasonably expect to do something, so it's an
        // error rather than a no-op (same call SourceValidation already makes for Connector: below).
        if (def.Kind != SourceKinds.Ingest && def.Ingest is not null)
        {
            errors.Add("ingest configuration is only valid for kind 'ingest'");
        }

        if (def.Kind == SourceKinds.Generator)
        {
            // Pre-006 behavior, unchanged: only generator-kind sources need EventsPerSecond —
            // connectors are driven by their own Schedule, not a synthetic emission rate.
            if (def.EventsPerSecond <= 0)
            {
                errors.Add("eventsPerSecond must be > 0");
            }

            return errors;
        }

        if (def.Kind == SourceKinds.Ingest)
        {
            if (def.Ingest is null)
            {
                errors.Add("kind 'ingest' requires an ingest configuration");
            }
            else
            {
                ValidateIngest(def.Ingest, errors);
            }

            return errors;
        }

        var connector = def.Connector;
        if (connector is null)
        {
            errors.Add($"kind '{def.Kind}' requires a connector configuration");
            return errors;
        }

        switch (def.Kind)
        {
            case SourceKinds.Url:
                ValidateUrl(connector, errors);
                break;
            case SourceKinds.File:
                ValidateFile(connector, errors);
                break;
            case SourceKinds.Folder:
                ValidateFolder(connector, errors);
                break;
            case SourceKinds.Grpc:
                ValidateGrpc(connector, errors);
                break;
        }

        // Schedule/Mapping apply to poll-driven kinds only — a grpc source is a persistent
        // subscription; its Schedule (if any) is ignored by the driver (ConnectorConfig doc
        // comment).
        if (def.Kind is SourceKinds.Url or SourceKinds.File or SourceKinds.Folder)
        {
            // An absent Schedule is valid — the driver applies a documented 30 s default; only
            // validate one the caller actually supplied.
            if (connector.Schedule is not null)
            {
                foreach (var e in ScheduleCalc.Validate(connector.Schedule))
                {
                    errors.Add($"connector.schedule: {e}");
                }
            }

            if (connector.Mapping is not null)
            {
                ValidateMapping(connector.Mapping, errors);
            }
        }

        return errors;
    }

    private static void ValidateUrl(ConnectorConfig connector, List<string> errors)
    {
        var url = connector.Url;
        if (url is null)
        {
            errors.Add("kind 'url' requires connector.url");
            return;
        }

        if (string.IsNullOrWhiteSpace(url.Url))
        {
            errors.Add("connector.url.url is required");
        }
        else if (!Uri.TryCreate(url.Url, UriKind.Absolute, out var uri) ||
                 (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
        {
            errors.Add("connector.url.url must be an absolute http(s) URL");
        }
    }

    private static void ValidateFile(ConnectorConfig connector, List<string> errors)
    {
        var file = connector.File;
        if (file is null)
        {
            errors.Add("kind 'file' requires connector.file");
            return;
        }

        if (string.IsNullOrWhiteSpace(file.Path))
        {
            errors.Add("connector.file.path is required");
        }

        if (!KnownFileFormats.Contains(file.Format))
        {
            errors.Add($"connector.file.format '{file.Format}' is not recognized (expected one of: ndjson, json, csv)");
        }
    }

    private static void ValidateFolder(ConnectorConfig connector, List<string> errors)
    {
        var folder = connector.Folder;
        if (folder is null)
        {
            errors.Add("kind 'folder' requires connector.folder");
            return;
        }

        if (string.IsNullOrWhiteSpace(folder.Path))
        {
            errors.Add("connector.folder.path is required");
        }

        if (!KnownFileFormats.Contains(folder.Format))
        {
            errors.Add($"connector.folder.format '{folder.Format}' is not recognized (expected one of: ndjson, json, csv)");
        }
    }

    private static void ValidateGrpc(ConnectorConfig connector, List<string> errors)
    {
        var grpc = connector.Grpc;
        if (grpc is null)
        {
            errors.Add("kind 'grpc' requires connector.grpc");
            return;
        }

        if (string.IsNullOrWhiteSpace(grpc.Address))
        {
            errors.Add("connector.grpc.address is required");
        }

        if (string.IsNullOrWhiteSpace(grpc.EntityKey) || !GrpcEntityKeyPattern.IsMatch(grpc.EntityKey))
        {
            errors.Add("connector.grpc.entityKey must match '(source|pipeline|table):<id>'");
        }

        if (grpc.SchemaSource is not ("reflection" or "proto"))
        {
            errors.Add("connector.grpc.schemaSource must be 'reflection' or 'proto'");
        }
        else if (grpc.SchemaSource == "proto" && string.IsNullOrWhiteSpace(grpc.ProtoText))
        {
            errors.Add("connector.grpc.protoText is required when schemaSource is 'proto'");
        }

        // GrpcSubscriberCore's own doc comment: it will not guess a REST port from the gRPC
        // Address (they need not be related at all) — surface a missing RestAddress here, at
        // config-save time, rather than as a runtime "error" status after the first connect.
        if (!string.IsNullOrEmpty(grpc.Username) && string.IsNullOrEmpty(grpc.RestAddress))
        {
            errors.Add("connector.grpc.restAddress is required when username/password are set (needed to POST /api/auth/login)");
        }
    }

    /// <summary>Plan 008 W4: kind 'ingest' config validation. Capacity/MaxBatchRows just need to be
    /// positive (SourceIngressBuffer trusts them at face value); MaxWaitMs is checked against the same
    /// <see cref="IngressAdmission.MaxBlockWaitMs"/> server cap the buffer itself silently clamps to at
    /// push time — flagging an out-of-range value HERE, at save time, is an honest error instead of a
    /// setting that looks respected but silently isn't.</summary>
    private static void ValidateIngest(IngestConfig ingest, List<string> errors)
    {
        if (ingest.CapacityRows <= 0)
        {
            errors.Add("ingest.capacityRows must be > 0");
        }

        if (ingest.MaxBatchRows <= 0)
        {
            errors.Add("ingest.maxBatchRows must be > 0");
        }

        if (ingest.MaxWaitMs <= 0 || ingest.MaxWaitMs > IngressAdmission.MaxBlockWaitMs)
        {
            errors.Add($"ingest.maxWaitMs must be > 0 and <= {IngressAdmission.MaxBlockWaitMs} (server cap)");
        }
    }

    private static void ValidateMapping(MappingSpec mapping, List<string> errors)
    {
        ValidatePathSyntax(mapping.ItemsPath, "connector.mapping.itemsPath", errors);

        var fieldNames = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < mapping.Fields.Count; i++)
        {
            var entry = mapping.Fields[i];
            var path = entry.SourcePath ?? entry.Field.Name;
            ValidatePathSyntax(path, $"connector.mapping.fields[{i}]", errors);
            if (!string.IsNullOrEmpty(entry.Field.Name))
            {
                fieldNames.Add(entry.Field.Name);
            }
        }

        if (mapping.DedupKeyField is { Length: > 0 } dedup && !fieldNames.Contains(dedup))
        {
            errors.Add($"connector.mapping.dedupKeyField '{dedup}' is not among the mapped fields");
        }

        if (mapping.TimestampField is { Length: > 0 } ts && !fieldNames.Contains(ts))
        {
            errors.Add($"connector.mapping.timestampField '{ts}' is not among the mapped fields");
        }
    }

    /// <summary>Syntax-only JSONPath-lite check (no data to evaluate against at this point) —
    /// mirrors MappingLoader's own (private) path-syntax check; duplicated here on purpose since
    /// this file doesn't own MappingLoader.cs (same "duplicated across an ownership boundary" call
    /// ConfigImportService.cs's BuildSourceSchemas doc comment makes).</summary>
    private static void ValidatePathSyntax(string path, string context, List<string> errors)
    {
        try
        {
            using var dummy = JsonDocument.Parse("{}");
            JsonPathLite.Select(dummy.RootElement, path);
        }
        catch (FormatException ex)
        {
            errors.Add($"{context}: invalid path '{path}': {ex.Message}");
        }
    }
}

/// <summary>Outcome of GET /api/sources/{name}/status (plan 006 W4 item 3), decided as a pure enum
/// separately from its ASP.NET <c>IResult</c> mapping so the 404-vs-204-vs-200 decision itself is
/// directly unit-testable.</summary>
public enum SourceStatusOutcome { NotFound, NoContent, Ok }

/// <summary>The three schema-helper endpoint bodies behind /api/sources/schema/* (plan 006 W4
/// items 4-6), plus the pure status-outcome decision behind item 3. Handlers in SourcesEndpoints.cs
/// stay thin; everything here is either pure or has its I/O isolated to one clearly-marked spot.</summary>
public static class SourceSchemaService
{
    public static SourceStatusOutcome DecideStatusOutcome(bool sourceExists, ConnectorRuntimeStatus? status) =>
        DecideStatusOutcome(sourceExists, status is not null);

    /// <summary>Same NotFound/NoContent/Ok three-way as the <see cref="ConnectorRuntimeStatus"/>
    /// overload above, generalized over "is there a status to show" (plan 008 W4) so GET
    /// /api/sources/{name}/ingest (whose status type is <see cref="IngestStatus"/>, not
    /// ConnectorRuntimeStatus) can reuse the exact same decision without a fake status instance.</summary>
    public static SourceStatusOutcome DecideStatusOutcome(bool sourceExists, bool statusPresent) =>
        !sourceExists ? SourceStatusOutcome.NotFound : !statusPresent ? SourceStatusOutcome.NoContent : SourceStatusOutcome.Ok;

    // ------------------------------------------------------------------
    // POST /api/sources/schema/mapping-validate (pure — no I/O).
    // ------------------------------------------------------------------

    /// <summary>Parses a mapping document (JSON or YAML, via <see cref="MappingLoader"/>) and, when
    /// a Sample is supplied and the spec parsed with zero diagnostics, dry-runs
    /// <see cref="RecordExtractor.Extract"/> against it for up to 10 preview rows. A sample that
    /// isn't valid JSON adds one more diagnostic rather than throwing.</summary>
    public static MappingValidateResult ValidateMappingDocument(MappingValidateRequest request)
    {
        var (spec, parseDiagnostics) = MappingLoader.Parse(request.Document ?? "");
        var diagnostics = new List<string>(parseDiagnostics);
        var previewRows = new List<Dictionary<string, object?>>();

        var specOk = spec is not null && parseDiagnostics.Count == 0;
        if (specOk && !string.IsNullOrWhiteSpace(request.Sample))
        {
            try
            {
                using var sampleDoc = JsonDocument.Parse(request.Sample);
                var rows = RecordExtractor.Extract(sampleDoc.RootElement, spec!, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                previewRows = rows.Take(10).ToList();
            }
            catch (JsonException ex)
            {
                diagnostics.Add($"sample is not valid JSON: {ex.Message}");
            }
        }

        return new MappingValidateResult
        {
            Ok = diagnostics.Count == 0,
            Mapping = spec,
            Diagnostics = diagnostics,
            PreviewRows = previewRows,
        };
    }

    // ------------------------------------------------------------------
    // POST /api/sources/schema/derive-openapi (I/O isolated to FetchOpenApiDocAsync below).
    // ------------------------------------------------------------------

    private static readonly HttpClient DeriveHttpClient = new() { Timeout = TimeSpan.FromSeconds(15) };
    private const long MaxOpenApiDocBytes = 5 * 1024 * 1024;

    /// <summary>DocUrl set -> fetched server-side (15 s timeout, 5 MB cap; non-2xx/oversize/network
    /// failure -> a diagnostic, never a thrown exception) else DocInline is used directly. Either
    /// way, delegates the actual derivation to the pure <see cref="OpenApiSchemaDeriver.Derive"/>.</summary>
    public static async Task<SchemaDeriveResult> DeriveOpenApiAsync(SchemaDeriveRequest request, CancellationToken ct)
    {
        var reference = request.OpenApi;

        string docText;
        if (!string.IsNullOrWhiteSpace(reference.DocUrl))
        {
            var (text, error) = await FetchOpenApiDocAsync(reference.DocUrl, ct).ConfigureAwait(false);
            if (error is not null)
            {
                return new SchemaDeriveResult { Fields = [], Diagnostics = [error] };
            }

            docText = text!;
        }
        else if (!string.IsNullOrWhiteSpace(reference.DocInline))
        {
            docText = reference.DocInline;
        }
        else
        {
            return new SchemaDeriveResult { Fields = [], Diagnostics = ["OpenApi.docUrl or OpenApi.docInline is required"] };
        }

        return OpenApiSchemaDeriver.Derive(docText, reference);
    }

    private static async Task<(string? Text, string? Error)> FetchOpenApiDocAsync(string docUrl, CancellationToken ct)
    {
        try
        {
            using var response = await DeriveHttpClient.GetAsync(docUrl, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                return (null, $"fetching '{docUrl}' returned HTTP {(int)response.StatusCode}");
            }

            await using var stream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            var bytes = await ReadCappedAsync(stream, MaxOpenApiDocBytes, ct).ConfigureAwait(false);
            if (bytes is null)
            {
                return (null, $"document at '{docUrl}' exceeds the 5 MB cap");
            }

            return (Encoding.UTF8.GetString(bytes), null);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or IOException)
        {
            return (null, $"fetching '{docUrl}' failed: {ex.Message}");
        }
    }

    /// <summary>Reads <paramref name="stream"/> fully into memory, aborting (returning null) the
    /// instant more than <paramref name="maxBytes"/> have been read — bounds memory use even
    /// against a server that lies about (or omits) Content-Length.</summary>
    private static async Task<byte[]?> ReadCappedAsync(Stream stream, long maxBytes, CancellationToken ct)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[81920];
        long total = 0;
        int read;
        while ((read = await stream.ReadAsync(chunk, ct).ConfigureAwait(false)) > 0)
        {
            total += read;
            if (total > maxBytes)
            {
                return null;
            }

            buffer.Write(chunk, 0, read);
        }

        return buffer.ToArray();
    }

    // ------------------------------------------------------------------
    // POST /api/sources/schema/from-remote (dials the remote's gRPC surface — Editor only, D-G).
    // ------------------------------------------------------------------

    /// <summary>Reuses <see cref="GrpcSubscriberCore.FetchSchemaOnceAsync"/> (the one-shot,
    /// never-throws schema fetch added alongside the reconnecting subscribe loop) and serializes
    /// the resulting field-number map exactly the way
    /// <c>StreamForge.Host.Grpc.Dynamic.EntitySchemas.ParseMap</c> expects to read it back (plain
    /// <c>JsonSerializer.Serialize</c>, no naming policy — matching RegistryGrain/RegistryActor's
    /// own EnsureFieldNumbersAsync persistence). Network/auth failures surface as
    /// <see cref="RemoteSchemaResult.Diagnostics"/> on a 200, never a 5xx.</summary>
    public static async Task<RemoteSchemaResult> FromRemoteAsync(RemoteSchemaRequest request, CancellationToken ct)
    {
        var (fields, numbers, diagnostics) = await GrpcSubscriberCore.FetchSchemaOnceAsync(request.Grpc, ct).ConfigureAwait(false);
        if (fields is null || numbers is null)
        {
            return new RemoteSchemaResult { Fields = [], FieldNumbersJson = "", Diagnostics = [.. diagnostics] };
        }

        return new RemoteSchemaResult
        {
            Fields = fields,
            FieldNumbersJson = JsonSerializer.Serialize(numbers),
            Diagnostics = [.. diagnostics],
        };
    }
}

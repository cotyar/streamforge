using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Grpc;
using StreamForge.AppCore.Connectors.Mapping;
using StreamForge.AppCore.Connectors.OpenApi;
using StreamForge.AppCore.Connectors.Scheduling;
using StreamForge.AppCore.Ingest;
using StreamForge.AppCore.Transports;

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
    /// <summary>Kinds with a driver of their own. Message-transport kinds are NOT listed here — they come
    /// from <see cref="InboundTransports.Kinds"/> (plan 010), so registering a transport makes its kind
    /// valid, validated and listed in the error message below without editing this file.</summary>
    private static readonly HashSet<string> BuiltInKinds = new(StringComparer.Ordinal)
    {
        SourceKinds.Generator, SourceKinds.Url, SourceKinds.File, SourceKinds.Folder, SourceKinds.Grpc,
        SourceKinds.Ingest,
        // Plan 020 wave B: like Generator and Ingest and unlike everything the registries supply, a CRDT
        // document has a driver of its own rather than a transport (D3), so it is listed here by hand.
        SourceKinds.Crdt,
    };

    private static bool IsKnownKind(string kind) =>
        BuiltInKinds.Contains(kind) || InboundTransports.Find(kind) is not null || PolledTransports.Find(kind) is not null;

    private static readonly HashSet<string> KnownFileFormats = new(StringComparer.Ordinal)
    {
        FileFormats.Ndjson, FileFormats.JsonArray, FileFormats.Csv, FileFormats.Fix,
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

        if (!IsKnownKind(def.Kind))
        {
            // Plan 014: list the polled kinds alongside the built-ins and the message transports, so an
            // operator who typed e.g. 'postgres' on a build without the connector registered is told what
            // IS available rather than just "unknown".
            var known = string.Join(", ", BuiltInKinds.Concat(InboundTransports.Kinds).Concat(PolledTransports.Kinds));
            errors.Add($"kind '{def.Kind}' is not recognized (expected one of: {known})");
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

        if (def.Kind == SourceKinds.Crdt)
        {
            ValidateCrdt(def, errors);
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
            default:
                // Plan 010: every message-transport kind validates itself — the rules for "is this nats
                // config usable" live next to the code that uses it, not in a switch arm here.
                InboundTransports.Find(def.Kind)?.Validate(def, errors);
                // Plan 014: same idea, second registry — a pull-shaped (polled) kind validates itself too.
                // The two registries are disjoint by construction (PolledTransports' own doc comment), so
                // at most one of these two calls does anything for a given def.Kind.
                PolledTransports.Find(def.Kind)?.Validate(def, errors);
                break;
        }

        // Schedule applies to poll-driven kinds only — grpc and the message transports are persistent
        // subscriptions; their Schedule (if any) is ignored by the driver (ConnectorConfig doc comment).
        // Plan 014: "does this kind take a schedule" is a registry lookup rather than a hardcoded kind
        // list for the very reason PolledTransports exists as a second registry instead of an IsPolled
        // flag on the first (see that class's doc) — a kind this assembly has never heard of gets a
        // schedule because it is registered, not because someone remembered to add it to this list.
        if (def.Kind is SourceKinds.Url or SourceKinds.File or SourceKinds.Folder || PolledTransports.Find(def.Kind) is not null)
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
        }

        // Mapping applies to url/file/folder AND every message transport (plan 009 B1 / 010 — a transport's
        // payload goes through the exact same mapping path a polled body does, by construction: that is
        // SubscriberCore's contract); grpc decodes against its own schema and never reads
        // Connector.Mapping at all.
        if ((def.Kind is SourceKinds.Url or SourceKinds.File or SourceKinds.Folder || InboundTransports.Find(def.Kind) is not null)
            && connector.Mapping is not null)
        {
            ValidateMapping(connector.Mapping, errors);
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

        // Plan 012: same vocabulary the file/folder kinds validate against. Empty is accepted as "json"
        // — a definition stored before the field existed deserializes with it unset, and rejecting those
        // on the next PUT would make an additive field a breaking one.
        if (!string.IsNullOrEmpty(url.Format) && !KnownFileFormats.Contains(url.Format))
        {
            errors.Add($"connector.url.format '{url.Format}' is not recognized (expected one of: ndjson, json, csv, fix)");
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
            errors.Add($"connector.file.format '{file.Format}' is not recognized (expected one of: ndjson, json, csv, fix)");
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
            errors.Add($"connector.folder.format '{folder.Format}' is not recognized (expected one of: ndjson, json, csv, fix)");
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

        // Plan 016 wave 5: a named peer (GrpcSubConfig.Peer) resolves BOTH the gRPC address and the REST
        // address at each (re)connect — see that property's own doc comment — so naming one is a
        // legitimate alternative to hardcoding either address, and the two "is required" rules below
        // must not fire against a source that has done exactly that.
        var hasPeer = !string.IsNullOrWhiteSpace(grpc.Peer);

        if (string.IsNullOrWhiteSpace(grpc.Address) && !hasPeer)
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
        if (!string.IsNullOrEmpty(grpc.Username) && string.IsNullOrEmpty(grpc.RestAddress) && !hasPeer)
        {
            errors.Add("connector.grpc.restAddress is required when username/password are set (needed to POST /api/auth/login)");
        }
    }

    // Plan 010: kind 'nats' validation moved to NatsInboundTransport.Validate — see the default arm of the
    // switch above. Every message transport owns its own rules now.

    /// <summary>Plan 008 W4: kind 'ingest' config validation. Capacity/MaxBatchRows just need to be
    /// positive (SourceIngressBuffer trusts them at face value); MaxWaitMs is checked against the same
    /// <see cref="IngressAdmission.MaxBlockWaitMs"/> server cap the buffer itself silently clamps to at
    /// push time — flagging an out-of-range value HERE, at save time, is an honest error instead of a
    /// setting that looks respected but silently isn't.</summary>
    /// <summary>Plan 020 wave B. Two rules, and both exist because the projector refuses to invent
    /// schema: the key column must be a column the source actually declares, and the reserved columns the
    /// platform stamps (<c>_ts</c>, <c>_source</c>, <c>_weight</c>, <c>_op</c>) cannot be borrowed for it.
    /// An ERP document is entirely likely to contain a field literally called <c>_ts</c>; the projector
    /// renames such a document key defensively at projection time, but letting the CONFIG point the key
    /// column at one would corrupt <c>EventRecord.Timestamp</c> on every row, which no rename can
    /// undo.</summary>
    private static void ValidateCrdt(SourceDefinition def, List<string> errors)
    {
        var crdt = def.Connector?.Crdt;
        if (crdt is null)
        {
            errors.Add("kind 'crdt' requires a crdt configuration");
            return;
        }

        if (string.IsNullOrWhiteSpace(crdt.RootMap))
        {
            errors.Add("crdt.rootMap is required");
        }

        if (string.IsNullOrWhiteSpace(crdt.KeyField))
        {
            errors.Add("crdt.keyField is required");
            return;
        }

        if (ReservedRowColumns.Contains(crdt.KeyField))
        {
            errors.Add($"crdt.keyField '{crdt.KeyField}' is a reserved column name");
        }
        else if (!def.Fields.Any(f => string.Equals(f.Name, crdt.KeyField, StringComparison.Ordinal)))
        {
            errors.Add($"crdt.keyField '{crdt.KeyField}' is not one of this source's declared fields");
        }
    }

    /// <summary>The columns the platform stamps onto a row itself. Spelled here rather than referenced
    /// from <c>StreamForge.Connectors.Database.CdcStamp</c> because this assembly does not depend on that
    /// connector project — the two are pinned equal by <c>CrdtProjectorTests</c>.</summary>
    private static readonly HashSet<string> ReservedRowColumns =
        new(StringComparer.Ordinal) { "_ts", "_source", "_weight", "_op", "_retract" };

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

    // ------------------------------------------------------------------
    // POST /api/transports/{kind}/probe (plan 014 — Editor only; dials whatever host the request body
    // names, same trust the url/file/folder source kinds already extend to an Editor).
    // ------------------------------------------------------------------

    /// <summary>Kind-aware, unbounded-timeout-free schema discovery for any registered POLLED transport
    /// that also implements <see cref="ISchemaProbe"/> — the logic half of the probe endpoint, split out
    /// exactly the way <see cref="FromRemoteAsync"/> is, so the unknown-kind / cannot-probe / success /
    /// throws-mid-probe branching is unit-testable without an HTTP-level harness. <paramref name="timeout"/>
    /// is the caller's job to pick (the endpoint reads it from config); this method just enforces it.</summary>
    public static async Task<ProbeOutcome> ProbeAsync(string kind, SourceDefinition def, TimeSpan timeout, CancellationToken ct)
    {
        // BOTH registries, polled first. ISchemaProbe is documented as an optional capability ANY
        // transport may implement, but until this looked in InboundTransports too, only a polled kind
        // could ever be reached through it — a push-shaped kind that implements ISchemaProbe (a broker
        // that can list a topic's fields, a service catalog that describes its own tables) 404'd as
        // "unknown kind" no matter what it implemented. Widening the lookup is the whole fix; everything
        // below is already generic over "an object that might be ISchemaProbe".
        object? transport = PolledTransports.Find(kind) ?? (object?)InboundTransports.Find(kind);
        if (transport is null)
        {
            // Distinct from "cannot probe" below: nobody registered this kind at all — on a build with
            // neither registry populated EVERY kind lands here, and the message says which are known.
            var known = string.Join(", ", PolledTransports.Kinds.Concat(InboundTransports.Kinds));
            return new ProbeOutcome(ProbeOutcomeKind.UnknownKind, null,
                $"kind '{kind}' is not a registered transport (known: {known})");
        }

        if (transport is not ISchemaProbe probe)
        {
            // The kind exists and runs sources today — it has simply never implemented schema discovery.
            return new ProbeOutcome(ProbeOutcomeKind.CannotProbe, null, $"kind '{kind}' does not support schema discovery");
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        try
        {
            var result = await probe.ProbeAsync(def, cts.Token).ConfigureAwait(false);
            return new ProbeOutcome(ProbeOutcomeKind.Ok, result, null);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // The CALLER went away (request aborted), not a probe failure to report — the same "caller is
            // shutting down" carve-out FileSinkClient.PublishAsync/NatsSinkClient.PublishAsync both take.
            throw;
        }
        catch (OperationCanceledException)
        {
            // Our own CancelAfter bound tripped, not the caller's token — a clean diagnostic, not a 500/504.
            // This is the actual timeout enforcement: ISchemaProbe.ProbeAsync dials a caller-supplied host
            // and has no bound of its own, so a stalled/black-holed connector would otherwise park this
            // request thread indefinitely.
            return new ProbeOutcome(ProbeOutcomeKind.Ok,
                new SchemaProbeResult([], [$"probe of kind '{kind}' timed out after {timeout.TotalSeconds:0}s"]), null);
        }
        catch (Exception ex)
        {
            // ISchemaProbe's own contract: throwing means "could not look" and is surfaced verbatim rather
            // than becoming an unhandled-exception 500 an Editor can't act on.
            return new ProbeOutcome(ProbeOutcomeKind.Ok,
                new SchemaProbeResult([], [$"probe of kind '{kind}' failed: {ex.Message}"]), null);
        }
    }
}

/// <summary>Which of three answers <see cref="SourceSchemaService.ProbeAsync"/> gave, so the endpoint maps
/// it to an HTTP status without re-deciding anything: <see cref="UnknownKind"/> (404 — nobody registered
/// this kind) and <see cref="CannotProbe"/> (400 — the kind exists but never implemented
/// <see cref="ISchemaProbe"/>) are deliberately different answers, per plan 014's own instruction that a
/// kind nobody registered and a kind that cannot probe must not collapse into one "no" a caller can't act
/// on. <see cref="Ok"/> covers both a genuinely successful probe AND a probe that threw or timed out — those
/// two land as <see cref="SchemaProbeResult.Diagnostics"/> on a 200, mirroring how every other schema-helper
/// endpoint in this file (<see cref="SourceSchemaService.FromRemoteAsync"/>,
/// <see cref="SourceSchemaService.DeriveOpenApiAsync"/>) already reports "could not look" as a diagnostic
/// rather than a 5xx.</summary>
public enum ProbeOutcomeKind { UnknownKind, CannotProbe, Ok }

/// <summary><see cref="SourceSchemaService.ProbeAsync"/>'s result: <see cref="Message"/> is set for
/// <see cref="ProbeOutcomeKind.UnknownKind"/> and <see cref="ProbeOutcomeKind.CannotProbe"/> (the endpoint's
/// error body); <see cref="Result"/> is set for <see cref="ProbeOutcomeKind.Ok"/> (the endpoint's 200 body).</summary>
public sealed record ProbeOutcome(ProbeOutcomeKind Kind, SchemaProbeResult? Result, string? Message);

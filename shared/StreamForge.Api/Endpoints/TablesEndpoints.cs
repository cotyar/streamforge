using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using StreamForge.AppCore.Sinks;
using StreamForge.AppCore.Sql;
using StreamForge.Engine;
using StreamForge.Host.Grains;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Api;

public static class TablesEndpoints
{
    public static void MapTablesEndpoints(this WebApplication app, StreamForgeApiOptions options)
    {
        var group = app.MapGroup("/api/tables");

        group.MapGet("/", async (ICatalogFacade registry) =>
            Results.Ok((await registry.GetTablesAsync()).Select(SecretsMasker.MaskTable).ToList())
        ).RequireAuthorization("Viewer");

        group.MapGet("/{id}", async (string id, ICatalogFacade registry) =>
        {
            var t = await registry.GetTableAsync(id);
            return t is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskTable(t));
        }).RequireAuthorization("Viewer");

        group.MapPost("/", async (CreateTableRequest req, ClaimsPrincipal principal, ICatalogFacade registry, ILoggerFactory loggers) =>
        {
            if (string.IsNullOrWhiteSpace(req.Name) || string.IsNullOrWhiteSpace(req.Sql))
            {
                return Results.BadRequest(new ErrorResponse("name and sql are required"));
            }

            // Plan 014 K: 'INSERT INTO <sink> SELECT …' is stripped before the definition is built — the
            // stored text is the naked query and the named sink is switched on. See the identical note in
            // PipelinesEndpoints' create handler; an unknown target is a 400, never a silent no-op.
            var sinks = req.Sinks ?? [];
            var sugar = SinkSugar.ApplyTo(req.Sql, sinks, "table");
            if (sugar.Diagnostics.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", sugar.Diagnostics)));
            }

            // Wishlist item 13 gap 3: the missing ISinkTransport.Validate call site — see
            // SinkTransports.Validate's own doc for what this does and does not change. Always run on
            // create: 'sinks' is always the request's own (possibly empty) list here, never a
            // carried-over stored one.
            var sinkErrors = new List<string>();
            SinkTransports.Validate(sinks, sinkErrors);
            if (sinkErrors.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", sinkErrors)));
            }

            try
            {
                var def = new TableDefinition
                {
                    Name = req.Name,
                    Description = req.Description,
                    Sql = sugar.Sql,
                    CreatedBy = principal.Identity?.Name ?? "",
                    SearchEnabled = req.SearchEnabled,
                    SearchMode = req.SearchMode,
                    HistoryEnabled = req.HistoryEnabled,
                    HistoryMode = req.HistoryMode,
                    HistoryLimit = req.HistoryLimit,
                    HistoryByField = req.HistoryByField,
                    HistoryWindowMs = req.HistoryWindowMs,
                    Tags = req.Tags ?? [],
                    Metadata = req.Metadata ?? [],
                    Parallelism = req.Parallelism,
                    Persistence = req.Persistence,
                    FlushMs = req.FlushMs,
                    JournalMaxEntries = req.JournalMaxEntries,
                    RetentionMaxRows = req.RetentionMaxRows,
                    RetentionTtlMs = req.RetentionTtlMs,
                    // Plan 011 D1: null = not sharded, the default.
                    ShardBy = req.ShardBy ?? [],
                    // Plan 009 B2: see the identical note in PipelinesEndpoints' create handler — a
                    // freshly-created table has no stored secrets to merge against.
                    // Plan 014 K: 'sinks' is that same list, after the sugar enabled a member of it.
                    Sinks = sinks,
                };
                var created = await registry.CreateTableAsync(def);
                WarnOnDegradedRowIdentity(loggers, created);
                return Results.Created($"/api/tables/{created.Id}", SecretsMasker.MaskTable(created));
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPut("/{id}", async (string id, CreateTableRequest req, ICatalogFacade registry, ILoggerFactory loggers) =>
        {
            var existing = await registry.GetTableAsync(id);
            if (existing is null)
            {
                return Results.NotFound();
            }

            // Plan 009 B2: null Sinks = unchanged; a non-null Sinks carrying "***" is restored from the
            // stored value first (SecretsMasker.MergeSinkSecrets — see the identical, more detailed note
            // in PipelinesEndpoints' PUT handler, including the Orleans-flavor RegistryGrain gap that
            // applies here too, in TablesEndpoints' case via RegistryGrain.UpdateTableAsync).
            var sinks = req.Sinks is null ? existing.Sinks : SecretsMasker.MergeSinkSecrets(req.Sinks, existing.Sinks);

            // Plan 014 K: after the merge and before the field copies — same ordering, and the same reason
            // for it, as PipelinesEndpoints' PUT handler.
            var sugar = SinkSugar.ApplyTo(req.Sql, sinks, "table");
            if (sugar.Diagnostics.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", sugar.Diagnostics)));
            }

            // Wishlist item 13 gap 3: only when the request is actually touching Sinks (req.Sinks is
            // null means "leave unchanged" — see the comment above) so a PUT that edits some unrelated
            // field on a table whose sinks predate this validation, or were saved back before this rule
            // existed, can never be blocked by a sink spec nobody asked to change.
            if (req.Sinks is not null)
            {
                var sinkErrors = new List<string>();
                SinkTransports.Validate(sinks, sinkErrors);
                if (sinkErrors.Count > 0)
                {
                    return Results.BadRequest(new ErrorResponse(string.Join("; ", sinkErrors)));
                }
            }

            existing.Name = req.Name;
            existing.Description = req.Description;
            existing.Sql = sugar.Sql;
            existing.SearchEnabled = req.SearchEnabled;
            existing.SearchMode = req.SearchMode;
            existing.HistoryEnabled = req.HistoryEnabled;
            existing.HistoryMode = req.HistoryMode;
            existing.HistoryLimit = req.HistoryLimit;
            existing.HistoryByField = req.HistoryByField;
            existing.HistoryWindowMs = req.HistoryWindowMs;
            existing.Tags = req.Tags ?? existing.Tags;
            existing.Metadata = req.Metadata ?? existing.Metadata;
            existing.Parallelism = req.Parallelism;
            existing.Persistence = req.Persistence;
            existing.FlushMs = req.FlushMs;
            existing.JournalMaxEntries = req.JournalMaxEntries;
            existing.RetentionMaxRows = req.RetentionMaxRows;
            existing.RetentionTtlMs = req.RetentionTtlMs;
            // Plan 011 D1: null means "leave as-is" (matching Tags/Metadata/Sinks' convention here), so a
            // client that predates the field cannot silently un-shard a table by omitting it.
            existing.ShardBy = req.ShardBy ?? existing.ShardBy;
            existing.Sinks = sinks;

            try
            {
                var updated = await registry.UpdateTableAsync(existing);
                if (updated is not null)
                {
                    WarnOnDegradedRowIdentity(loggers, updated);
                }
                return updated is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskTable(updated));
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapDelete("/{id}", async (string id, ICatalogFacade registry) =>
        {
            try
            {
                var removed = await registry.DeleteTableAsync(id);
                return removed ? Results.NoContent() : Results.NotFound();
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/start", async (string id, ICatalogFacade registry) =>
        {
            try
            {
                var updated = await registry.SetTableStatusAsync(id, PipelineStatus.Running);
                // Plan 009 B2: see the identical note on PipelinesEndpoints' /start handler — found live.
                return updated is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskTable(updated));
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/stop", async (string id, ICatalogFacade registry) =>
        {
            try
            {
                var updated = await registry.SetTableStatusAsync(id, PipelineStatus.Stopped);
                return updated is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskTable(updated));
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPost("/validate", async (ValidateRequest req, ICatalogFacade registry) =>
        {
            // Plan 014 K: the sugar is stripped before compiling, and the named sink's existence is NOT
            // checked here — see the fuller note on PipelinesEndpoints' /validate for why a ValidateRequest
            // deliberately stays "SQL and nothing else".
            var sugar = SinkSugar.Desugar(req.Sql);
            if (sugar.Diagnostics.Count > 0)
            {
                return Results.Ok(new ValidateTableResponse(
                    false,
                    [.. sugar.Diagnostics.Select(d => new SqlDiagnosticDto(d, 1, 1, nameof(DiagnosticSeverity.Error)))],
                    null,
                    [],
                    [],
                    []));
            }

            var streamSchemas = await BuildStreamSchemasAsync(registry);
            var tableSchemas = await BuildTableSchemasAsync(registry);
            var result = SqlCompiler.CompileTable(sugar.Sql, streamSchemas, tableSchemas);
            return Results.Ok(new ValidateTableResponse(
                result.Ok,
                result.Diagnostics.Select(d => new SqlDiagnosticDto(d.Message, d.Line, d.Column, d.Severity.ToString())).ToList(),
                result.PlanSummary,
                result.StreamInputs.ToList(),
                result.TableInputs.ToList(),
                result.OutputSchema?.Fields.Select(f => new FieldDefDto(f.Key, f.Value.ToString())).ToList() ?? []));
        }).RequireAuthorization("Editor");

        // Plan 008 W5: lineage + execution-plan view for the console's React Flow page. Physical: true
        // only for a Parallelism >= 2 table on the Orleans flavor whose plan shape supports partitioning
        // (see PlanEndpointsLogic's class doc for the full degradation matrix) — otherwise the logical
        // view (planSummary + inputs) with an explanatory UnavailableReason. Recompiled fresh every call
        // (following OrleansArrangementMetaFacade.GetArrangementsAsync's precedent); never persisted.
        group.MapGet("/{id}/plan", async (string id, ICatalogFacade registry) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            var streamSchemas = await BuildStreamSchemasAsync(registry);
            var tableSchemas = await BuildTableSchemasAsync(registry, excludeTableId: def.Id);
            return Results.Ok(PlanEndpointsLogic.BuildTablePlan(def, streamSchemas, tableSchemas, options.Flavor));
        }).RequireAuthorization("Viewer");

        // The .Produces<>() below is what puts this response's shape into the generated OpenAPI document —
        // an IResult-returning handler describes nothing on its own. That in turn is what lets the
        // per-entity document (EntityOpenApiEndpoints) replace the free-form TableRowDto.row with this
        // table's real output schema, which is most of the point of a per-entity spec. Same for /search
        // and for PipelinesEndpoints' /results.
        group.MapGet("/{id}/rows", async (string id, int? limit, int? offset, ICatalogFacade registry, ITableReadFacade tables) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            var rows = await tables.GetRowsAsync(def.Name, limit ?? 100, offset ?? 0);
            var total = await tables.GetRowCountAsync(def.Name);
            var seq = await tables.GetSeqAsync(def.Name);
            var frontierEpoch = await tables.GetSnapshotFrontierEpochAsync(def.Name);
            // Plan 011 D1: on a sharded table, say where these rows came from — and do NOT go and get
            // them from the shards. See TableRowsShardNote's own doc for the trap this closes; the short
            // version is that this endpoint is polled every two seconds by the console, and a keyless
            // listing that fanned out across the directory would wake every shard on every poll.
            // Plan 011 D2: the wording changed with the thing it describes. A sharded table no longer KEEPS
            // a persisted consolidated snapshot (see TableGrain's "sharded tables" paragraph — the shards
            // are the durable per-key copy, and mirroring them in the table grain was a second copy of the
            // same rows). These rows are the table's own LIVE executor output, which is if anything fresher
            // than the up-to-one-flush-interval-stale mirror it replaced. Still no shard is contacted.
            var shardNote = def.ShardBy.Count == 0 ? null : new TableRowsShardNote(
                def.ShardBy,
                ShardsConsulted: false,
                "Served live from the table's own executor; no shard was activated. Use POST /shard/lookup for one key, or GET /shards/scan (optionally ?fenced=true) for an explicit full scan.");
            return Results.Ok(new TableRowsResponse(rows, total, seq, frontierEpoch, shardNote));
        }).Produces<TableRowsResponse>().RequireAuthorization("Viewer");

        // Plan 012: the same rows as a file. Deliberately its OWN route rather than a ?format=csv branch on
        // /rows: that one is .Produces<TableRowsResponse>(), and the per-entity OpenAPI document
        // (EntityOpenApiEndpoints) rewrites its row shape into this table's real output schema — a second
        // content type on the same operation would have made that document describe two things at once.
        group.MapGet("/{id}/rows.csv", async (string id, int? limit, int? offset, ICatalogFacade registry, ITableReadFacade tables) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            // Cap higher than /rows' 100 (this is an export, not a console poll) but still capped: an
            // unbounded default on a million-row table would render the whole thing into one string.
            var rows = await tables.GetRowsAsync(def.Name, Math.Clamp(limit ?? 10_000, 1, 100_000), offset ?? 0);
            var csv = CsvExport.Table(def, rows);
            return Results.File(Encoding.UTF8.GetBytes(csv), "text/csv; charset=utf-8", $"{def.Name}.csv");
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/metrics", async (string id, ICatalogFacade registry, ITableReadFacade tables) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            var metrics = await tables.GetMetricsAsync(def.Name);
            // The row-identity warning (see TableRowIdentityWarning). DERIVED from the definition, not
            // measured by the grain/actor, and stamped here for two reasons: it belongs on the object the
            // console already polls for "this table is not in the state you think it is" conditions
            // (TableMetrics.Rebuilding is the precedent), and doing it in the shared endpoint means BOTH
            // flavors report it from one implementation instead of two that can drift. Pure and cheap — a
            // textual pass over this table's own SQL — and it can never go stale relative to that SQL,
            // which a value persisted at upsert could.
            metrics.RowIdentityWarning = TableRowIdentityWarning.For(def);
            // Plan 014: the successful half of the same derivation, for the console's upsert key-column
            // prefill. Only offered when the extractor actually resolved the declared keys to output
            // columns — where it could not, the warning above is set and a suggestion would be a guess at
            // precisely the thing the operator has to get right.
            var identity = TableGroupKeyExtractor.Describe(def.Sql);
            metrics.DeclaredKeyColumns = identity.Columns is { Count: > 0 } cols ? [.. cols] : [];
            return Results.Ok(metrics);
        }).RequireAuthorization("Viewer");

        // Downloadable, self-contained .proto for this table: DescriptorFactory's schema (built from
        // the already-compiled TableDefinition.OutputFields, no recompilation needed) plus the
        // DynamicStreamService streaming contract. 409 if the table has never successfully compiled
        // (no OutputFields to describe), mirroring the pipeline endpoint's compile-failure behavior.
        group.MapGet("/{id}/proto", async (string id, ICatalogFacade registry) =>
        {
            var def = await registry.GetTableAsync(id)
                ?? (await registry.GetTablesAsync()).FirstOrDefault(t => t.Name == id);
            if (def is null)
            {
                return Results.NotFound();
            }

            if (def.OutputFields.Count == 0)
            {
                return Results.Conflict(new ErrorResponse(
                    def.Error ?? "Table has no compiled output schema yet — start/re-save the table to compile its SQL."));
            }

            var numbersJson = await registry.EnsureFieldNumbersAsync(EntitySchemas.TableKey(def.Id), def.OutputFields);
            var numbers = EntitySchemas.ParseMap(numbersJson);
            var schema = DescriptorFactory.Generate(def.Name, def.OutputFields, numbers);
            var protoText = ProtoFileBuilder.Build("table", def.Name, schema);

            return Results.File(Encoding.UTF8.GetBytes(protoText), "text/plain; charset=utf-8", schema.FileProto.Name);
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/search", async (string id, string? q, int? limit, ICatalogFacade registry, ITableReadFacade tables) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            if (!def.SearchEnabled)
            {
                return Results.BadRequest(new ErrorResponse("Search is not enabled for this table."));
            }

            var query = q ?? "";
            List<TableRowDto> rows = string.IsNullOrWhiteSpace(query)
                ? []
                : await tables.SearchAsync(def.Name, query, limit ?? 100);
            return Results.Ok(new TableSearchResponse(rows, def.SearchMode.ToString(), def.SearchEnabled, rows.Count));
        }).Produces<TableSearchResponse>().RequireAuthorization("Viewer");

        // Row history (Feature B). POST (not GET) because the lookup key is the row's own content — an
        // arbitrary-shaped object, awkward to round-trip through a query string — not a resource
        // identifier; mirrors this API's existing "/validate" precedent for a read-only POST-with-body.
        // The server derives the row-identity key from req.Row via TableGroupKeyExtractor/RowKeyCodec, the
        // same way TableHistoryGrain derives it from live deltas, so the client never needs to know the
        // table's GROUP BY identity columns or the key encoding.
        group.MapPost("/{id}/history/lookup", async (string id, HistoryLookupRequest req, int? limit, ICatalogFacade registry, ITableHistoryFacade history) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            // Plan 011 D1: on a SHARDED table the per-key shards hold the version trails and the
            // table-wide history grain is deliberately disabled, so this endpoint would answer 200 with a
            // convincingly empty result — the worst possible reply, because it looks like "this key has no
            // history" rather than "you are asking the wrong tier". Point the caller at the right one.
            if (def.ShardBy.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(
                    $"This table is sharded by {string.Join(", ", def.ShardBy)}; its row history lives per shard key. Use POST /api/tables/{id}/shard/lookup instead."));
            }

            if (!def.HistoryEnabled)
            {
                return Results.BadRequest(new ErrorResponse("Row history is not enabled for this table."));
            }

            var identityColumns = TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql);
            var key = RowKeyCodec.EncodeIdentity(req.Row, identityColumns);
            var result = await history.GetHistoryAsync(def.Name, key, limit ?? 0);
            return Results.Ok(result);
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/history/stats", async (string id, ICatalogFacade registry, ITableHistoryFacade history) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            // Plan 011 D1: on a SHARDED table the per-key shards hold the version trails and the
            // table-wide history grain is deliberately disabled, so this endpoint would answer 200 with a
            // convincingly empty result — the worst possible reply, because it looks like "this key has no
            // history" rather than "you are asking the wrong tier". Point the caller at the right one.
            if (def.ShardBy.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(
                    $"This table is sharded by {string.Join(", ", def.ShardBy)}; its row history lives per shard key. Use POST /api/tables/{id}/shard/lookup instead."));
            }

            if (!def.HistoryEnabled)
            {
                return Results.BadRequest(new ErrorResponse("Row history is not enabled for this table."));
            }

            return Results.Ok(await history.GetStatsAsync(def.Name));
        }).RequireAuthorization("Viewer");

        // ------------------------------------------------------------------
        // Plan 011 wave D1 — SHARDED TABLES.
        //
        // Three endpoints, and the split between them IS the design. A per-key lookup activates exactly
        // one shard, which is the point. The listing endpoint activates none, which is what makes it
        // pollable. A full scan activates every shard in its page, which is why it is a separate URL that
        // nothing reaches by accident — not a query parameter on one of the other two.
        // ------------------------------------------------------------------

        // "Give me everything for this key" — the query the whole tier exists for. One grain, one ordered
        // delta stream, Orleans serializing its turns: strictly consistent by construction, no fence.
        group.MapPost("/{id}/shard/lookup", async (string id, ShardLookupRequest req, int? historyLimit, ICatalogFacade registry, ITableShardFacade shards) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            if (def.ShardBy.Count == 0)
            {
                return Results.BadRequest(new ErrorResponse("This table is not sharded (shardBy is empty)."));
            }

            var missing = def.ShardBy.Where(c => !req.Row.ContainsKey(c)).ToList();
            if (missing.Count > 0)
            {
                // Refused rather than defaulted to null: a missing shard column would silently address the
                // "all nulls" shard, which exists and would answer, so the caller would get a confident
                // empty result for a typo.
                return Results.BadRequest(new ErrorResponse(
                    $"row is missing shardBy column(s): {string.Join(", ", missing)}. This table shards by {string.Join(", ", def.ShardBy)}."));
            }

            var shardKey = TableShardKeys.EncodeShardKey(req.Row, def.ShardBy);
            return Results.Ok(await shards.GetShardAsync(def.Name, shardKey, historyLimit ?? 0));
        }).RequireAuthorization("Viewer");

        // Shard metrics + the live key set. Wakes NOTHING — router and directory only.
        group.MapGet("/{id}/shards", async (string id, int? limit, int? offset, ICatalogFacade registry, ITableShardFacade shards) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            if (def.ShardBy.Count == 0)
            {
                return Results.Ok(new TableShardsResponse(false, [], 0, 0, 0, 0, -1, 0, 0, false, []));
            }

            var info = await shards.GetInfoAsync(def.Name);
            // limit=0 means "metrics only, list no keys" — the shape a 2s console poll should use, since
            // the key list is O(distinct keys) to serialize and none of the numbers need it. (Note the
            // directory grain's own GetKeysAsync reads limit<=0 as "all", which is right for the delete
            // path and wrong here; the endpoint is where the caller-facing meaning is fixed.)
            var take = Math.Max(0, limit ?? 100);
            var keys = take == 0 ? [] : await shards.GetKeysAsync(def.Name, take, offset ?? 0);
            return Results.Ok(new TableShardsResponse(
                info.Enabled, info.ShardBy, info.ShardCount, info.ResidentShardCount,
                info.Activations, info.Deactivations, info.RouterSeq, info.RoutedBatches, info.RoutedDeltas,
                info.RouterActive, keys));
        }).RequireAuthorization("Viewer");

        // THE EXPLICIT FULL SCAN. This one does activate every shard in its page — that is the whole
        // reason it is its own endpoint rather than a flag, so that no routine poll can reach it.
        //
        // ?fenced=true (plan 011 D2) upgrades it from a set of observations to a genuine CONSISTENT CUT:
        // the router names the sequence it has forwarded up to and reads every shard without letting
        // another batch through, so no delta from at-or-before the fence is missing and none from after it
        // is present — checkably, since the shards' own deltasApplied must then sum to
        // routedDeltasAtFence. Off by default because the fence pauses the shard tier's ingest for the
        // duration of the scan; that cost is real, is paid only by the caller who asks for it, and buys no
        // extra memory anywhere (nothing versioned is retained).
        //
        // Per-key reads need none of this and get none of it: one key is one grain, already strictly
        // consistent by construction.
        group.MapGet("/{id}/shards/scan", async (string id, int? limit, int? offset, bool? fenced, ICatalogFacade registry, ITableShardFacade shards) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            if (def.ShardBy.Count == 0)
            {
                return Results.BadRequest(new ErrorResponse("This table is not sharded (shardBy is empty)."));
            }

            var take = limit ?? 100;
            var skip = offset ?? 0;

            if (fenced == true)
            {
                var cut = await shards.ScanFencedAsync(def.Name, take, skip);
                return Results.Ok(new TableShardScanResponse(
                    cut.Shards, cut.Shards.Count, skip, take,
                    Fenced: true, FenceSeq: cut.FenceSeq, RoutedDeltasAtFence: cut.RoutedDeltasAtFence,
                    ShardCount: cut.ShardCount));
            }

            var stats = await shards.ScanAsync(def.Name, take, skip);
            return Results.Ok(new TableShardScanResponse(stats, stats.Count, skip, take));
        }).RequireAuthorization("Viewer");
    }

    private static async Task<Dictionary<string, SourceSchema>> BuildStreamSchemasAsync(ICatalogFacade registry)
    {
        var sources = await registry.GetSourcesAsync();
        var schemas = new Dictionary<string, SourceSchema>();
        foreach (var src in sources)
        {
            var fields = src.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type));
            schemas[src.Name] = new SourceSchema(src.Name, fields);
        }
        return schemas;
    }

    // excludeTableId (plan 008 W5, additive/optional — default null keeps the pre-existing /validate
    // call site unchanged): the /{id}/plan endpoint recompiles an EXISTING table's SQL, so its own
    // OutputFields shouldn't appear as a candidate FROM/JOIN target for itself — mirrors
    // RegistryGrain.CompileTableSql/BuildTableSchemas's excludeTableId parameter for updates.
    private static async Task<Dictionary<string, SourceSchema>> BuildTableSchemasAsync(ICatalogFacade registry, string? excludeTableId = null)
    {
        var tables = await registry.GetTablesAsync();
        var schemas = new Dictionary<string, SourceSchema>();
        foreach (var t in tables.Where(t => t.Id != excludeTableId && t.OutputFields.Count > 0))
        {
            var fields = t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type));
            schemas[t.Name] = new SourceSchema(t.Name, fields);
        }
        return schemas;
    }

    /// <summary>The upsert half of the row-identity report (the other half is GET /{id}/metrics, which
    /// serves the same sentence to the console). A create/update that leaves a table with a degraded
    /// version trail is ACCEPTED — the whole-row fallback is pre-existing behavior and real catalogs rely
    /// on it, so rejecting would break working installs — but it no longer passes in silence: the server
    /// log names the table and says what happened, at the moment the definition that caused it was
    /// written. Draft-friendly in exactly the sense ApplyCompileResult is: it reports, it never blocks.</summary>
    private static void WarnOnDegradedRowIdentity(ILoggerFactory loggers, TableDefinition def)
    {
        var warning = TableRowIdentityWarning.For(def);
        if (warning is null)
        {
            return;
        }

        loggers.CreateLogger("StreamForge.Api.Tables")
            .LogWarning("Table '{Table}': {Warning}", def.Name, warning);
    }

    private static FieldKind MapFieldKind(FieldType type) => type switch
    {
        FieldType.String => FieldKind.String,
        FieldType.Double => FieldKind.Double,
        FieldType.Long => FieldKind.Long,
        FieldType.Bool => FieldKind.Bool,
        FieldType.Timestamp => FieldKind.Timestamp,
        FieldType.Json => FieldKind.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };
}

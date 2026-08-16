using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using StreamForge.AppCore.Ingest;
using StreamForge.AppCore.Json;
using StreamForge.Host.Auth;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Api;

public static class SourcesEndpoints
{
    public static void MapSourcesEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/sources");

        // D-H: every read path masks secrets (URL header values; gRPC password/token) as "***".
        group.MapGet("/", async (ICatalogFacade registry) =>
            Results.Ok((await registry.GetSourcesAsync()).Select(SecretsMasker.Mask).ToList())
        ).RequireAuthorization("Viewer");

        group.MapGet("/{name}", async (string name, ICatalogFacade registry) =>
        {
            var src = await registry.GetSourceAsync(name);
            return src is null ? Results.NotFound() : Results.Ok(SecretsMasker.Mask(src));
        }).RequireAuthorization("Viewer");

        // Downloadable, self-contained .proto for this source: DescriptorFactory's schema plus the
        // DynamicStreamService streaming contract, ready for a client to compile standalone (see
        // tools/generate-client.sh). Field numbers always come from ICatalogFacade.EnsureFieldNumbersAsync
        // so they stay identical to what the dynamic gRPC reflection surface uses.
        group.MapGet("/{name}/proto", async (string name, ICatalogFacade registry) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (src is null)
            {
                return Results.NotFound();
            }

            var numbersJson = await registry.EnsureFieldNumbersAsync(EntitySchemas.SourceKey(src.Name), src.Fields);
            var numbers = EntitySchemas.ParseMap(numbersJson);
            var schema = DescriptorFactory.Generate(src.Name, src.Fields, numbers);
            var protoText = ProtoFileBuilder.Build("source", src.Name, schema);

            return Results.File(Encoding.UTF8.GetBytes(protoText), "text/plain; charset=utf-8", schema.FileProto.Name);
        }).RequireAuthorization("Viewer");

        group.MapPost("/", async (SourceDefinition def, ICatalogFacade registry) =>
        {
            var errors = SourceValidation.Validate(def);
            if (errors.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", errors)));
            }

            var existing = await registry.GetSourceAsync(def.Name);
            if (existing is not null)
            {
                return Results.BadRequest(new ErrorResponse("source name already exists"));
            }

            // D-H: incoming "***" values have nothing to restore from on a create (existing is
            // null here) — MergeSecrets is still run for compositional symmetry with PUT below
            // (it's a no-op when `stored` is null).
            var effective = SecretsMasker.MergeSecrets(def, existing);
            await registry.UpsertSourceAsync(effective);
            return Results.Created($"/api/sources/{effective.Name}", SecretsMasker.Mask(effective));
        }).RequireAuthorization("Editor");

        group.MapPut("/{name}", async (string name, SourceDefinition def, ICatalogFacade registry) =>
        {
            var existing = await registry.GetSourceAsync(name);
            if (existing is null)
            {
                return Results.NotFound();
            }

            def.Name = name;
            var errors = SourceValidation.Validate(def);
            if (errors.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", errors)));
            }

            // D-H CRITICAL invariant: an incoming "***" (the SPA's GET-then-edit-then-PUT-whole-
            // object cycle round-tripping a masked value verbatim) is replaced with the STORED real
            // secret before it ever reaches UpsertSourceAsync — never persist the literal mask.
            var effective = SecretsMasker.MergeSecrets(def, existing);
            await registry.UpsertSourceAsync(effective);
            return Results.Ok(SecretsMasker.Mask(effective));
        }).RequireAuthorization("Editor");

        group.MapDelete("/{name}", async (string name, ICatalogFacade registry) =>
        {
            var removed = await registry.DeleteSourceAsync(name);
            return removed ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization("Editor");

        // GET /{name}/status — connector runtime status (D-C). null (no connector status tracked,
        // e.g. generator-kind sources) -> 204; missing source -> 404.
        group.MapGet("/{name}/status", async (string name, ICatalogFacade registry, IConnectorStatusFacade statusFacade) =>
        {
            var src = await registry.GetSourceAsync(name);
            var status = src is null ? null : await statusFacade.GetStatusAsync(name);
            return SourceSchemaService.DecideStatusOutcome(src is not null, status) switch
            {
                SourceStatusOutcome.NotFound => Results.NotFound(),
                SourceStatusOutcome.NoContent => Results.NoContent(),
                _ => Results.Ok(status),
            };
        }).RequireAuthorization("Viewer");

        // ---- Client-push ingress (plan 008 W4). POST holds NO policy of its own — every branch below
        // is a 1:1 mapping of IngestOutcome onto an HTTP status per IngestModels.cs's own doc comments;
        // all the actual admission/coercion decisions live in IIngressFacade/AppCore.Ingest. ----

        // Plan 009 A1.2: this route deliberately does NOT `.RequireAuthorization("Editor")` — a
        // machine pushing telemetry may authenticate with a per-source push key instead of an Editor
        // JWT (AllowAnonymous below, then a manual dual check: Editor JWT OR a valid X-SF-Ingest-Key
        // for THIS source). An ingest source with zero configured keys is JWT-only, not open — see
        // IsAuthorizedToPushAsync/IIngressFacade.ValidateKeyAsync's own doc comment.
        group.MapPost("/{name}/events", async (
            string name, IngestEventsRequest req, IIngressFacade ingress, ICatalogFacade registry,
            IAuthorizationService authz, HttpContext http) =>
        {
            if (!await IsAuthorizedToPushAsync(name, http, authz, ingress))
            {
                return Results.Unauthorized();
            }

            // Plan 009 A1.1: body wins over the header when both are present (IngestEventsRequest's
            // own doc comment).
            var idempotencyKey = !string.IsNullOrEmpty(req.IdempotencyKey)
                ? req.IdempotencyKey
                : http.Request.Headers["Idempotency-Key"].FirstOrDefault();

            // Wishlist "explicit key retraction through ingest": a "_retract" row is only meaningful
            // to a LATEST BY consumer (TableLatestByOp is the only op that tracks "the current row for
            // a key" — see its class doc). Validated HERE, before admission, because IIngressFacade's
            // real implementations (OrleansIngressFacade/DaprIngressFacade) have no catalog access and
            // their call into IngressRowAcceptance.AcceptBatch is a frozen call site that cannot be
            // widened to take one — this REST handler is the one place upstream of admission that both
            // sees the SQL catalog and can still turn a bad request into a 400 instead of a corrupted
            // or silently-ignored table. KNOWN GAP, stated plainly: the gRPC ingest path
            // (IngestGrpcService) calls IIngressFacade.PushAsync directly and does not run this gate —
            // a retraction pushed that way is protected only by TableReduceOp's unmatched-retraction
            // handling (reports nothing, never a wrong number — see its own doc) and TableLatestByOp's
            // unknown-key no-op; safe, but silent, exactly what this gate exists to avoid on the REST
            // path the wishlist actually asked for.
            var retractRowIndexes = CollectRetractRowIndexes(req.Events);
            if (retractRowIndexes.Count > 0)
            {
                var offendingTable = RetractConsumerValidation.FindNonLatestByConsumer(
                    name, await registry.GetSourcesAsync(), await registry.GetTablesAsync());
                if (offendingTable is not null)
                {
                    var message = $"\"_retract\" is only valid when every running table reading source '{name}' directly is a LATEST BY table; '{offendingTable}' is not";
                    var retractErrors = retractRowIndexes.Select(i => $"row {i}: {message}").ToList();

                    if (!req.Partial)
                    {
                        return Results.Json(
                            new IngestErrorResponse($"{retractErrors.Count} row(s) failed retract validation", 0, retractErrors),
                            statusCode: StatusCodes.Status400BadRequest);
                    }

                    // Partial: admit everything else, fold the offending rows into Invalid/RowErrors
                    // exactly like a coercion failure would (IngestModels.cs: "Accepted + Dropped +
                    // Invalid + Duplicate accounts for every row in the request") — an admitted
                    // retraction into a wrongly-shaped table is not a lesser evil than dropping it.
                    var offendingSet = new HashSet<int>(retractRowIndexes);
                    var filteredEvents = req.Events.Where((_, i) => !offendingSet.Contains(i)).ToList();
                    var partialResult = await ingress.PushAsync(name, filteredEvents, req.Partial, idempotencyKey);
                    partialResult.Invalid += retractErrors.Count;
                    partialResult.RowErrors = [.. partialResult.RowErrors, .. retractErrors];
                    return await BuildEventsResponseAsync(name, partialResult, ingress, http);
                }
            }

            var result = await ingress.PushAsync(name, req.Events, req.Partial, idempotencyKey);
            return await BuildEventsResponseAsync(name, result, ingress, http);
        }).AllowAnonymous(); // real gate is the manual dual check above, not route-level authorization

        // GET /{name}/ingest — ingress buffer status (plan 008 W4). Deliberately NOT overloaded onto
        // /status above, which is the connector-runtime surface; reuses the same three-way
        // NotFound/NoContent/Ok decision (SourceSchemaService.DecideStatusOutcome's bool overload).
        group.MapGet("/{name}/ingest", async (string name, ICatalogFacade registry, IIngressFacade ingress) =>
        {
            var src = await registry.GetSourceAsync(name);
            var status = src is null ? null : await ingress.GetStatusAsync(name);
            return SourceSchemaService.DecideStatusOutcome(src is not null, status is not null) switch
            {
                SourceStatusOutcome.NotFound => Results.NotFound(),
                SourceStatusOutcome.NoContent => Results.NoContent(),
                _ => Results.Ok(ToIngestStatusResponse(status!)),
            };
        }).RequireAuthorization("Viewer");

        // ---- Per-source push keys (plan 009 A1.2). All three Editor — key material is a credential-
        // management concern, the same bar as every other source-mutating endpoint in this file. ----

        // POST generates a fresh secret, stores only its salted hash (AppCore/Auth/PasswordHasher —
        // the same primitive user passwords use), and returns the plaintext secret EXACTLY ONCE. There
        // is no other way to ever read it back — GET below only ever returns identity + usage.
        group.MapPost("/{name}/ingest/keys", async (string name, CreateIngestKeyRequest req, ICatalogFacade registry) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (src is null)
            {
                return Results.NotFound();
            }

            if (src.Kind != SourceKinds.Ingest)
            {
                return Results.Json(new ErrorResponse($"source '{name}' is not ingest-kind"), statusCode: StatusCodes.Status409Conflict);
            }

            var secret = GenerateIngestKeySecret();
            var (hash, salt) = PasswordHasher.Hash(secret);
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var key = new IngestKey
            {
                Id = Guid.NewGuid().ToString("N"),
                Hash = hash,
                Salt = salt,
                Label = req.Label ?? "",
                CreatedAtMs = nowMs,
            };

            src.Ingest ??= new IngestConfig();
            src.Ingest.Keys.Add(key);
            await registry.UpsertSourceAsync(src);

            return Results.Ok(new CreatedIngestKeyResponse(key.Id, key.Label, secret, key.CreatedAtMs));
        }).RequireAuthorization("Editor");

        // GET lists identity + best-effort usage only (IngestKeyUsageTracker.GetLastUsedMs overlays
        // this replica's own in-memory view on top of whatever was last durably stored — see that
        // class's doc comment for why LastUsedMs isn't round-tripped through UpsertSourceAsync on
        // every push).
        group.MapGet("/{name}/ingest/keys", async (string name, ICatalogFacade registry, IngestKeyUsageTracker usage) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (src is null)
            {
                return Results.NotFound();
            }

            var keys = (src.Ingest?.Keys ?? [])
                .Select(k => new IngestKeyResponse(k.Id, k.Label, k.CreatedAtMs, Math.Max(k.LastUsedMs, usage.GetLastUsedMs(name, k.Id))))
                .ToList();
            return Results.Ok(keys);
        }).RequireAuthorization("Editor");

        // DELETE revokes — the key immediately stops authorizing pushes to this source (ValidateKeyAsync
        // only ever checks IngestConfig.Keys as currently stored).
        group.MapDelete("/{name}/ingest/keys/{id}", async (string name, string id, ICatalogFacade registry) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (src is null)
            {
                return Results.NotFound();
            }

            var removed = (src.Ingest?.Keys.RemoveAll(k => k.Id == id) ?? 0) > 0;
            if (!removed)
            {
                return Results.NotFound();
            }

            await registry.UpsertSourceAsync(src);
            return Results.NoContent();
        }).RequireAuthorization("Editor");

        // ---- Schema helper endpoints for the UI (Editor — even mapping-validate/derive-openapi,
        // which don't themselves dial out, since they're part of the connector-authoring flow that
        // from-remote (which DOES dial out) also belongs to; keeps all three under one auth policy). ----

        group.MapPost("/schema/mapping-validate", (MappingValidateRequest request) =>
            Results.Ok(SourceSchemaService.ValidateMappingDocument(request))
        ).RequireAuthorization("Editor");

        group.MapPost("/schema/derive-openapi", async (SchemaDeriveRequest request, CancellationToken ct) =>
            Results.Ok(await SourceSchemaService.DeriveOpenApiAsync(request, ct))
        ).RequireAuthorization("Editor");

        // Dials out to a remote gRPC endpoint per the request body — Editor-only (D-G).
        group.MapPost("/schema/from-remote", async (RemoteSchemaRequest request, CancellationToken ct) =>
            Results.Ok(await SourceSchemaService.FromRemoteAsync(request, ct))
        ).RequireAuthorization("Editor");
    }

    private static IngestStatusResponse ToIngestStatusResponse(IngestStatus s) => new(
        s.Policy.ToString(),
        s.CapacityRows,
        s.DepthRows,
        s.MaxBatchRows,
        s.TotalAccepted,
        s.TotalRejected,
        s.TotalDropped,
        s.TotalInvalid,
        s.TotalPublished,
        s.DownstreamDropped,
        s.LastPushMs,
        s.TotalDuplicate,
        s.InstanceId,
        s.Aggregated);

    /// <summary>Plan 009 A1.2: the dual auth check POST /{name}/events runs instead of a route-level
    /// <c>RequireAuthorization("Editor")</c> — an Editor JWT (checked via the REAL "Editor" policy
    /// through <see cref="IAuthorizationService"/>, not a hand-rolled role check, so Admin still
    /// satisfies it exactly like every other Editor-gated endpoint) always suffices; otherwise the
    /// <c>X-SF-Ingest-Key</c> header is validated against THIS source's configured keys. An ingest
    /// source with zero configured keys is JWT-only, not open — that rule lives in
    /// <see cref="IIngressFacade.ValidateKeyAsync"/>, not here, so REST and gRPC can't drift.</summary>
    private static async Task<bool> IsAuthorizedToPushAsync(string sourceName, HttpContext http, IAuthorizationService authz, IIngressFacade ingress)
    {
        var editorResult = await authz.AuthorizeAsync(http.User, "Editor");
        if (editorResult.Succeeded)
        {
            return true;
        }

        var presentedKey = http.Request.Headers["X-SF-Ingest-Key"].FirstOrDefault();
        return await ingress.ValidateKeyAsync(sourceName, presentedKey);
    }

    /// <summary>Plan 009 A1.2: cryptographically secure secret generation — 32 random bytes, hex-
    /// encoded, with a stable "sfk_" prefix so a leaked secret is recognizable as a StreamForge ingest
    /// key at a glance (same spirit as Stripe/GitHub-style prefixed tokens).</summary>
    private static string GenerateIngestKeySecret() => "sfk_" + RandomNumberGenerator.GetHexString(64, lowercase: true);

    /// <summary>Wishlist "explicit key retraction through ingest": indexes of every row in
    /// <paramref name="events"/> that asks for a retraction, ahead of IngressRowAcceptance.Accept ever
    /// running (that happens inside <see cref="IIngressFacade.PushAsync"/>, after this validate-time
    /// gate) — so this reads the raw request body's own value for
    /// <c>IngressRowAcceptance.RetractField</c> and normalizes it itself
    /// (<see cref="JsonValueNormalizer.Normalize"/>) rather than reusing the coerced/normalized row
    /// IngressRowAcceptance would produce, which does not exist yet at this point in the request.</summary>
    private static List<int> CollectRetractRowIndexes(IReadOnlyList<Dictionary<string, object?>> events)
    {
        var indexes = new List<int>();
        for (var i = 0; i < events.Count; i++)
        {
            if (events[i].TryGetValue(IngressRowAcceptance.RetractField, out var raw)
                && JsonValueNormalizer.Normalize(raw) is true)
            {
                indexes.Add(i);
            }
        }
        return indexes;
    }

    /// <summary>The POST /{name}/events response mapping, extracted unchanged from the handler so the
    /// retract-validation branch above (which may itself call <see cref="IIngressFacade.PushAsync"/>,
    /// on the FILTERED batch, before merging its own row errors in) can reuse the exact same
    /// IngestOutcome -&gt; HTTP mapping as the plain path below it — one switch, not two copies that
    /// could drift.</summary>
    private static async Task<IResult> BuildEventsResponseAsync(string name, IngestResult result, IIngressFacade ingress, HttpContext http)
    {
        switch (result.Outcome)
        {
            case IngestOutcome.Accepted:
                // DepthRows/CapacityRows aren't on IngestResult (that contract is frozen) — a
                // second, in-memory-only GetStatusAsync call (no extra I/O; same buffer instance)
                // fills them in for the 202 body.
                var status = await ingress.GetStatusAsync(name);
                return Results.Json(
                    new IngestAcceptedResponse(
                        result.Accepted, result.Dropped, result.Invalid, status?.DepthRows ?? 0, status?.CapacityRows ?? 0,
                        result.Duplicate, result.Replayed),
                    statusCode: StatusCodes.Status202Accepted);

            case IngestOutcome.Invalid:
                return Results.Json(
                    new IngestErrorResponse(result.Error ?? "one or more rows failed coercion", 0, result.RowErrors),
                    statusCode: StatusCodes.Status400BadRequest);

            case IngestOutcome.NotFound:
                return Results.NotFound();

            case IngestOutcome.WrongKind:
                return Results.Json(
                    new IngestErrorResponse(result.Error ?? $"source '{name}' is not ingest-kind", 0, result.RowErrors),
                    statusCode: StatusCodes.Status409Conflict);

            case IngestOutcome.TooLarge:
                return Results.Json(
                    new IngestErrorResponse(result.Error ?? "batch exceeds the source's ingest limits", 0, result.RowErrors),
                    statusCode: StatusCodes.Status413PayloadTooLarge);

            case IngestOutcome.Overloaded:
            default:
                // Retry-After: whole seconds clamped to [1,30] — IngestResult.RetryAfterMs is the
                // unclamped estimate (IngressAdmission.Decision doc), this is where it becomes an
                // honest HTTP header.
                var retryAfterSeconds = Math.Clamp((int)Math.Ceiling(result.RetryAfterMs / 1000.0), 1, 30);
                http.Response.Headers.RetryAfter = retryAfterSeconds.ToString(CultureInfo.InvariantCulture);
                return Results.Json(
                    new IngestErrorResponse(result.Error ?? "ingress buffer has no room for this batch", retryAfterSeconds * 1000, result.RowErrors),
                    statusCode: StatusCodes.Status429TooManyRequests);
        }
    }
}

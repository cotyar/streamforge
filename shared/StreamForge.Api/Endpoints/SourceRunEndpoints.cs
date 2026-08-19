using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;
using StreamForge.Host.Generators;

namespace StreamForge.Api;

/// <summary>Wishlist #8: <c>POST /api/sources/{name}/run</c> — run-on-demand for a
/// <see cref="GeneratorProfiles.Scenario"/>-profile generator source. Computes the deterministic batch
/// with the exact same <c>ScenarioGenerator.GenerateBatch</c> call
/// (shared/StreamForge.AppCore/Generators/ScenarioGenerator.cs) that
/// <c>IGeneratorGrain.RunAsync</c>/<c>GeneratorActor.RunAsync</c> use, and returns the wishlist's literal
/// <c>{ accepted, rows }</c> shape.
///
/// <para><b>KNOWN GAP — this endpoint does not publish onto the source's live stream/pub-sub.</b> Both
/// runtime flavors' generator (Orleans <c>GeneratorGrain.RunAsync</c>,
/// orleans/src/StreamForge.Host/Grains/GeneratorGrain.cs; Dapr <c>GeneratorActor.RunAsync</c>,
/// dapr/src/StreamForge.Dapr.Host/Actors/GeneratorActor.cs) DO publish every generated row the same way a
/// tick would — but this file lives in shared/StreamForge.Api, which (like every other file in this
/// project — see e.g. <c>SourcesEndpoints.cs</c>) has ZERO Orleans/Dapr-specific dependencies: every
/// runtime-specific capability it uses arrives as a facade interface (<see cref="ICatalogFacade"/>,
/// <c>IIngressFacade</c>, ...) registered per-flavor in each host's DI container
/// (orleans/src/StreamForge.Host/Facades/OrleansFacades.cs and its Dapr-Host-Program.cs counterpart —
/// neither file was in this change's file-ownership scope, a sibling-agent wave-discipline constraint;
/// see AGENTS.md's "Multi-agent wave discipline"). Adding a facade for "publish a generated batch onto a
/// generator source's stream" and registering it in both hosts is therefore the concrete next step to
/// close this gap; it was out of scope here. What IS fully wired and tested: the deterministic row math
/// itself (ScenarioGeneratorTests), and each flavor's own RunAsync end-to-end INCLUDING the stream/pub-sub
/// publish, exercised directly via IGrainFactory in a TestCluster
/// (orleans/tests/StreamForge.Host.Tests/GeneratorGrainScenarioRunTests.cs) — this endpoint is "just" the
/// missing last hop from HTTP to that already-working grain call.</para>
/// </summary>
public static class SourceRunEndpoints
{
    public static void MapSourceRunEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/sources");

        // Editor-gated: like POST /api/sources itself, running a generator on demand is an operator
        // action, not a machine-push credential path (contrast with POST /{name}/events' per-source-key
        // AllowAnonymous dual-auth in SourcesEndpoints.cs — that's for telemetry producers, this isn't).
        //
        // Plan 015 wave 3-A: the Editor policy above stays as the compatibility floor and the handler
        // additionally asks AccessGuard for source.run AT THIS SOURCE, with the definition's Tags, so a
        // `tag:demo`-scoped or `dev-*`-scoped entitlement to run generators is expressible. The guard
        // runs BEFORE the 404/409 answers below for the reason it does everywhere in this wave: an
        // unentitled caller must not be able to use the status code to enumerate what exists. `src` is
        // looked up first only because the check needs its Tags — the null case falls back to the route
        // segment, which is the source's name anyway.
        group.MapPost("/{name}/run", async (string name, ScenarioRunRequest request, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var src = await registry.GetSourceAsync(name);

            var decision = await guard.CheckAsync(principal, Actions.SourceRun, src?.Name ?? name, src?.Tags);
            if (!decision.IsAllowed)
            {
                return AccessGuard.Deny(decision);
            }

            if (src is null)
            {
                return Results.NotFound();
            }

            if (src.GeneratorProfile != GeneratorProfiles.Scenario || src.Scenario is null)
            {
                return Results.Json(
                    new ErrorResponse($"source '{name}' is not a scenario-profile generator (GeneratorProfile must be '{GeneratorProfiles.Scenario}' with a scenario spec configured)"),
                    statusCode: StatusCodes.Status409Conflict);
            }

            // Delegate to the runtime rather than generating here. Computing the batch in this assembly
            // would return rows to the caller while emitting NOTHING onto the source's stream — a run
            // that looks successful and moves no data. Only a runtime can publish, so the facade owns it.
            var result = await registry.RunSourceAsync(name, request);
            return result.Outcome switch
            {
                ScenarioRunOutcome.Accepted => Results.Ok(new ScenarioRunResponse(result.Accepted, result.Rows)),
                ScenarioRunOutcome.ValidationError => Results.Json(
                    new ErrorResponse(string.Join("; ", result.Errors)),
                    statusCode: StatusCodes.Status400BadRequest),
                // Both pre-checked above (src null / wrong profile), so in practice the runtime cannot
                // answer with either — but it re-checks on its own side, and a source deleted between
                // the two calls would legitimately land here. Kept exhaustive rather than assumed away.
                ScenarioRunOutcome.NotFound => Results.NotFound(),
                _ => Results.Json(
                    new ErrorResponse($"source '{name}' is not a scenario-profile generator"),
                    statusCode: StatusCodes.Status409Conflict),
            };
        }).RequireAuthorization("Editor");
    }
}

/// <summary>The wishlist's literal <c>{ accepted, rows }</c> response shape.</summary>
public sealed record ScenarioRunResponse(int Accepted, List<ScenarioRow> Rows);

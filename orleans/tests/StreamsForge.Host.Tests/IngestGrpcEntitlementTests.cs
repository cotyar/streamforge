using System.Security.Claims;
using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging.Abstractions;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.Host.Grpc;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 015 wave 3-B — the gRPC ingest push path is the one gRPC method with no <c>[Authorize]</c>, and
/// that is deliberate (plan 009 A1.2: it authorizes per MESSAGE, because the source name travels on every
/// message and a key only ever authorizes one source). Adding an entitlement check there had exactly one
/// way to go wrong: breaking the key holders.
///
/// <para><b>The load-bearing test is <see cref="AnIngestKeyStillWorksForAPrincipalWithNoEntitlementsAtAll"/>.</b>
/// An ingest key authenticates a machine, not a user — there is no username to resolve, no entry in the
/// access document, and therefore no entitlements to hold. If the guard were consulted on that branch,
/// every telemetry producer in every deployment would start getting <c>Unauthenticated</c> the moment this
/// wave shipped.</para>
///
/// <para>Driven through <see cref="IngestGrpcService.AuthorizeMessageAsync"/>, which takes values rather
/// than a <see cref="ServerCallContext"/> for exactly this reason — the same "extract the seam, there is
/// no HTTP/gRPC harness in this repo" move <c>IngestGrpcServiceRetractGateTests</c> already makes against
/// <c>BuildRetractErrors</c>/<c>RejectedResult</c>.</para>
/// </summary>
public class IngestGrpcEntitlementTests
{
    private const string Source = "telemetry";

    private static IngestGrpcService Service(bool editorPolicyPasses, bool keyValid, AccessPolicyDocument document, bool entitlements = true)
    {
        var resolver = new PermissionResolver(
            new CountingAccessPolicyFacade(document), NullLogger<PermissionResolver>.Instance, 600);

        return new IngestGrpcService(
            new StubIngress(keyValid),
            new StubAuthorization(editorPolicyPasses),
            // Never touched by the authorization seam; a throwing stub makes an accidental call loud.
            registry: null!,
            new AccessGuard(resolver, entitlements));
    }

    private static AccessPolicyDocument DocumentGranting(params PermissionGrant[] grants)
    {
        var document = PermissionResolverTests.Doc(version: 1);
        document.Users.Add(new UserAccessEntry { Username = "pusher", Grants = [.. grants] });
        return document;
    }

    // ---------------------------------------------------------------------------------------------
    // The ingest-key branch: untouched, and it must stay that way
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task AnIngestKeyStillWorksForAPrincipalWithNoEntitlementsAtAll()
    {
        // No JWT at all (the Editor policy fails), a valid key, and an access document that grants this
        // caller precisely nothing. This is the shape every machine producer in the field has.
        var service = Service(editorPolicyPasses: false, keyValid: true, PermissionResolverTests.Doc(version: 1));

        var refusal = await service.AuthorizeMessageAsync(new ClaimsPrincipal(new ClaimsIdentity()), "sfk_whatever", Source);

        Assert.Null(refusal);
    }

    [Fact]
    public async Task AnIngestKeyRescuesAJwtCallerWhoseEntitlementDoesNotCoverTheSource()
    {
        // Both credentials presented, the JWT half refused. Before this wave the message was admitted on
        // the JWT alone; short-circuiting the refusal would therefore have stopped this exact producer
        // working. The key is consulted in strictly more cases than before and never in fewer.
        var service = Service(editorPolicyPasses: true, keyValid: true, DocumentGranting());

        Assert.Null(await service.AuthorizeMessageAsync(PermissionResolverTests.Principal("pusher"), "sfk_whatever", Source));
    }

    [Fact]
    public async Task NoJwtAndNoValidKeyIsStillTheSameUnauthenticatedRefusal()
    {
        var service = Service(editorPolicyPasses: false, keyValid: false, PermissionResolverTests.Doc(version: 1));

        var refusal = await service.AuthorizeMessageAsync(new ClaimsPrincipal(new ClaimsIdentity()), null, Source);

        Assert.NotNull(refusal);
        Assert.Equal(StatusCode.Unauthenticated, refusal!.Value.StatusCode);
        Assert.Equal("an Editor JWT or a valid X-SF-Ingest-Key for this source is required", refusal.Value.Detail);
    }

    // ---------------------------------------------------------------------------------------------
    // The JWT branch: this is where the entitlement went
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task AnEditorJwtEntitledToIngestThisSourceIsAdmitted()
    {
        var service = Service(
            editorPolicyPasses: true,
            keyValid: false,
            DocumentGranting(new PermissionGrant { Action = Actions.SourceIngest, Scope = Source }));

        Assert.Null(await service.AuthorizeMessageAsync(PermissionResolverTests.Principal("pusher"), null, Source));
    }

    [Fact]
    public async Task AnEditorJwtScopedToAnotherSourceIsRefusedWithTheReasonAndNotABareCode()
    {
        var service = Service(
            editorPolicyPasses: true,
            keyValid: false,
            DocumentGranting(new PermissionGrant { Action = Actions.SourceIngest, Scope = "other-*" }));

        var refusal = await service.AuthorizeMessageAsync(PermissionResolverTests.Principal("pusher"), null, Source);

        Assert.NotNull(refusal);
        Assert.Equal(StatusCode.PermissionDenied, refusal!.Value.StatusCode);
        // Not the generic sentence: this caller authenticated fine and their problem is the grant, which
        // is what the detail has to say. A bare PermissionDenied is the failure mode plan 015 removes.
        Assert.Contains(Actions.SourceIngest, refusal.Value.Detail, StringComparison.Ordinal);
        Assert.Contains(Source, refusal.Value.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARequiresApprovalGrantIsRefusedDistinctlyAndNotAsADenial()
    {
        // Waves 4-5 own filing the request. Until then it must be refused — but as its own status, so a
        // client can tell "nobody may do this" from "somebody has to say yes first" without parsing text.
        var service = Service(
            editorPolicyPasses: true,
            keyValid: false,
            DocumentGranting(new PermissionGrant { Action = Actions.SourceIngest, Scope = Source, RequiresApproval = true }));

        var refusal = await service.AuthorizeMessageAsync(PermissionResolverTests.Principal("pusher"), null, Source);

        Assert.NotNull(refusal);
        Assert.Equal(StatusCode.FailedPrecondition, refusal!.Value.StatusCode);
        Assert.Contains("approval", refusal.Value.Detail, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ADeniedGrantBeatsAnAllowAndSaysWhichGrantDidIt()
    {
        var service = Service(
            editorPolicyPasses: true,
            keyValid: false,
            DocumentGranting(
                new PermissionGrant { Action = Actions.SourceIngest, Scope = "*" },
                new PermissionGrant { Action = Actions.SourceIngest, Scope = Source, Effect = PermissionEffect.Deny }));

        var refusal = await service.AuthorizeMessageAsync(PermissionResolverTests.Principal("pusher"), null, Source);

        Assert.Equal(StatusCode.PermissionDenied, refusal!.Value.StatusCode);
        Assert.Contains("denied by grant", refusal.Value.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InLegacyModeAnEditorJwtIsAdmittedWithNoEntitlementAtAll()
    {
        // Auth:Mode=legacy is the one-flag rollback; it has to reach the ingest path too.
        var service = Service(editorPolicyPasses: true, keyValid: false, DocumentGranting(), entitlements: false);

        Assert.Null(await service.AuthorizeMessageAsync(PermissionResolverTests.Principal("pusher"), null, Source));
    }

    // ---------------------------------------------------------------------------------------------
    // Fakes
    // ---------------------------------------------------------------------------------------------

    /// <summary>Stands in for the real "Editor" policy — the service resolves it through
    /// <see cref="IAuthorizationService"/> precisely so REST and gRPC cannot drift, and what this test
    /// cares about is which of the two branches the answer selects.</summary>
    private sealed class StubAuthorization(bool succeeds) : IAuthorizationService
    {
        public Task<AuthorizationResult> AuthorizeAsync(ClaimsPrincipal user, object? resource, IEnumerable<IAuthorizationRequirement> requirements) =>
            Task.FromResult(succeeds ? AuthorizationResult.Success() : AuthorizationResult.Failed());

        public Task<AuthorizationResult> AuthorizeAsync(ClaimsPrincipal user, object? resource, string policyName) =>
            Task.FromResult(succeeds ? AuthorizationResult.Success() : AuthorizationResult.Failed());
    }

    private sealed class StubIngress(bool keyValid) : IIngressFacade
    {
        public Task<bool> ValidateKeyAsync(string sourceName, string? presentedKey) =>
            Task.FromResult(keyValid && !string.IsNullOrEmpty(presentedKey));

        public Task<IngestResult> PushAsync(string sourceName, IReadOnlyList<Dictionary<string, object?>> events, bool partial, string? idempotencyKey = null) =>
            throw new NotImplementedException("the authorization seam never pushes");

        public Task<IngestStatus?> GetStatusAsync(string sourceName) => throw new NotImplementedException();
    }
}

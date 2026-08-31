using System.Security.Claims;
using Microsoft.Extensions.Logging;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Access;

namespace StreamsForge.Api.Auth;

/// <summary>
/// Plan 015 wave 2 — the one place a live HTTP request meets the stored access policy.
///
/// <para><b>Why permissions are not in the token.</b> Tokens live 12 hours (see
/// <see cref="JwtTokenService.Lifetime"/>). Baking the grant set into the JWT would mean a revoked
/// entitlement, a disabled account or a completed approval took up to twelve hours to matter, which makes
/// an approval workflow theatre. So the decision is made server-side, per request, against a
/// version-stamped snapshot that this type keeps fresh.</para>
///
/// <para><b>Why a version poll and not a read.</b> A per-request store lookup is a grain call on Orleans
/// and a <i>sidecar round trip</i> on Dapr — on every read in the system. Instead: hold the document,
/// and no more than once per <c>Auth:PolicyCacheSeconds</c> (default 10) ask the store for its
/// <see cref="IAccessPolicyFacade.GetVersionAsync"/> — a single long — refetching the document only when
/// that number has moved. The TTL gates <b>the version poll itself</b>, not merely the document fetch;
/// gating only the fetch would leave the cheap call happening on every request, which on Dapr is the
/// expensive thing. Cost: one tiny call per TTL per replica. Benefit: revocation lands in ~10s instead
/// of 12h.</para>
///
/// <para><b>Three failure modes, three deliberate answers.</b>
/// <list type="bullet">
///   <item><i>Concurrency.</i> Refreshes are single-flight behind a semaphore with the deadline
///   re-checked inside it, so a hundred simultaneous requests arriving on a cold cache produce exactly
///   one version poll and at most one document fetch — the waiters find the deadline already renewed and
///   return the snapshot the winner installed.</item>
///   <item><i>A throwing store.</i> The previous snapshot keeps serving and the deadline is renewed
///   anyway. The alternative — failing the request — turns one flaky grain call into a cluster-wide
///   outage; the alternative to renewing the deadline is hammering a store that is already unwell.</item>
///   <item><i>A cold cache that has never loaded.</i> <see cref="HasSnapshot"/> stays false and the
///   snapshot is an empty document, which grants nothing. That is fail-closed for the guard, and the
///   Viewer policy deliberately reads <see cref="HasSnapshot"/> so that "the policy store is
///   unreachable" cannot lock every account out of a running cluster — see
///   <c>StrictViewerHandler</c>.</item>
/// </list></para>
/// </summary>
public sealed class PermissionResolver
{
    /// <summary>The OIDC seam. An IdP's group claim is read from day one even though OIDC itself is
    /// deferred (015 §OIDC), so that when it lands the mapping onto
    /// <see cref="GroupDefinition.ExternalClaimValues"/> is already implemented and tested rather than
    /// designed under deadline. A local login simply has none of these.</summary>
    public const string GroupsClaimType = "groups";

    private static readonly AccessPolicyDocument Empty = new();

    private readonly IAccessPolicyFacade _facade;
    private readonly ILogger<PermissionResolver> _logger;
    private readonly long _ttlMs;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private volatile AccessPolicyDocument _snapshot = Empty;
    private volatile bool _loaded;
    private long _nextPollAtMs;

    public PermissionResolver(
        IAccessPolicyFacade facade,
        ILogger<PermissionResolver> logger,
        int policyCacheSeconds)
    {
        _facade = facade;
        _logger = logger;
        _ttlMs = Math.Max(0, policyCacheSeconds) * 1000L;
    }

    /// <summary>False until a document has been read successfully at least once. The Viewer policy uses
    /// it to tell "this user has nothing" apart from "we have not managed to ask".</summary>
    public bool HasSnapshot => _loaded;

    /// <summary>The version of the snapshot currently serving. Diagnostics and tests only.</summary>
    public long Version => _snapshot.Version;

    /// <summary>Eager invalidation for a local mutation: the next call re-polls instead of waiting out
    /// the TTL. A mutation on another replica is picked up by the poll — this only shortens the window
    /// for the replica that made the change, which is the one whose caller is about to look.</summary>
    public void Invalidate() => Interlocked.Exchange(ref _nextPollAtMs, long.MinValue);

    /// <summary>The current policy snapshot, refreshed at most once per TTL.</summary>
    public async Task<AccessPolicyDocument> GetPolicyAsync()
    {
        if (_loaded && Environment.TickCount64 < Interlocked.Read(ref _nextPollAtMs))
        {
            return _snapshot;
        }

        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            // Re-checked inside the gate: this is what makes the refresh single-flight. Everyone who
            // queued behind the winner finds a renewed deadline and leaves without touching the store.
            if (_loaded && Environment.TickCount64 < Interlocked.Read(ref _nextPollAtMs))
            {
                return _snapshot;
            }

            try
            {
                var version = await _facade.GetVersionAsync().ConfigureAwait(false);
                if (!_loaded || version != _snapshot.Version)
                {
                    _snapshot = await _facade.GetPolicyAsync().ConfigureAwait(false);
                    _loaded = true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Access policy refresh failed; continuing to serve snapshot version {Version} (loaded: {Loaded}).",
                    _snapshot.Version,
                    _loaded);
            }

            Interlocked.Exchange(ref _nextPollAtMs, Environment.TickCount64 + _ttlMs);
            return _snapshot;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>One principal's flattened, version-stamped view of the current snapshot.</summary>
    public async Task<EffectivePermissions> ResolveAsync(ClaimsPrincipal principal) =>
        Build(await GetPolicyAsync().ConfigureAwait(false), principal);

    /// <summary>The claims → <see cref="EffectivePermissions"/> mapping, split out so it can be applied
    /// to a snapshot the caller already holds (the Viewer policy needs both).</summary>
    public static EffectivePermissions Build(AccessPolicyDocument document, ClaimsPrincipal principal)
    {
        ArgumentNullException.ThrowIfNull(principal);

        return EffectivePermissionsBuilder.Build(
            document,
            UsernameOf(principal),
            principal.FindAll(GroupsClaimType).Select(c => c.Value).ToList(),
            // ClaimTypes.Role is the PRE-UPGRADE fallback and nothing more: the builder consults it only
            // when the document has no entry for this user at all. See EffectivePermissionsBuilder's
            // remarks for why "empty role list" must not fall back to the token.
            principal.FindFirstValue(ClaimTypes.Role));
    }

    /// <summary>Tokens minted by <see cref="JwtTokenService"/> carry both <c>sub</c> and
    /// <see cref="ClaimTypes.Name"/>; the fallbacks are for a principal that came from somewhere else
    /// (a test, and one day an IdP whose inbound claim mapping is off).</summary>
    private static string UsernameOf(ClaimsPrincipal principal) =>
        principal.Identity?.Name
        ?? principal.FindFirstValue(ClaimTypes.Name)
        ?? principal.FindFirstValue(ClaimTypes.NameIdentifier)
        ?? principal.FindFirstValue("sub")
        ?? "";
}

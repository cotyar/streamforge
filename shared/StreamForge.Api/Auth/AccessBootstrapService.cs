using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StreamForge.Abstractions;
using StreamForge.AppCore.Access;

namespace StreamForge.Api.Auth;

/// <summary>
/// Plan 015 wave 2 — running <see cref="LegacyRoleMigration"/> for real, on both flavours, exactly once
/// per start.
///
/// <para><b>Why it is registered from <c>AddStreamForgeApi</c> and not from a host's
/// <c>Program.cs</c>.</b> Both hosts already call <c>AddStreamForgeApi</c>, so this is the single place
/// that reaches Orleans and Dapr at once; putting it in either <c>Program.cs</c> would mean editing both
/// (and one of the two most dangerous files in the repo) to say the same thing twice. A
/// <see cref="BackgroundService"/> is also the one shape that works on the Dapr compose stack, which
/// runs with no scheduler — reminders are off the table, and 015 established the shape after finding
/// five hosted services already running on the Orleans side.</para>
///
/// <para><b>It tolerates a store that is not ready.</b> On Dapr the sidecar may not be answering when
/// the app starts; on Orleans the silo may still be joining. So: bounded retries, then give up loudly.
/// It must never take the host down — a cluster that refuses to start because a role-mirroring
/// migration could not run is a far worse outage than one running on legacy-equivalent policies, and
/// the Editor/Admin policies are satisfied by the legacy role claim regardless of whether this ever
/// completes.</para>
///
/// <para><b>It writes only when something changed.</b> <see cref="LegacyRoleMigration.Apply"/> reports
/// that, and it matters more than it looks: every write bumps <see cref="AccessPolicyDocument.Version"/>
/// and therefore invalidates every replica's policy cache. A migration that wrote unconditionally would
/// do that on every host restart, forever.</para>
/// </summary>
internal sealed class AccessBootstrapService(
    IAccessPolicyFacade policy,
    IUserStoreFacade users,
    PermissionResolver resolver,
    ILogger<AccessBootstrapService> logger) : BackgroundService
{
    // ponytail: fixed, not configurable. 10 × 3s is ~30s of patience, which is longer than either
    // flavour takes to become answerable and short enough that a genuinely broken store is reported
    // while somebody is still watching the log. Ceiling: a deployment whose store takes minutes to come
    // up gets a logged give-up and no migration until the next restart. Upgrade path is two config
    // lookups on the day that happens.
    private const int MaxAttempts = 10;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(3);

    private const string Actor = "system";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Hosted services are started sequentially and awaited up to their first await; yielding here
        // keeps a slow or unreachable policy store off the host's startup path entirely.
        await Task.Yield();

        for (var attempt = 1; attempt <= MaxAttempts && !stoppingToken.IsCancellationRequested; attempt++)
        {
            try
            {
                await MigrateAsync().ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt < MaxAttempts)
            {
                logger.LogInformation(
                    ex,
                    "Access bootstrap attempt {Attempt}/{MaxAttempts} failed (policy or user store not ready yet); retrying in {Delay}s.",
                    attempt,
                    MaxAttempts,
                    RetryDelay.TotalSeconds);

                try
                {
                    await Task.Delay(RetryDelay, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "Access bootstrap gave up after {MaxAttempts} attempts. Built-in roles may be missing and user "
                    + "roles unmirrored, so entitlement-based authorization will grant nothing until this is fixed; "
                    + "the Editor/Admin policies still admit the legacy role claim. Restart the host to retry.",
                    MaxAttempts);
                return;
            }
        }
    }

    /// <summary>Read, migrate, write the delta.
    ///
    /// <para>The delta, rather than the document: <see cref="IAccessPolicyFacade"/> deliberately has no
    /// "replace the whole document" member — one exists nowhere precisely so that no caller can clobber
    /// a concurrent administrative edit — so the migration's result is applied as the upserts that
    /// produce it. <see cref="LegacyRoleMigration.Apply"/> only ever ADDS built-in roles that are absent
    /// and adds or fills user entries, so the diff is those two loops and nothing else.</para></summary>
    private async Task MigrateAsync()
    {
        var stored = await policy.GetPolicyAsync().ConfigureAwait(false);
        var records = await users.GetUsersAsync().ConfigureAwait(false);

        var target = LegacyRoleMigration.Apply(
            stored,
            records,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Actor,
            out var changed);

        if (!changed)
        {
            logger.LogDebug(
                "Access bootstrap: nothing to migrate (policy version {Version}, {UserCount} user(s)).",
                stored.Version,
                records.Count);
            return;
        }

        var seededRoles = 0;
        foreach (var role in target.Roles)
        {
            if (stored.Roles.Any(r => string.Equals(r.Name, role.Name, StringComparison.Ordinal)))
            {
                continue;
            }

            await policy.UpsertRoleAsync(role, Actor).ConfigureAwait(false);
            seededRoles++;
        }

        var mirroredUsers = 0;
        foreach (var entry in target.Users)
        {
            var before = stored.Users.FirstOrDefault(u => string.Equals(u.Username, entry.Username, StringComparison.Ordinal));
            if (before is not null && before.Roles.SequenceEqual(entry.Roles, StringComparer.Ordinal))
            {
                continue;
            }

            await policy.UpsertUserAccessAsync(entry, Actor).ConfigureAwait(false);
            mirroredUsers++;
        }

        // The writes above moved the version; without this the replica that made them would keep serving
        // its pre-migration snapshot for up to a full TTL — on the very first request after a fresh
        // start, which is the worst possible moment to look empty.
        resolver.Invalidate();

        logger.LogInformation(
            "Access bootstrap: seeded {Roles} built-in role(s) and mirrored {Users} user role list(s) into the access policy.",
            seededRoles,
            mirroredUsers);
    }
}

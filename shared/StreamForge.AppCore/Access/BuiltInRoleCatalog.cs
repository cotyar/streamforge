using StreamForge.Abstractions;

namespace StreamForge.AppCore.Access;

/// <summary>
/// Plan 015 — the three legacy roles, expressed as entitlements that reproduce today's behaviour
/// exactly.
///
/// <para><b>What "today" is.</b> The entire authorization model of the platform before this plan is
/// three ASP.NET policies registered in <c>shared/StreamForge.Api/StreamForgeApiExtensions.cs</c>:
/// <c>Viewer</c> = <c>RequireAuthenticatedUser()</c>, <c>Editor</c> = <c>RequireRole("Editor","Admin")</c>,
/// <c>Admin</c> = <c>RequireRole("Admin")</c>. Every route's requirement is the string it passes to
/// <c>RequireAuthorization(…)</c> at its map site under <c>shared/StreamForge.Api/Endpoints/**</c>, and
/// the gRPC surface says the same thing with <c>[Authorize(Policy = …)]</c> under
/// <c>orleans/src/StreamForge.Host/Grpc/**</c>. Those map sites — not anybody's memory of them — are
/// what the legacy-equivalence matrix in the tests enumerates.</para>
///
/// <para><b>Why Viewer's list is the interesting one.</b> "Viewer" today means *any authenticated user*,
/// so the Viewer role must hold an Allow for every action a Viewer-gated route will map to, or the
/// upgrade silently takes capability away from every account in the system. Editor is Viewer's set plus
/// the mutating half; Admin is a single <c>*</c> on <c>*</c>, because that is precisely what
/// <c>RequireRole("Admin")</c> on the most privileged routes plus membership of the other two policies
/// adds up to — and enumerating a hundred actions to say "everything" would be a list that goes stale
/// the first time somebody adds an action.</para>
///
/// <para><b>No new action names were needed.</b> Every route in the REST surface maps onto a constant
/// that already exists in <see cref="Actions"/>. The platform-metadata routes (<c>/api/meta/*</c>,
/// <c>/api/sql/functions</c>, <c>GET /api/transports</c>) map onto <see cref="Actions.CatalogRead"/> and
/// the transport probe onto <see cref="Actions.CatalogWrite"/>; those two constants exist for exactly
/// this "legacy-equivalent bundle" purpose.</para>
/// </summary>
public static class BuiltInRoleCatalog
{
    /// <summary>Everything a Viewer-gated route needs. Note <see cref="Actions.ApprovalRequest"/>: it
    /// grants no legacy capability (approvals ship disabled and have no legacy route), and it is here
    /// because *asking* for a second pair of eyes is not a privilege — deciding is. A build where the
    /// only people who could file a request were the people who did not need one would be a feature
    /// that shipped dead.</summary>
    private static readonly string[] ViewerActions =
    [
        Actions.SourceRead,
        Actions.PipelineRead,
        Actions.TableRead,
        Actions.ConfigExport,
        Actions.CatalogRead,
        Actions.ApprovalRequest,
    ];

    /// <summary>What Editor adds on top of <see cref="ViewerActions"/> — one entry per Editor-gated
    /// route group. <see cref="Actions.ChatUse"/> is in here because <c>POST /api/chat</c> is
    /// <c>RequireAuthorization("Editor")</c> today, which is also the reason plan 015 makes the chat's
    /// own tools re-check permissions: gated once at the door, it is otherwise the way around every
    /// entitlement this plan adds.</summary>
    private static readonly string[] EditorAdditionalActions =
    [
        Actions.SourceWrite,
        Actions.SourceDelete,
        Actions.SourceIngest,
        Actions.SourceRun,
        Actions.PipelineWrite,
        Actions.PipelineDelete,
        Actions.PipelineControl,
        Actions.TableWrite,
        Actions.TableDelete,
        Actions.TableControl,
        Actions.ConfigReplace,
        Actions.CatalogWrite,
        Actions.ChatUse,
    ];

    /// <summary>Fresh instances every call — never a shared static graph. These objects are written into
    /// the stored <see cref="AccessPolicyDocument"/> and an administrator edits them afterwards; handing
    /// out the same <see cref="PermissionGrant"/> instances twice would make one seed's edit show up in
    /// another's.</summary>
    public static List<RoleDefinition> Create(long nowMs = 0, string updatedBy = "system") =>
    [
        new RoleDefinition
        {
            Name = BuiltInRoles.Admin,
            Description = "Everything, everywhere. Reproduces the legacy Admin role, which satisfies all three policies.",
            BuiltIn = true,
            UpdatedAtMs = nowMs,
            UpdatedBy = updatedBy,
            // ponytail: one wildcard instead of an enumeration. Ceiling: an Admin cannot be carved back
            // ("everything except config.replace") without editing this grant into a list. Upgrade path
            // is exactly that edit — built-in roles are editable by design (only deletion is refused),
            // so the carve-back needs no code, and enumerating today would need maintenance forever.
            Grants = [new PermissionGrant { Action = "*", Scope = "*" }],
        },
        new RoleDefinition
        {
            Name = BuiltInRoles.Editor,
            Description = "Read everything, change everything in the catalog. Reproduces the legacy Editor role.",
            BuiltIn = true,
            UpdatedAtMs = nowMs,
            UpdatedBy = updatedBy,
            Grants = [.. ViewerActions.Concat(EditorAdditionalActions).Select(Allow)],
        },
        new RoleDefinition
        {
            Name = BuiltInRoles.Viewer,
            Description = "Read-only. Reproduces the legacy Viewer policy, which admits any authenticated user.",
            BuiltIn = true,
            UpdatedAtMs = nowMs,
            UpdatedBy = updatedBy,
            Grants = [.. ViewerActions.Select(Allow)],
        },
    ];

    private static PermissionGrant Allow(string action) =>
        new() { Action = action, Scope = "*", Effect = PermissionEffect.Allow };
}

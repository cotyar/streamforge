namespace StreamForge.Abstractions;

// Plan 015 — RBAC → entitlements, groups, approvals, escalation, audit.
//
// Everything the authorization decision reads lives in ONE document (AccessPolicyDocument) behind ONE
// singleton store, deliberately NOT in the user store: credentials are rewritten on every password
// change, policy is read on every request and changes rarely. The split is what lets the per-request
// resolver cache policy aggressively while never holding a password hash in memory (015 D:"Storage is a
// NEW singleton"). It is also why `Disabled` and the effective role list live HERE and not on
// UserRecord — a resolver that had to read UserRecord to learn a user is disabled would be caching
// exactly the thing the split exists to avoid.

/// <summary>Allow or Deny, with deny-overrides at evaluation time.</summary>
[GenerateSerializer]
public enum PermissionEffect { Allow = 0, Deny = 1 }

/// <summary>The decision is tri-state, not boolean. <see cref="RequiresApproval"/> is load-bearing all
/// the way to the SPA button label ("Request approval…"); retrofitting it later means touching every
/// call site twice.</summary>
[GenerateSerializer]
public enum AccessDecision { Denied = 0, Allowed = 1, RequiresApproval = 2 }

/// <summary>One entitlement. <see cref="Action"/> is a flat dotted string with <c>*</c> wildcards
/// (<c>pipeline.update</c>, <c>source.*</c>, <c>*</c>) — one string is what a policy name, a claim, an
/// audit row and a client <c>can()</c> all want. <see cref="Scope"/> is <c>*</c> | an exact id/name |
/// a prefix (<c>prod-*</c>) | a tag (<c>tag:finance</c>); all three entity types already carry Tags, so
/// tag scoping costs nothing new.</summary>
[GenerateSerializer]
public sealed class PermissionGrant
{
    [Id(0)] public string Action { get; set; } = "";
    [Id(1)] public string Scope { get; set; } = "*";
    [Id(2)] public PermissionEffect Effect { get; set; } = PermissionEffect.Allow;
    /// <summary>Allow-only. A Deny that "requires approval" is a contradiction and the evaluator
    /// ignores the flag on a Deny rather than inventing a fourth state.</summary>
    [Id(3)] public bool RequiresApproval { get; set; }
    [Id(4)] public string? Note { get; set; }
}

/// <summary>A named bundle of grants. The three legacy roles ship as built-ins whose grant sets
/// reproduce today's behaviour exactly, so an untouched catalog behaves identically before and
/// after.</summary>
[GenerateSerializer]
public sealed class RoleDefinition
{
    [Id(0)] public string Name { get; set; } = "";
    [Id(1)] public string Description { get; set; } = "";
    [Id(2)] public List<PermissionGrant> Grants { get; set; } = [];
    /// <summary>Built-in roles may be edited but not deleted — deleting Viewer would strand every
    /// pre-upgrade token.</summary>
    [Id(3)] public bool BuiltIn { get; set; }
    [Id(4)] public long UpdatedAtMs { get; set; }
    [Id(5)] public string UpdatedBy { get; set; } = "";
}

/// <summary>Groups carry roles and grants, and <b>membership lives on the group</b>: "who is in this
/// group" is the common query, and the user list is already rewritten whole on every mutation — a
/// second whole-list-rewrite path would double the write-conflict surface on the hottest singleton in
/// the system.</summary>
[GenerateSerializer]
public sealed class GroupDefinition
{
    [Id(0)] public string Name { get; set; } = "";
    [Id(1)] public string Description { get; set; } = "";
    [Id(2)] public List<string> Members { get; set; } = [];
    [Id(3)] public List<string> Roles { get; set; } = [];
    [Id(4)] public List<PermissionGrant> Grants { get; set; } = [];
    /// <summary>OIDC seam (deferred to its own plan): values of the IdP's <c>groups</c> claim that map
    /// onto this group. The resolver takes membership from BOTH the store and the claim from day one,
    /// so when OIDC lands the mapping is already implemented and tested.</summary>
    [Id(5)] public List<string> ExternalClaimValues { get; set; } = [];
    [Id(6)] public long CreatedAtMs { get; set; }
    [Id(7)] public long UpdatedAtMs { get; set; }
    [Id(8)] public string UpdatedBy { get; set; } = "";
}

/// <summary>Per-user policy: the half of a user that authorization reads. Kept out of
/// <see cref="UserRecord"/> on purpose — see the file header.
///
/// <para><see cref="Roles"/> is the effective role list. The user store MIRRORS
/// <c>UserRecord.Role</c> here on every create/update (LegacyRoleMigration does it once for an existing
/// data dir), which is what makes a role change take effect within the resolver's TTL instead of at the
/// next login: the evaluator uses this list and falls back to the token's role claim only when no entry
/// exists (a pre-upgrade catalog).</para></summary>
[GenerateSerializer]
public sealed class UserAccessEntry
{
    [Id(0)] public string Username { get; set; } = "";
    /// <summary>The cheap 90% of token revocation: the resolver returns an empty grant set for a
    /// disabled user, so disabling kills a live token within <c>Auth:PolicyCacheSeconds</c> without a
    /// revocation list. A JTI denylist is deferred.</summary>
    [Id(1)] public bool Disabled { get; set; }
    [Id(2)] public List<string> Roles { get; set; } = [];
    [Id(3)] public List<PermissionGrant> Grants { get; set; } = [];
    [Id(4)] public long UpdatedAtMs { get; set; }
    [Id(5)] public string UpdatedBy { get; set; } = "";
}

/// <summary>Which actions need a second pair of eyes, for whom, and for how long. Templates ship
/// seeded but inert (<c>Approvals:Enabled=false</c>), so existing deployments are byte-identical.</summary>
[GenerateSerializer]
public sealed class ApprovalTemplate
{
    [Id(0)] public string Name { get; set; } = "";
    [Id(1)] public string ActionPattern { get; set; } = "";
    [Id(2)] public string ScopePattern { get; set; } = "*";
    /// <summary>The N of N-of-M. M is however many people the approver groups contain.</summary>
    [Id(3)] public int RequiredApprovals { get; set; } = 1;
    [Id(4)] public List<string> ApproverGroups { get; set; } = [];
    [Id(5)] public int ExpiresAfterSeconds { get; set; } = 3600;
    /// <summary>0 = never escalate.</summary>
    [Id(6)] public int EscalateAfterSeconds { get; set; }
    [Id(7)] public List<string> EscalationGroups { get; set; } = [];
    [Id(8)] public bool Enabled { get; set; } = true;
}

[GenerateSerializer]
public enum ApprovalState { Pending = 0, Approved = 1, Rejected = 2, Expired = 3, Executed = 4, Failed = 5, Cancelled = 6 }

[GenerateSerializer]
public sealed class ApprovalVote
{
    [Id(0)] public string Username { get; set; } = "";
    [Id(1)] public bool Approve { get; set; }
    [Id(2)] public long AtMs { get; set; }
    [Id(3)] public string? Comment { get; set; }
}

/// <summary>A privileged action parked until enough people say yes.
///
/// <para><see cref="PayloadJson"/> is the request that would have executed, serialized. Re-executing
/// from it is deliberately the only replay mechanism: nothing about the original HTTP request is
/// retained, so an approved request cannot smuggle in a header or a claim it did not have when it was
/// filed.</para></summary>
[GenerateSerializer]
public sealed class ApprovalRequest
{
    [Id(0)] public string Id { get; set; } = "";
    [Id(1)] public string RequestedBy { get; set; } = "";
    [Id(2)] public long RequestedAtMs { get; set; }
    [Id(3)] public string Action { get; set; } = "";
    [Id(4)] public string Scope { get; set; } = "";
    [Id(5)] public string Reason { get; set; } = "";
    [Id(6)] public string TemplateName { get; set; } = "";
    [Id(7)] public int RequiredApprovals { get; set; } = 1;
    [Id(8)] public List<ApprovalVote> Votes { get; set; } = [];
    [Id(9)] public ApprovalState State { get; set; } = ApprovalState.Pending;
    [Id(10)] public long ExpiresAtMs { get; set; }
    [Id(11)] public long? EscalatedAtMs { get; set; }
    [Id(12)] public string? PayloadJson { get; set; }
    [Id(13)] public string? Outcome { get; set; }
    [Id(14)] public long? DecidedAtMs { get; set; }
    /// <summary>"rest" | "chat" | "grpc" — an LLM-proposed action must be visibly distinguishable from
    /// a human-typed one in the inbox, not only in the audit log.</summary>
    [Id(15)] public string Origin { get; set; } = "rest";
    [Id(16)] public List<string> ApproverGroups { get; set; } = [];
}

/// <summary>One thing that happened. Day-sharded storage (<c>audit:{yyyyMMdd}</c>) so a day activates
/// only when written to or read, the mechanism plan 011-D1 established for TableShardGrain.</summary>
[GenerateSerializer]
public sealed class AuditEntry
{
    [Id(0)] public string Id { get; set; } = "";
    [Id(1)] public long AtMs { get; set; }
    [Id(2)] public string Actor { get; set; } = "";
    [Id(3)] public string Action { get; set; } = "";
    [Id(4)] public string Scope { get; set; } = "";
    /// <summary>"allowed" | "denied" | "requires-approval" | "executed" | "failed".</summary>
    [Id(5)] public string Outcome { get; set; } = "";
    [Id(6)] public string? Detail { get; set; }
    /// <summary>Set when the chat acted: Actor is the model, OnBehalfOf the human whose token it
    /// carried. The one place an LLM's action is attributed, and it must never collapse into one
    /// field.</summary>
    [Id(7)] public string? OnBehalfOf { get; set; }
    [Id(8)] public string? ApprovalId { get; set; }
    [Id(9)] public string? BeforeJson { get; set; }
    [Id(10)] public string? AfterJson { get; set; }
    [Id(11)] public string Origin { get; set; } = "rest";
}

/// <summary><see cref="Truncated"/> is persisted and never reset, so drop-oldest silence is never
/// mistaken for absence.</summary>
[GenerateSerializer]
public sealed class AuditPage
{
    [Id(0)] public List<AuditEntry> Entries { get; set; } = [];
    [Id(1)] public long Truncated { get; set; }
    [Id(2)] public int Total { get; set; }
}

/// <summary>Everything the authorization decision reads, in one versioned document behind one
/// singleton. <see cref="Version"/> is monotonic and bumped on every mutation; the resolver polls
/// <c>GetVersionAsync()</c> on a TTL and refetches only when it moves.</summary>
[GenerateSerializer]
public sealed class AccessPolicyDocument
{
    [Id(0)] public List<RoleDefinition> Roles { get; set; } = [];
    [Id(1)] public List<GroupDefinition> Groups { get; set; } = [];
    [Id(2)] public List<UserAccessEntry> Users { get; set; } = [];
    [Id(3)] public List<ApprovalTemplate> ApprovalTemplates { get; set; } = [];
    [Id(4)] public long Version { get; set; }
    [Id(5)] public long UpdatedAtMs { get; set; }
}

/// <summary>What the resolver hands the evaluator: one user's flattened, version-stamped view.</summary>
[GenerateSerializer]
public sealed class EffectivePermissions
{
    [Id(0)] public string Username { get; set; } = "";
    [Id(1)] public bool Disabled { get; set; }
    [Id(2)] public List<string> Roles { get; set; } = [];
    [Id(3)] public List<string> Groups { get; set; } = [];
    [Id(4)] public List<PermissionGrant> Grants { get; set; } = [];
    [Id(5)] public long Version { get; set; }
}

/// <summary>The permission grammar's vocabulary. Flat dotted strings, one per REST verb-ish operation
/// the platform already exposes — deliberately NOT a per-field taxonomy (cut, explicitly).</summary>
public static class Actions
{
    public const string SourceRead = "source.read";
    public const string SourceWrite = "source.write";
    public const string SourceDelete = "source.delete";
    public const string SourceIngest = "source.ingest";
    public const string SourceRun = "source.run";

    public const string PipelineRead = "pipeline.read";
    public const string PipelineWrite = "pipeline.write";
    public const string PipelineDelete = "pipeline.delete";
    public const string PipelineControl = "pipeline.control";

    public const string TableRead = "table.read";
    public const string TableWrite = "table.write";
    public const string TableDelete = "table.delete";
    public const string TableControl = "table.control";

    public const string ConfigExport = "config.export";
    public const string ConfigReplace = "config.replace";

    public const string UserRead = "user.read";
    public const string UserWrite = "user.write";

    public const string AccessRead = "access.read";
    public const string AccessWrite = "access.write";

    public const string AuditRead = "audit.read";

    public const string ApprovalRequest = "approval.request";
    public const string ApprovalDecide = "approval.decide";
    /// <summary>Held by nobody by default. Its whole purpose is to be conspicuous in an audit row.</summary>
    public const string ApprovalBypass = "approval.bypass";

    public const string ChatUse = "chat.use";

    /// <summary>Legacy-equivalent bundles, used by the built-in role seeds. `catalog.write` is the
    /// permission the "Editor" ASP.NET policy is satisfied by, which is what keeps all 59
    /// RequireAuthorization sites compiling unchanged.</summary>
    public const string CatalogWrite = "catalog.write";
    public const string CatalogRead = "catalog.read";
}

public static class BuiltInRoles
{
    public const string Admin = "Admin";
    public const string Editor = "Editor";
    public const string Viewer = "Viewer";

    public static readonly string[] All = [Admin, Editor, Viewer];
}

namespace StreamForge.Abstractions;

public static class StreamConstants
{
    public const string ProviderName = "streams";
    public const string SourcesNamespace = "sources";
    public const string OutputNamespace = "pipeline-out";
    public const string TableDeltaNamespace = "table-delta";
    public const string LifecycleNamespace = "lifecycle";
    public const string LifecycleEventsKey = "events";
    public const string MetricsKey = "metrics";
    public const string StorageName = "definitions";
    public const string PubSubStoreName = "PubSubStore";
    public const string RegistryKey = "catalog";
    public const string UsersKey = "users";

    // Plan 015. AccessKey is the access-policy singleton (Orleans AccessPolicyGrain / Dapr
    // AccessPolicyActor) — a SEPARATE store from UsersKey on purpose: policy is read on every request
    // and cached hard, credentials are rewritten on every password change and never cached.
    public const string AccessKey = "access";
    /// <summary>Approvals are one singleton, not day-sharded: the pending set is small and is queried
    /// as a whole by the inbox and by the escalation sweeper.</summary>
    public const string ApprovalsKey = "approvals";
    /// <summary>Audit is day-sharded — <c>audit:20260819</c> — so a day activates only when written to
    /// or read and is evicted when idle, the mechanism plan 011-D1 established for TableShardGrain.</summary>
    public const string AuditKeyPrefix = "audit:";

    /// <summary>The key an audit entry with this timestamp belongs to. UTC, because a day boundary that
    /// moves with the host's locale would silently split or merge shards on a redeploy.</summary>
    public static string AuditKeyFor(long atMs) =>
        AuditKeyPrefix + DateTimeOffset.FromUnixTimeMilliseconds(atMs).UtcDateTime.ToString("yyyyMMdd");
}

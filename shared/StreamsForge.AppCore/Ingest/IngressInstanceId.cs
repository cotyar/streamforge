namespace StreamsForge.AppCore.Ingest;

/// <summary>
/// Plan 009 A1.3 — "every host stamps its own instance id" (<c>IngestStatus.InstanceId</c>'s own doc
/// comment). One value per process lifetime, computed once and reused by every
/// <c>IIngressFacade.GetStatusAsync</c> call on this host — cheap, stable for the life of the process,
/// and distinguishable across replicas without needing any cluster coordination (which is exactly the
/// point: it labels a per-replica view, it doesn't require one).
/// </summary>
public static class IngressInstanceId
{
    /// <summary>Machine name plus a short random suffix — the suffix is what actually disambiguates
    /// two replicas on the same host (e.g. two local test instances on different ports), since
    /// machine name alone collides for those.</summary>
    public static readonly string Value = $"{Environment.MachineName}-{Guid.NewGuid().ToString("N")[..8]}";
}

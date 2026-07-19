namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 W4: wraps an actor method's result when the underlying <see cref="Catalog.CatalogStore"/>
/// call can throw <see cref="InvalidOperationException"/> (name collisions, unsupported Parallelism,
/// running-dependent guards — see CatalogStore's doc comment on each). RegistryActor catches the
/// exception and returns <see cref="Failure"/> instead of letting it cross the actor-invocation boundary
/// as a thrown exception.
///
/// <para><b>Why not just let the exception cross the wire:</b> the Dapr .NET actor client wraps a
/// server-side exception in its own exception type; nothing in this plan wants the shared endpoints
/// (StreamForge.Api's TablesEndpoints etc., written once for both runtimes) to depend on exactly how/
/// whether the Dapr SDK reconstructs the original CLR exception type across that boundary. Using an
/// explicit result type here means the ONLY place that needs to know about
/// <see cref="InvalidOperationException"/> at all is <see cref="Catalog.CatalogStore"/> (which throws
/// it, ported verbatim from RegistryGrain) and <see cref="Facades.DaprCatalogFacade"/> (which re-throws
/// it client-side from <see cref="Error"/>) — so the shared endpoints' existing
/// <c>catch (InvalidOperationException)</c> → 409 pathway fires identically on both flavors, guaranteed
/// by this project's own code rather than by an SDK implementation detail.</para>
/// </summary>
public sealed record ActorResult<T>(bool Ok, T? Value, string? Error)
{
    public static ActorResult<T> Success(T value) => new(true, value, null);

    public static ActorResult<T> Failure(string error) => new(false, default, error);
}

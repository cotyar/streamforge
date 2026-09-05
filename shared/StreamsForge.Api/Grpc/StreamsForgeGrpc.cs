using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using StreamsForge.Api;
using StreamsForge.Host.Grpc.Dynamic;

namespace StreamsForge.Host.Grpc;

/// <summary>
/// Plan 025 G1 — the two lines a host needs to serve the whole gRPC surface, and the ONE list that says
/// what that surface is.
///
/// <para><b>Why a shared pair of extension methods rather than seven <c>MapGrpcService</c> calls per
/// host.</b> Before this, the Orleans <c>Program.cs</c> owned the mapping list AND a hand-written copy of
/// the same service names in its <see cref="StreamsForgeApiOptions.GrpcStaticServices"/> literal. Two
/// copies of one fact, in one file, is survivable; four copies across two hosts is how a service gets
/// mapped on one flavor and advertised on neither. <see cref="StaticServiceNames"/> is now the single
/// definition, <see cref="MapStreamsForgeGrpc"/> the single mapping, and each host passes the former
/// straight into its options record — so <c>GET /api/meta/instance</c>'s <c>capabilities</c> and
/// <c>endpoints.grpc</c> cannot claim a service the host does not map.</para>
///
/// <para><b>The name list is the REFLECTION list, not the C# class list.</b> The strings are the gRPC
/// service names a reflection client (grpcurl's <c>list</c>) sees — <c>ServerReflection</c> for the
/// hand-rolled <see cref="DynamicReflectionService"/>, and no entry at all for helpers. It is what the
/// API Explorer renders and what plan 016's <c>servesGrpc</c> check counts, so it must stay in the same
/// order and spelling the Orleans host used before this move: any change there is a visible API
/// change.</para>
/// </summary>
public static class StreamsForgeGrpc
{
    /// <summary>The fixed static gRPC service names this assembly serves, in the order the Orleans host
    /// has advertised them since plan 004. Passed verbatim into
    /// <c>StreamsForgeApiOptions.GrpcStaticServices</c> by both hosts.</summary>
    public static readonly IReadOnlyList<string> StaticServiceNames =
    [
        "SourceService", "PipelineService", "TableService", "StreamService", "IngestService", "DynamicStreamService", "ServerReflection",
    ];

    /// <summary>gRPC server services. Nothing else: the seven service CLASSES are instantiated per call
    /// by the ASP.NET Core gRPC framework straight from the request's service provider, so they are not
    /// registered here — only their dependencies are (<c>ICatalogFacade</c>, <c>ITableReadFacade</c>,
    /// <c>IIngressFacade</c>, <c>AccessGuard</c> from <c>AddStreamsForgeApi</c>/the flavor's own facade
    /// registration, plus <see cref="StreamsForge.Abstractions.IEntityStreamFacade"/>, which is the one
    /// thing each host must register itself because it is the only genuinely runtime-specific
    /// dependency).</summary>
    public static IServiceCollection AddStreamsForgeGrpc(this IServiceCollection services)
    {
        services.AddGrpc();
        return services;
    }

    /// <summary>Maps all seven services. Tier 1 is the static control plane + streaming
    /// (<c>Protos/streamsforge.proto</c>); tier 2 is server reflection over BOTH the static descriptors
    /// and per-entity descriptors generated on the fly for the current catalog (see
    /// <see cref="DynamicReflectionService"/> for why this replaces the built-in
    /// <c>Grpc.AspNetCore.Server.Reflection</c> package) plus one generic typed-streaming RPC
    /// (<see cref="DynamicStreamService"/>) whose row payloads are encoded against those
    /// descriptors.</summary>
    public static IEndpointRouteBuilder MapStreamsForgeGrpc(this IEndpointRouteBuilder app)
    {
        app.MapGrpcService<SourceGrpcService>();
        app.MapGrpcService<PipelineGrpcService>();
        app.MapGrpcService<TableGrpcService>();
        app.MapGrpcService<StreamGrpcService>();
        app.MapGrpcService<IngestGrpcService>();
        app.MapGrpcService<DynamicReflectionService>();
        app.MapGrpcService<DynamicStreamService>();
        return app;
    }
}

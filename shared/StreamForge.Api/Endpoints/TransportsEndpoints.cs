using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.AppCore.Sinks;
using StreamForge.AppCore.Transports;

namespace StreamForge.Api;

/// <summary>
/// Plan 010 (console wave): <c>GET /api/transports</c> — the registered transports and their form
/// descriptors, so the SPA renders a config editor for a transport it has never heard of.
///
/// <para>This is the last of the places that used to need editing per transport. With it, the cost of a new
/// transport is one <see cref="IInboundTransport"/> (and/or one <see cref="ISinkTransport"/>) plus one line
/// in the registry — validation, secret masking, both connector drivers, both publisher services and now the
/// console all derive from that.</para>
///
/// <para>Viewer, like every other read: a descriptor is field metadata, never a value. Secrets never appear
/// here — the SPA learns that a field IS a secret (so it renders masked and honors the "*** keeps the stored
/// value" rule), not what it contains.</para>
/// </summary>
public static class TransportsEndpoints
{
    public static void MapTransportsEndpoints(this WebApplication app)
    {
        app.MapGet("/api/transports", () => Results.Ok(new TransportCatalog(
                Inbound: [.. InboundTransports.Kinds.Select(k => InboundTransports.Find(k)!.Describe())],
                Outbound: [.. SinkTransports.Kinds.Select(k => SinkTransports.Find(k)!.Describe())])))
            .RequireAuthorization("Viewer");
    }
}

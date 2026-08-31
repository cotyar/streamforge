using StreamsForge.Abstractions;
using StreamsForge.AppCore.Transports;

namespace StreamsForge.AppCore.Sinks;

/// <summary>
/// Wishlist item 9(a): HTTP as an <see cref="ISinkTransport"/>. All the publish work is
/// <see cref="HttpSinkClient"/>'s — read its class doc first; this type only says which kind it serves,
/// what counts as configured, how to construct one, the console form, and (new relative to
/// <see cref="NatsSinkTransport"/>/<see cref="FileSinkTransport"/>, which predate this seam and so still
/// use <see cref="ISinkTransport.Validate"/>'s default no-op) an actual <see cref="Validate"/> — see that
/// method's doc for why this kind implements it rather than staying silent like its two older siblings.
/// </summary>
public sealed class HttpSinkTransport : ISinkTransport
{
    public string Kind => SinkKinds.Http;

    public bool IsConfigured(SinkSpec spec) => spec.Http is { } h && !string.IsNullOrWhiteSpace(h.Url);

    public ISinkClient Create(SinkSpec spec, string entityKind, string entityName, Action<string, Exception>? onFailure) =>
        new HttpSinkClient(spec.Http!, entityKind, entityName, onFailure);

    /// <summary>"The URL comes from config and a missing one is a validation error" (this wave's own
    /// brief) — <see cref="IsConfigured"/> alone only makes a blank URL silently never run, which is the
    /// exact gap <see cref="ISinkTransport.Validate"/>'s doc comment names ("a broken config is silently
    /// <c>IsConfigured</c> == false … no error surfaces anywhere"). This is a NEW sink kind, so — per that
    /// same doc comment — it is the one free to actually implement the seam instead of inheriting the
    /// default no-op <see cref="NatsSinkTransport"/>/<see cref="FileSinkTransport"/> still use.
    /// <b>Honest limit, restated from that doc comment:</b> nothing in this repo calls
    /// <see cref="ISinkTransport.Validate"/> today — wiring a REST call site so this message actually
    /// reaches an operator is explicitly out of scope for the wave that added the seam, and stays out of
    /// scope here too (it would mean touching <c>TablesEndpoints.cs</c>/<c>PipelinesEndpoints.cs</c>,
    /// neither of which this wave owns). This method is the correct, complete answer to "a missing URL is
    /// a validation error" for the seam as it exists today; surfacing it in the console is the same later
    /// wave's job the seam's own author already deferred it to.</summary>
    public void Validate(SinkSpec spec, List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(spec.Http?.Url))
        {
            errors.Add("kind 'http' requires http.url");
        }
    }

    public TransportDescriptor Describe() => new()
    {
        Kind = SinkKinds.Http,
        Version = "1.0.0", // plan 016 wave 4: explicit contract version — see TransportDescriptor.Version.
        Label = "HTTP",
        Help = "Fire-and-forget: POSTs one JSON event per row/delta. A slow or unreachable endpoint drops rather than slowing the entity down. This is the 'scenario clock' feedback loop's own transport (wishlist #9) — maxDepth bounds it.",
        ConfigProperty = "http",
        Groups =
        [
            new TransportGroup
            {
                Key = "auth",
                Label = "Header",
                Help = "Optional. Sent only when both a name and a value are set — e.g. name 'X-SF-Ingest-Key' for looping back into this same host's own ingest endpoint, or name 'Authorization' with value 'Bearer …' for a third-party receiver.",
            },
            new TransportGroup
            {
                Key = "guard",
                Label = "Loop guard",
                Help = "The bounded-feedback-loop cycle-breaker (wishlist #9). Normally the loop's own SQL (WHERE step < D) terminates it; this is a backstop, off by default.",
            },
        ],
        Fields =
        [
            new TransportField
            {
                Key = "url", Label = "URL", Required = true, Mono = true,
                Placeholder = "https://host/api/sources/{name}/events",
                Help = "{name} is replaced with this pipeline's id / table's name, so one spec can serve a whole catalog.",
            },
            new TransportField { Key = "headerName", Label = "Header name", Group = "auth", Placeholder = "X-SF-Ingest-Key" },
            new TransportField { Key = "headerValue", Label = "Header value", Type = TransportFieldTypes.Secret, Group = "auth" },
            new TransportField
            {
                Key = "timeoutMs", Label = "Timeout (ms)", Type = TransportFieldTypes.Number, Default = "3000",
            },
            new TransportField
            {
                Key = "stepField", Label = "Step field", Group = "guard", Mono = true, Default = "step",
                Help = "Row field the loop guard reads its step counter from.",
            },
            new TransportField
            {
                Key = "maxDepth", Label = "Max depth", Type = TransportFieldTypes.Number, Group = "guard", Default = "0",
                Help = "Rows whose step field is >= this are dropped and counted as a sink failure (see LastError). 0 disables the guard.",
            },
        ],
    };
}

using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.AppCore.Sinks;

/// <summary>
/// Wishlist item 9(b): loopback as an <see cref="ISinkTransport"/>. All the publish work is
/// <see cref="LoopbackSinkClient"/>'s — read its class doc, and <see cref="StreamForge.Host.Generators.LoopbackHub"/>'s,
/// first. This type only says which kind it serves, what counts as configured, how to construct one, the
/// console form, and — like <see cref="HttpSinkTransport"/> (a NEW sink kind, so free of the "existing
/// kinds keep the no-op default" reasoning <see cref="ISinkTransport.Validate"/>'s own doc comment
/// states) — an actual <see cref="Validate"/>.
/// </summary>
public sealed class LoopbackSinkTransport : ISinkTransport
{
    public string Kind => SinkKinds.Loopback;

    public bool IsConfigured(SinkSpec spec) => spec.Loopback is { } l && !string.IsNullOrWhiteSpace(l.TargetSourceName);

    public ISinkClient Create(SinkSpec spec, string entityKind, string entityName, Action<string, Exception>? onFailure) =>
        new LoopbackSinkClient(spec.Loopback!, entityKind, entityName, onFailure);

    /// <summary>Same reasoning as <see cref="HttpSinkTransport.Validate"/>: a missing target is a
    /// validation error, not a silently-never-configured sink. Same restated honest limit too — nothing
    /// in this repo calls <see cref="ISinkTransport.Validate"/> from a REST call site today (see that
    /// interface member's own doc comment); wiring that is out of scope here, same as it was for 9(a).</summary>
    public void Validate(SinkSpec spec, List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(spec.Loopback?.TargetSourceName))
        {
            errors.Add("kind 'loopback' requires loopback.targetSourceName");
        }
    }

    public TransportDescriptor Describe() => new()
    {
        Kind = SinkKinds.Loopback,
        Version = "1.0.0", // plan 016 wave 4: explicit contract version — see TransportDescriptor.Version.
        Label = "Loopback (in-process)",
        Help = "In-process only: writes each row/delta directly into the target source's own generator — no HTTP, no network, no serialize/parse round trip. The target must be a generator-kind source that has been started. This is the 'scenario clock' feedback loop's native transport (wishlist #9(b)) — maxDepth bounds it exactly like the HTTP sink's guard.",
        ConfigProperty = "loopback",
        Groups =
        [
            new TransportGroup
            {
                Key = "guard",
                Label = "Loop guard",
                Help = "The bounded-feedback-loop cycle-breaker (wishlist #9). Normally the loop's own SQL (WHERE step < D) terminates it; this is a backstop, off by default. Identical semantics to the HTTP sink's own guard.",
            },
        ],
        Fields =
        [
            new TransportField
            {
                Key = "targetSourceName", Label = "Target source", Required = true, Mono = true,
                Placeholder = "{name}",
                Help = "{name} is replaced with this pipeline's id / table's name, so one spec can serve a whole catalog. Must name a generator-kind source that is currently started.",
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

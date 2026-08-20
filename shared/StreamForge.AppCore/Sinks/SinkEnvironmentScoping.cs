using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using StreamForge.AppCore.Environments;

namespace StreamForge.AppCore.Sinks;

/// <summary>
/// Plan 021 wave 2 — the two sink kinds that name a CATALOG ENTITY rather than an external endpoint, and
/// therefore have to be read in the environment they were authored in.
///
/// <para><b>The bug this closes was silent and it wrote across the boundary.</b> A <c>loopback</c> sink
/// names a target source (<c>LoopbackHub</c>); a <c>duplex</c> sink names a duplex-kind source
/// (<c>DuplexSessions</c>). Both registries are keyed by the entity's RUNTIME key, which wave 2 qualified
/// by environment — but the sink's config still held the bare catalog name. So a table in <c>staging</c>
/// with a loopback sink to <c>feed</c> published to the bare key <c>feed</c>, which is exactly the key
/// <c>default</c>'s own generator is attached at: staging's rows were injected into default's source, and
/// the publish REPORTED SUCCESS. Not a missing feature — a working cross-environment write.
///
/// <para><b>Resolved here, at client construction, and never written back</b> — the same rule plan 016
/// gave <c>@name</c> endpoints: the catalog keeps what the author wrote, an export from one environment
/// stays importable into another, and only the live client sees the qualified name. The default
/// environment returns the SAME instance, not a clone, so nothing about an untouched deployment changes
/// (plan D2) and no allocation is added to the refresh sweep.</para>
///
/// <para><b>Deliberately not extended to the other sink kinds.</b> <c>nats</c>/<c>http</c>/<c>db</c>/
/// <c>file</c> address something OUTSIDE this process — a subject, a URL, a table, a path — and an
/// environment has no opinion about those. Qualifying them would rename an operator's Kafka subject or
/// their database table out from under them. It also means <see cref="ISinkTransport"/> keeps its
/// signature, so an out-of-tree sink plugin written against plan 014-B's SPI still compiles.</para>
/// </summary>
public static class SinkEnvironmentScoping
{
    /// <summary>The spec a sink client should actually be built from, for an entity living in
    /// <paramref name="environment"/>. Returns <paramref name="spec"/> itself for the default environment
    /// and for any spec with nothing to scope.</summary>
    public static SinkSpec Scope(SinkSpec spec, string? environment)
    {
        if (string.IsNullOrEmpty(environment))
        {
            return spec;
        }

        var loopback = spec.Loopback?.TargetSourceName;
        var duplex = spec.Duplex?.SourceName;
        if (string.IsNullOrEmpty(loopback) && string.IsNullOrEmpty(duplex))
        {
            return spec;
        }

        // Cloned, because the caller's SinkSpec came off the catalog definition and is shared with
        // everything else reading that entity — mutating it in place would qualify the name the console
        // renders and the exporter writes.
        var scoped = ConfigJsonMapper.DeepCloneModel(spec);
        if (scoped.Loopback is { } l && !string.IsNullOrEmpty(l.TargetSourceName))
        {
            // Qualified BEFORE "{name}" is expanded (LoopbackSinkClient does that): "{name}_loop" becomes
            // "staging.{name}_loop" and then "staging.orders_loop", which is the key the generator grain
            // for staging's orders_loop is actually attached at.
            l.TargetSourceName = EnvKeys.Qualify(environment, l.TargetSourceName);
        }

        if (scoped.Duplex is { } d && !string.IsNullOrEmpty(d.SourceName))
        {
            d.SourceName = EnvKeys.Qualify(environment, d.SourceName);
        }

        return scoped;
    }
}

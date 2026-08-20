using StreamForge.Abstractions;
using StreamForge.AppCore.Discovery;

namespace StreamForge.AppCore.Config;

/// <summary>
/// Plan 016 wave 6 — the IMPORT-side half of named external endpoints. See
/// <see cref="NamedEndpoints"/>'s class doc for the whole feature; this file is the one piece of it
/// that runs at IMPORT time rather than CONNECT time, and the two deliberately disagree about what an
/// unresolvable <c>@name</c> means: <see cref="NamedEndpoints.Resolve"/> throws (there is nothing to
/// dial), <see cref="Scan"/> only ever WARNS (there is nothing to apply differently — the document still
/// imports byte-identical, exactly as authored, with <c>@name</c> untouched). That asymmetry is the
/// plan's explicit decision, restated here because it is the reason this whole class exists: "importing
/// a document destined for another environment must remain possible — that is what promotion is."
///
/// <para><b>Pure and runtime-free</b> — like the rest of <c>StreamForge.AppCore/Config</c>, this walks
/// plain data (a <see cref="ConfigDocument"/>) and returns plain data, so it needs no catalog access and
/// no HTTP context. <see cref="StreamForge.Api"/>'s <c>ConfigImportService.RunImportAsync</c> is the only
/// caller today: it runs this scan once per import (both a real apply AND <c>mode=validate</c> — the two
/// share one code path there, so this needs no separate validate-mode entry point) and folds each result
/// into the matching entity's report entry as an extra diagnostic line, WITHOUT ever setting that entry's
/// <c>Action</c> to <c>"error"</c> — see that file for exactly where.</para>
///
/// <para><b>Fields covered</b> (the plan's own starting list, exhaustively resolved against
/// <c>StreamForge.Contracts.ConnectorModels</c> rather than left as a starting point): a source's
/// <c>Connector.Url.Url</c> (<see cref="UrlPollConfig"/>), <c>Connector.Nats.Url</c>
/// (<see cref="NatsSubConfig"/>), <c>Connector.Grpc.Address</c> / <c>.RestAddress</c>
/// (<see cref="GrpcSubConfig"/>), <c>Connector.Db.Host</c> / <c>.ConnectionString</c>
/// (<see cref="DbSourceConfig"/>), and <c>Connector.Fix.Host</c> (<see cref="FixSourceConfig"/> — the
/// counterparty this platform dials as FIX initiator, the same "host to connect to" shape as the others,
/// even though the plan's own list didn't name it); plus every pipeline/table sink's
/// <c>Nats.Url</c> (<see cref="NatsPubConfig"/>), <c>Http.Url</c> (<see cref="HttpSinkConfig"/>), and
/// <c>Db.Host</c> / <c>.ConnectionString</c> (<see cref="DbSinkConfig"/>).
///
/// <para><b>Deliberately NOT covered:</b> <see cref="FilePollConfig.Path"/> / <see cref="FolderPollConfig.Path"/>
/// / <see cref="FileSinkConfig.Path"/> — a local filesystem path, not a network endpoint; there is
/// nothing environment-relative about a host's own disk that <c>@name</c> indirection helps with, and
/// the plan's own list never mentions them. <see cref="DuplexSinkConfig.SourceName"/> /
/// <see cref="LoopbackSinkConfig.TargetSourceName"/> — these name a CATALOG entity (another source in
/// the same instance), not an external endpoint; they resolve through the catalog, not
/// <see cref="NamedEndpoints"/>. <see cref="GrpcSubConfig.Peer"/> — plan 016 wave 5's own indirection
/// (a configured peer name resolved via <c>PeerDirectory</c>), a sibling mechanism to this one, not
/// this one; when it is set, <see cref="GrpcSubConfig.Address"/>/<see cref="GrpcSubConfig.RestAddress"/>
/// are typically blank, which this scan already treats as "not a reference" and skips.</para>
/// </summary>
public static class EndpointReferenceWarnings
{
    /// <summary>One entity's one unresolvable field. <see cref="Kind"/>/<see cref="Name"/> match a
    /// <c>ConfigImportReportEntry</c>'s own <c>Kind</c>/<c>Name</c> exactly, so the caller can fold this
    /// straight into that entry's <c>Diagnostics</c> by a simple key lookup — no re-derivation of what
    /// "the entity this warning is about" means.</summary>
    public readonly record struct EndpointWarning(string Kind, string Name, string Message);

    /// <summary>Every reference this instance cannot resolve, across the whole document. Empty means
    /// either the document has no <c>@name</c> references at all, or every one of them resolves here.</summary>
    public static List<EndpointWarning> Scan(ConfigDocument doc)
    {
        var warnings = new List<EndpointWarning>();
        foreach (var source in doc.Sources)
        {
            ScanSource(source, warnings);
        }

        foreach (var pipeline in doc.Pipelines)
        {
            ScanSinks("pipeline", pipeline.Name, pipeline.Sinks, warnings);
        }

        foreach (var table in doc.Tables)
        {
            ScanSinks("table", table.Name, table.Sinks, warnings);
        }

        return warnings;
    }

    private static void ScanSource(SourceDefinition source, List<EndpointWarning> warnings)
    {
        var c = source.Connector;
        if (c is null)
        {
            return;
        }

        Check("source", source.Name, "connector.url.url", c.Url?.Url, warnings);
        Check("source", source.Name, "connector.nats.url", c.Nats?.Url, warnings);
        Check("source", source.Name, "connector.grpc.address", c.Grpc?.Address, warnings);
        Check("source", source.Name, "connector.grpc.restAddress", c.Grpc?.RestAddress, warnings);
        Check("source", source.Name, "connector.db.host", c.Db?.Host, warnings);
        Check("source", source.Name, "connector.db.connectionString", c.Db?.ConnectionString, warnings);
        Check("source", source.Name, "connector.fix.host", c.Fix?.Host, warnings);
    }

    private static void ScanSinks(string entityKind, string entityName, List<SinkSpec> sinks, List<EndpointWarning> warnings)
    {
        foreach (var sink in sinks)
        {
            // A sink's own Name when it has one (the INSERT INTO target — see SinkSpec.Name's doc
            // comment), else its Kind — either way, a label that lets an operator find the right sink
            // among several on the same entity without inventing a positional index that would shift if
            // the document's sink list is ever reordered.
            var label = string.IsNullOrEmpty(sink.Name) ? sink.Kind : sink.Name;
            Check(entityKind, entityName, $"sinks[{label}].nats.url", sink.Nats?.Url, warnings);
            Check(entityKind, entityName, $"sinks[{label}].http.url", sink.Http?.Url, warnings);
            Check(entityKind, entityName, $"sinks[{label}].db.host", sink.Db?.Host, warnings);
            Check(entityKind, entityName, $"sinks[{label}].db.connectionString", sink.Db?.ConnectionString, warnings);
        }
    }

    private static void Check(string kind, string name, string field, string? value, List<EndpointWarning> warnings)
    {
        // Not a reference at all (empty, or a literal host/URL): nothing to resolve, nothing to warn
        // about — this is the overwhelmingly common case for every field this scan touches.
        if (!NamedEndpoints.IsReference(value))
        {
            return;
        }

        // TryResolve's own error text already names the reference and this instance's known names (see
        // NamedEndpoints.TryResolve) — reused verbatim rather than re-composed, so there is exactly one
        // sentence for "this environment doesn't know that name" in the whole codebase.
        if (!NamedEndpoints.TryResolve(value, out _, out var error))
        {
            warnings.Add(new EndpointWarning(kind, name, $"{field}: {error}"));
        }
    }
}

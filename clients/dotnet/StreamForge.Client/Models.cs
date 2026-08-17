namespace StreamForge.Client;

/// <summary>Minimal catalog entry -- id + name -- enough for the client's own id-resolution and
/// ad-hoc-table filtering. Deliberately not the engine's full TableDefinition: this client stays
/// decoupled from server-internal DTOs.</summary>
public readonly record struct TableSummary(string Id, string Name);

/// <summary>Result of <c>POST /api/tables/validate</c> (or its gRPC twin): whether the SQL
/// compiles, and if not, why.</summary>
public sealed record ValidateResult(bool Ok, IReadOnlyList<SqlDiagnostic> Diagnostics, string? PlanSummary);

/// <summary>Result of an ingest push, gRPC or REST. <see cref="Outcome"/> mirrors the engine's
/// <c>IngestOutcome</c> enum as a string (e.g. <c>INGEST_OUTCOME_ACCEPTED</c>).</summary>
public sealed record IngestAckResult(
    string Outcome,
    int Accepted,
    int Dropped,
    int Invalid,
    string? Error,
    IReadOnlyList<string> RowErrors);

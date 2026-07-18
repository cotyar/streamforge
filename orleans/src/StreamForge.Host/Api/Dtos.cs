using StreamForge.Abstractions;

namespace StreamForge.Host.Api;

// ============================================================================
// REST DTOs — must match web/src/api/types.ts exactly (camelCase via default
// System.Text.Json naming policy).
// ============================================================================

public sealed record LoginRequest(string Username, string Password);

public sealed record LoginResponse(string Token, string Username, string DisplayName, string Role);

public sealed record UserInfo(string Username, string DisplayName, string Role, long CreatedAtMs);

public sealed record CreateUserRequest(string Username, string DisplayName, string Role, string Password);

public sealed record UpdateUserRequest(string? DisplayName, string? Role, string? Password);

public sealed record CreatePipelineRequest(
    string Name,
    string Description,
    string Sql,
    List<string>? Tags = null,
    Dictionary<string, string>? Metadata = null);

public sealed record ValidateRequest(string Sql);

public sealed record SqlDiagnosticDto(string Message, int Line, int Column, string Severity);

public sealed record ValidateResponse(bool Ok, IReadOnlyList<SqlDiagnosticDto> Diagnostics, string? PlanSummary, IReadOnlyList<string> SourceNames);

public sealed record ErrorResponse(string Error);

public sealed record CreateTableRequest(
    string Name,
    string Description,
    string Sql,
    bool SearchEnabled = false,
    TableSearchMode SearchMode = TableSearchMode.Exact,
    bool HistoryEnabled = false,
    TableHistoryMode HistoryMode = TableHistoryMode.All,
    int HistoryLimit = 10,
    string? HistoryByField = null,
    long HistoryWindowMs = 0,
    List<string>? Tags = null,
    Dictionary<string, string>? Metadata = null,
    // Plan 003 M2: partitioned execution opt-in. 1 (default) = classic single-grain path. See
    // TableDefinition.Parallelism's doc comment; RegistryGrain validates 1..16.
    int Parallelism = 1);

public sealed record TableSearchResponse(IReadOnlyList<TableRowDto> Rows, string Mode, bool Enabled, int Total);

public sealed record FieldDefDto(string Name, string Kind);

public sealed record ValidateTableResponse(
    bool Ok,
    IReadOnlyList<SqlDiagnosticDto> Diagnostics,
    string? PlanSummary,
    IReadOnlyList<string> StreamInputs,
    IReadOnlyList<string> TableInputs,
    IReadOnlyList<FieldDefDto> OutputSchema);

public sealed record TableRowsResponse(IReadOnlyList<TableRowDto> Rows, int TotalRows, long Seq);

// Row history (Feature B). The client hands back the exact row object it already has (from the live grid
// or a search result) rather than pre-computing/round-tripping an opaque key — the server derives the
// row-identity key from it (TableGroupKeyExtractor + RowKeyCodec), so the client never needs to know
// whether/how a table's GROUP BY identity was derived.
public sealed record HistoryLookupRequest(Dictionary<string, object?> Row);

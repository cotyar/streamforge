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

public sealed record CreatePipelineRequest(string Name, string Description, string Sql);

public sealed record ValidateRequest(string Sql);

public sealed record SqlDiagnosticDto(string Message, int Line, int Column, string Severity);

public sealed record ValidateResponse(bool Ok, IReadOnlyList<SqlDiagnosticDto> Diagnostics, string? PlanSummary, IReadOnlyList<string> SourceNames);

public sealed record ErrorResponse(string Error);

public sealed record CreateTableRequest(string Name, string Description, string Sql);

public sealed record FieldDefDto(string Name, string Kind);

public sealed record ValidateTableResponse(
    bool Ok,
    IReadOnlyList<SqlDiagnosticDto> Diagnostics,
    string? PlanSummary,
    IReadOnlyList<string> StreamInputs,
    IReadOnlyList<string> TableInputs,
    IReadOnlyList<FieldDefDto> OutputSchema);

public sealed record TableRowsResponse(IReadOnlyList<TableRowDto> Rows, int TotalRows, long Seq);

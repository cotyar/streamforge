using StreamForge.Abstractions;
using StreamForge.Engine;
using V1 = StreamForge.Host.Grpc.V1;

namespace StreamForge.Host.Grpc;

/// <summary>Maps between StreamForge.Abstractions/Engine model types and the generated
/// StreamForge.Host.Grpc.V1 proto messages. Kept separate from the service implementations so the
/// REST-vs-gRPC field mapping is auditable in one place.</summary>
internal static class ProtoMappers
{
    // ------------------------------------------------------------------ FieldType / FieldDef

    public static V1.FieldType ToProto(FieldType type) => type switch
    {
        FieldType.String => V1.FieldType.String,
        FieldType.Double => V1.FieldType.Double,
        FieldType.Long => V1.FieldType.Long,
        FieldType.Bool => V1.FieldType.Bool,
        FieldType.Timestamp => V1.FieldType.Timestamp,
        FieldType.Json => V1.FieldType.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };

    public static FieldType FromProto(V1.FieldType type) => type switch
    {
        V1.FieldType.String => FieldType.String,
        V1.FieldType.Double => FieldType.Double,
        V1.FieldType.Long => FieldType.Long,
        V1.FieldType.Bool => FieldType.Bool,
        V1.FieldType.Timestamp => FieldType.Timestamp,
        V1.FieldType.Json => FieldType.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };

    public static V1.FieldDef ToProto(FieldDef field)
    {
        var proto = new V1.FieldDef { Name = field.Name, Type = ToProto(field.Type) };
        if (field.Children is { Count: > 0 })
        {
            proto.Children.AddRange(field.Children.Select(ToProto));
        }

        return proto;
    }

    public static FieldDef FromProto(V1.FieldDef field) => new(
        field.Name,
        FromProto(field.Type),
        field.Children.Count == 0 ? null : field.Children.Select(FromProto).ToList());

    // ------------------------------------------------------------------ PipelineStatus / TableSearchMode

    public static V1.PipelineStatus ToProto(PipelineStatus status) => status switch
    {
        PipelineStatus.Stopped => V1.PipelineStatus.Stopped,
        PipelineStatus.Running => V1.PipelineStatus.Running,
        PipelineStatus.Failed => V1.PipelineStatus.Failed,
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "Unknown pipeline status"),
    };

    public static V1.TableSearchMode ToProto(TableSearchMode mode) => mode switch
    {
        TableSearchMode.Exact => V1.TableSearchMode.Exact,
        TableSearchMode.Fuzzy => V1.TableSearchMode.Fuzzy,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown search mode"),
    };

    public static TableSearchMode FromProto(V1.TableSearchMode mode) => mode switch
    {
        V1.TableSearchMode.Exact => TableSearchMode.Exact,
        V1.TableSearchMode.Fuzzy => TableSearchMode.Fuzzy,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown search mode"),
    };

    // ------------------------------------------------------------------ SourceDefinition

    public static V1.SourceDefinition ToProto(SourceDefinition def)
    {
        var proto = new V1.SourceDefinition
        {
            Name = def.Name,
            Description = def.Description,
            GeneratorProfile = def.GeneratorProfile,
            EventsPerSecond = def.EventsPerSecond,
            Enabled = def.Enabled,
        };
        proto.Fields.AddRange(def.Fields.Select(ToProto));
        return proto;
    }

    public static SourceDefinition FromProto(V1.SourceDefinition def) => new()
    {
        Name = def.Name,
        Description = def.Description,
        Fields = def.Fields.Select(FromProto).ToList(),
        GeneratorProfile = string.IsNullOrEmpty(def.GeneratorProfile) ? "generic" : def.GeneratorProfile,
        EventsPerSecond = def.EventsPerSecond,
        Enabled = def.Enabled,
    };

    // ------------------------------------------------------------------ PipelineDefinition

    public static V1.PipelineDefinition ToProto(PipelineDefinition def)
    {
        var proto = new V1.PipelineDefinition
        {
            Id = def.Id,
            Name = def.Name,
            Description = def.Description,
            Sql = def.Sql,
            Status = ToProto(def.Status),
            CreatedBy = def.CreatedBy,
            CreatedAtMs = def.CreatedAtMs,
            UpdatedAtMs = def.UpdatedAtMs,
        };
        if (def.Error is not null)
        {
            proto.Error = def.Error;
        }

        return proto;
    }

    // ------------------------------------------------------------------ TableDefinition

    public static V1.TableDefinition ToProto(TableDefinition def)
    {
        var proto = new V1.TableDefinition
        {
            Id = def.Id,
            Name = def.Name,
            Description = def.Description,
            Sql = def.Sql,
            Status = ToProto(def.Status),
            CreatedBy = def.CreatedBy,
            CreatedAtMs = def.CreatedAtMs,
            UpdatedAtMs = def.UpdatedAtMs,
            SearchEnabled = def.SearchEnabled,
            SearchMode = ToProto(def.SearchMode),
        };
        if (def.Error is not null)
        {
            proto.Error = def.Error;
        }

        proto.OutputFields.AddRange(def.OutputFields.Select(ToProto));
        proto.StreamInputs.AddRange(def.StreamInputs);
        proto.TableInputs.AddRange(def.TableInputs);
        return proto;
    }

    // ------------------------------------------------------------------ Diagnostics / validate results

    public static V1.DiagnosticSeverity ToProto(DiagnosticSeverity severity) => severity switch
    {
        DiagnosticSeverity.Error => V1.DiagnosticSeverity.Error,
        DiagnosticSeverity.Warning => V1.DiagnosticSeverity.Warning,
        _ => throw new ArgumentOutOfRangeException(nameof(severity), severity, "Unknown severity"),
    };

    public static V1.SqlDiagnostic ToProto(SqlDiagnostic diagnostic) => new()
    {
        Message = diagnostic.Message,
        Line = diagnostic.Line,
        Column = diagnostic.Column,
        Severity = ToProto(diagnostic.Severity),
    };

    public static V1.ValidateResponse ToProtoValidateResponse(CompileResult result)
    {
        var proto = new V1.ValidateResponse { Ok = result.Ok };
        proto.Diagnostics.AddRange(result.Diagnostics.Select(ToProto));
        proto.SourceNames.AddRange(result.SourceNames);
        if (result.PlanSummary is not null)
        {
            proto.PlanSummary = result.PlanSummary;
        }

        return proto;
    }

    public static V1.ValidateTableResponse ToProtoValidateTableResponse(TableCompileResult result)
    {
        var proto = new V1.ValidateTableResponse { Ok = result.Ok };
        proto.Diagnostics.AddRange(result.Diagnostics.Select(ToProto));
        proto.StreamInputs.AddRange(result.StreamInputs);
        proto.TableInputs.AddRange(result.TableInputs);
        if (result.PlanSummary is not null)
        {
            proto.PlanSummary = result.PlanSummary;
        }

        if (result.OutputSchema is not null)
        {
            proto.OutputSchema.AddRange(result.OutputSchema.Fields.Select(f =>
                new V1.FieldDef { Name = f.Key, Type = ToProto(MapFieldKind(f.Value)) }));
        }

        return proto;
    }

    private static FieldType MapFieldKind(FieldKind kind) => kind switch
    {
        FieldKind.String => FieldType.String,
        FieldKind.Double => FieldType.Double,
        FieldKind.Long => FieldType.Long,
        FieldKind.Bool => FieldType.Bool,
        FieldKind.Timestamp => FieldType.Timestamp,
        FieldKind.Json => FieldType.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown field kind"),
    };

    public static FieldKind MapFieldKind(FieldType type) => type switch
    {
        FieldType.String => FieldKind.String,
        FieldType.Double => FieldKind.Double,
        FieldType.Long => FieldKind.Long,
        FieldType.Bool => FieldKind.Bool,
        FieldType.Timestamp => FieldKind.Timestamp,
        FieldType.Json => FieldKind.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };

    // ------------------------------------------------------------------ Rows

    public static V1.TableRow ToProto(TableRowDto row) => new()
    {
        Row = GrpcValueConverter.ToStruct(row.Row),
        Weight = row.Weight,
    };
}

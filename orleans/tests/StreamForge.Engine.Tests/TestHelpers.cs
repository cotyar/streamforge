namespace StreamForge.Engine.Tests;

internal static class TestHelpers
{
    public static SourceSchema Schema(string name, params (string Field, FieldKind Kind)[] fields)
        => new(name, fields.ToDictionary(f => f.Field, f => f.Kind));

    public static EventRecord Evt(long ts, string source, params (string Field, object? Value)[] fields)
    {
        var e = new EventRecord { ["_ts"] = ts, ["_source"] = source };
        foreach (var (k, v) in fields) e[k] = v;
        return e;
    }

    public static Dictionary<string, SourceSchema> Schemas(params SourceSchema[] schemas)
        => schemas.ToDictionary(s => s.Name);

    public static readonly SourceSchema Trades = Schema("trades", ("symbol", FieldKind.String), ("price", FieldKind.Double), ("qty", FieldKind.Long), ("active", FieldKind.Bool));
    public static readonly SourceSchema Quotes = Schema("quotes", ("symbol", FieldKind.String), ("bid", FieldKind.Double), ("ask", FieldKind.Double));
    public static readonly SourceSchema Ref = Schema("ref", ("symbol", FieldKind.String), ("tag", FieldKind.String));
    public static readonly SourceSchema Events = Schema("events", ("eventType", FieldKind.String), ("payload", FieldKind.Json));

    public static CompileResult Compile(string sql, params SourceSchema[] schemas) => SqlCompiler.Compile(sql, Schemas(schemas));

    public static PipelineExecutor CompileAndCreate(string sql, params SourceSchema[] schemas)
    {
        var result = Compile(sql, schemas);
        if (!result.Ok || result.Plan is null)
        {
            throw new Xunit.Sdk.XunitException($"Expected successful compile but got: {string.Join("; ", result.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"))}");
        }
        return result.Plan.CreateExecutor();
    }

    // ------------------------------------------------------------------
    // Table-mode helpers
    // ------------------------------------------------------------------

    public static TableCompileResult CompileTable(string sql, SourceSchema[] streamSchemas, SourceSchema[]? tableSchemas = null)
        => SqlCompiler.CompileTable(sql, Schemas(streamSchemas), Schemas(tableSchemas ?? []));

    public static TableCompileResult CompileTable(string sql, params SourceSchema[] streamSchemas)
        => CompileTable(sql, streamSchemas, null);

    public static TableExecutor CompileTableAndCreate(string sql, SourceSchema[] streamSchemas, SourceSchema[]? tableSchemas = null)
    {
        var result = CompileTable(sql, streamSchemas, tableSchemas);
        if (!result.Ok || result.Plan is null)
        {
            throw new Xunit.Sdk.XunitException($"Expected successful table compile but got: {string.Join("; ", result.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"))}");
        }
        return result.Plan.CreateExecutor();
    }

    public static TableExecutor CompileTableAndCreate(string sql, params SourceSchema[] streamSchemas)
        => CompileTableAndCreate(sql, streamSchemas, null);
}

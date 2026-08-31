namespace StreamsForge.Engine.Runtime;

/// <summary>The runtime row flowing through joins/filter/window/select. Columns are keyed
/// "{alias}_{field}" (so reserved "_ts"/"_source" become "{alias}__ts"/"{alias}__source"),
/// which lets a single lookup formula serve both plain and reserved fields uniformly.</summary>
internal sealed class WorkingRow
{
    public required long Ts { get; init; }
    public required List<string> Aliases { get; init; }
    public required Dictionary<string, object?> Fields { get; init; }

    public static WorkingRow FromEvent(string alias, EventRecord evt)
    {
        var fields = new Dictionary<string, object?>();
        foreach (var kv in evt) fields[$"{alias}_{kv.Key}"] = kv.Value;
        return new WorkingRow { Ts = evt.Timestamp, Aliases = [alias], Fields = fields };
    }

    public static WorkingRow Combine(WorkingRow left, WorkingRow right)
    {
        var fields = new Dictionary<string, object?>(left.Fields);
        foreach (var kv in right.Fields) fields[kv.Key] = kv.Value;
        var aliases = new List<string>(left.Aliases);
        aliases.AddRange(right.Aliases);
        return new WorkingRow { Ts = Math.Max(left.Ts, right.Ts), Aliases = aliases, Fields = fields };
    }

    /// <summary>Synthesizes an all-null row for one or more sides, used to null-pad outer-join misses.</summary>
    public static WorkingRow NullSide(IEnumerable<(string Alias, SourceSchema Schema)> sides)
    {
        var fields = new Dictionary<string, object?>();
        var aliases = new List<string>();
        foreach (var (alias, schema) in sides)
        {
            aliases.Add(alias);
            fields[$"{alias}_{EventRecord.TimestampField}"] = null;
            fields[$"{alias}_{EventRecord.SourceField}"] = null;
            foreach (var field in schema.Fields.Keys) fields[$"{alias}_{field}"] = null;
        }
        return new WorkingRow { Ts = 0, Aliases = aliases, Fields = fields };
    }
}

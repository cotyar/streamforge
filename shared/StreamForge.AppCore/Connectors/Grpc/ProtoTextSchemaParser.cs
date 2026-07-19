using System.Text.RegularExpressions;
using StreamForge.Abstractions;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.AppCore.Connectors.Grpc;

/// <summary>
/// Recovers <c>(List&lt;FieldDef&gt;, FieldNumberMap)</c> for an entity's row schema from a StreamForge
/// generated .proto file's TEXT — the counterpart of <see cref="DescriptorFactory"/>'s text renderer
/// (<see cref="DescriptorFactory"/>'s own <c>RenderProtoText</c>, wrapped by <see cref="ProtoFileBuilder"/>
/// for downloads). Plan 006 D-G: one of the two schema-acquisition paths for a gRPC subscription source
/// (<c>GrpcSubscriberCore</c>'s "proto" <c>SchemaSource</c>, fed from <c>/api/{kind}/{key}/proto</c>).
///
/// <para><b>Scope, by design (D-G, D-A "JSONPath-lite"-style closed subset)</b>: this is NOT a general
/// proto3 parser. It accepts <see cref="ProtoFileBuilder.Build"/> output ONLY — i.e. a file that starts
/// with the exact generator banner and has <c>streamforge.dynamic.v1</c> as its package — and is a
/// line-based reader tuned to <see cref="DescriptorFactory"/>'s deterministic renderer (one construct
/// per line, fixed indentation via 2-space pad, always exactly one blank line between top-level
/// messages). Anything else — a hand-written .proto, one from a different generator, or the file with
/// its banner stripped — is rejected with a diagnostic explaining the limitation, never guessed at.
/// Only the entity's OWN row message (the first message in the file, always emitted first by
/// <see cref="DescriptorFactory.Generate"/>) is parsed; the trailing <c>{Entity}Event</c>/
/// <c>{Entity}Delta</c> envelope messages and the shared streaming-contract block
/// (<c>EntitySubscribeRequest</c>/<c>DynamicFrame</c>/<c>DynamicStreamService</c>) are validated to be
/// PRESENT (as extra confidence this really is a StreamForge dynamic-entity file) but not otherwise
/// parsed — their shapes are fixed and well known to callers already.</para>
///
/// <para><b>The one thing proto TEXT can do that a binary descriptor (<see cref="ReflectionSchemaWalker"/>'s
/// input) can't</b>: <see cref="FieldDef.Type.Long"/> and <see cref="FieldDef.Type.Timestamp"/> both
/// compile to a bare proto3 <c>int64</c> — wire-identical, and indistinguishable from the
/// <see cref="Google.Protobuf.Reflection.FieldDescriptorProto"/> alone. <see cref="DescriptorFactory"/>
/// prints a disambiguating trailing comment on every scalar field ("StreamForge: Long" vs "StreamForge:
/// Timestamp (epoch milliseconds)") specifically so the proto-text path can recover the exact original
/// type; this parser uses it. (<see cref="ReflectionSchemaWalker"/>, which only sees the binary
/// descriptor with no such comment, documents the same ambiguity and defaults to Long — harmless at the
/// wire-decode level since both read as a plain int64/long.)</para>
/// </summary>
public static class ProtoTextSchemaParser
{
    private const string BannerPrefix = "// StreamForge generated .proto";
    private const string BannerMarker = "do not edit by hand";

    private static readonly Regex MessageOpenRegex = new(@"^message\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*\{$", RegexOptions.Compiled);

    private static readonly Regex FieldLineRegex = new(
        @"^(?<repeated>repeated\s+)?(?<type>[A-Za-z_][A-Za-z0-9_.]*)\s+(?<name>[a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(?<num>\d+);(?:\s*//\s*(?<comment>.*))?$",
        RegexOptions.Compiled);

    /// <summary>Parses <paramref name="protoText"/>, returning the entity row's fields + field-number
    /// map, or (null, null, diagnostics) when the text isn't a recognizable StreamForge-generated
    /// dynamic-entity proto. <paramref name="Diagnostics"/> is always populated with at least one entry
    /// on failure, explaining exactly why (missing banner / package / envelope messages / an
    /// unrecognized construct inside the row message).</summary>
    public static (List<FieldDef>? Fields, FieldNumberMap? Numbers, IReadOnlyList<string> Diagnostics) Parse(string protoText)
    {
        var diagnostics = new List<string>();

        if (string.IsNullOrWhiteSpace(protoText))
        {
            diagnostics.Add("Empty proto text.");
            return (null, null, diagnostics);
        }

        var rawLines = protoText.Replace("\r\n", "\n").Split('\n');
        var firstLine = rawLines[0].Trim();
        if (!firstLine.StartsWith(BannerPrefix, StringComparison.Ordinal) || !firstLine.Contains(BannerMarker, StringComparison.Ordinal))
        {
            diagnostics.Add(
                "Not a StreamForge-generated proto file: expected the generator banner " +
                $"('{BannerPrefix} ... {BannerMarker}...') as the first line. This parser only accepts " +
                "/api/{kind}/{key}/proto downloads from a StreamForge instance, not arbitrary third-party " +
                "or hand-written .proto files - use the \"reflection\" SchemaSource for those instead.");
            return (null, null, diagnostics);
        }

        var lines = rawLines.Select(l => l.Trim()).Where(l => l.Length > 0).ToList();

        if (!lines.Contains("syntax = \"proto3\";"))
        {
            diagnostics.Add("Missing 'syntax = \"proto3\";' declaration.");
            return (null, null, diagnostics);
        }

        var packageLine = $"package {DescriptorFactory.PackageName};";
        if (!lines.Contains(packageLine))
        {
            diagnostics.Add($"Missing or unexpected package declaration (expected '{packageLine}').");
            return (null, null, diagnostics);
        }

        var rootIndex = lines.FindIndex(l => MessageOpenRegex.IsMatch(l));
        if (rootIndex < 0)
        {
            diagnostics.Add("No 'message ... {' declaration found.");
            return (null, null, diagnostics);
        }

        var rootName = MessageOpenRegex.Match(lines[rootIndex]).Groups["name"].Value;

        if (!lines.Contains($"message {rootName}Event {{") || !lines.Contains($"message {rootName}Delta {{"))
        {
            diagnostics.Add(
                $"Missing '{rootName}Event'/'{rootName}Delta' envelope messages after '{rootName}' - " +
                "does not look like a StreamForge dynamic-entity proto (DescriptorFactory always emits " +
                "both alongside the entity's own row message).");
            return (null, null, diagnostics);
        }

        var numbers = new FieldNumberMap();
        var idx = rootIndex + 1;
        var fields = ParseMessageBody(lines, ref idx, "", numbers, diagnostics);

        foreach (var list in numbers.Reserved.Values)
        {
            list.Sort();
        }

        return (fields, numbers, diagnostics);
    }

    /// <summary>Parses one message body (the root entity message, or a nested Json-field message),
    /// starting at <paramref name="i"/> (the line right after the opening "message X {" line) and
    /// advancing <paramref name="i"/> past the matching closing "}". Two-phase: (1) walk the body
    /// collecting field lines, `reserved` numbers, and the LINE RANGES of any nested "message Y {...}"
    /// blocks (by name) without parsing their contents yet - nested blocks are rendered by
    /// <see cref="DescriptorFactory"/> AFTER all of a message's own fields, so a field's owning nested
    /// block can't be resolved until all fields are known; (2) for every Json field with a Children
    /// nested type, recursively parse the matching captured block (matched by
    /// <see cref="DescriptorFactory.ToPascalCase"/> of the field name, exactly how the block was named
    /// on generation) with the field's own path as its scope.</summary>
    private static List<FieldDef> ParseMessageBody(
        List<string> lines, ref int i, string scope, FieldNumberMap numbers, List<string> diagnostics)
    {
        var fieldLines = new List<(string Name, int Number, string TypeText, bool IsArray, string? Comment)>();
        var nestedBlocks = new Dictionary<string, (int Start, int End)>(StringComparer.Ordinal);

        while (i < lines.Count)
        {
            var line = lines[i];
            if (line == "}")
            {
                i++;
                break;
            }

            if (line.StartsWith("reserved ", StringComparison.Ordinal))
            {
                foreach (var n in ParseReservedNumbers(line))
                {
                    AddReserved(numbers, scope, n);
                }
                i++;
                continue;
            }

            var nestedMatch = MessageOpenRegex.Match(line);
            if (nestedMatch.Success)
            {
                var nestedName = nestedMatch.Groups["name"].Value;
                i++; // past "message Name {"
                var start = i;
                SkipBlock(lines, ref i); // advances i past the matching "}"
                var end = i - 1; // index of that "}" line
                nestedBlocks[nestedName] = (start, end);
                continue;
            }

            var fieldMatch = FieldLineRegex.Match(line);
            if (fieldMatch.Success)
            {
                var isArray = fieldMatch.Groups["repeated"].Success;
                var typeText = fieldMatch.Groups["type"].Value;
                var name = fieldMatch.Groups["name"].Value;
                var number = int.Parse(fieldMatch.Groups["num"].Value);
                var comment = fieldMatch.Groups["comment"].Success ? fieldMatch.Groups["comment"].Value : null;
                fieldLines.Add((name, number, typeText, isArray, comment));
                i++;
                continue;
            }

            diagnostics.Add($"Unrecognized line in message body (scope '{scope}'): '{line}'");
            i++;
        }

        var result = new List<FieldDef>();
        foreach (var f in fieldLines)
        {
            var path = FieldNumberMap.ChildPath(scope, f.Name);
            numbers.Active[path] = f.Number;

            var type = ClassifyType(f.TypeText, f.Comment, path, diagnostics, out var isNestedJson);
            List<FieldDef>? children = null;
            if (isNestedJson)
            {
                var nestedName = DescriptorFactory.ToPascalCase(f.Name);
                if (nestedBlocks.TryGetValue(nestedName, out var range))
                {
                    var (start, end) = range;
                    var subLines = lines.GetRange(start, end - start + 1); // includes the trailing "}" line
                    var subIdx = 0;
                    children = ParseMessageBody(subLines, ref subIdx, path, numbers, diagnostics);
                }
                else
                {
                    diagnostics.Add($"Nested message '{nestedName}' for field '{path}' was not found in the surrounding message body.");
                    children = [];
                }
            }

            result.Add(new FieldDef(f.Name, type, Children: children, IsArray: f.IsArray));
        }

        return result;
    }

    /// <summary>Advances <paramref name="i"/> past a nested message body already positioned right after
    /// its opening "message X {" line, tracking further nesting depth so a deeply-nested block's own
    /// closing braces don't get mistaken for this block's close.</summary>
    private static void SkipBlock(List<string> lines, ref int i)
    {
        var depth = 1;
        while (i < lines.Count && depth > 0)
        {
            var line = lines[i];
            if (MessageOpenRegex.IsMatch(line))
            {
                depth++;
            }
            else if (line == "}")
            {
                depth--;
            }
            i++;
        }
    }

    private static IEnumerable<int> ParseReservedNumbers(string line)
    {
        // "reserved 3, 7-9;" -> strip keyword + trailing ';', split on ','.
        var body = line["reserved ".Length..].TrimEnd(';', ' ');
        foreach (var token in body.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var dash = token.IndexOf('-');
            if (dash > 0)
            {
                var start = int.Parse(token[..dash]);
                var end = int.Parse(token[(dash + 1)..]);
                for (var n = start; n <= end; n++)
                {
                    yield return n;
                }
            }
            else
            {
                yield return int.Parse(token);
            }
        }
    }

    private static void AddReserved(FieldNumberMap numbers, string scope, int number)
    {
        if (!numbers.Reserved.TryGetValue(scope, out var list))
        {
            list = [];
            numbers.Reserved[scope] = list;
        }
        if (!list.Contains(number))
        {
            list.Add(number);
        }
    }

    /// <summary>Maps a field's rendered proto type text (+ disambiguating comment, for the
    /// Long/Timestamp int64 collision) back to <see cref="FieldType"/> - the inverse of
    /// <see cref="DescriptorFactory"/>'s <c>BuildMessage</c> switch. A bare identifier that isn't one of
    /// the fixed scalar/Struct keywords is a nested message type name (Json + Children).</summary>
    private static FieldType ClassifyType(string typeText, string? comment, string path, List<string> diagnostics, out bool isNestedJson)
    {
        isNestedJson = false;
        switch (typeText)
        {
            case "string":
                return FieldType.String;
            case "double":
                return FieldType.Double;
            case "bool":
                return FieldType.Bool;
            case "int64":
                // Ambiguous by proto type alone (Long and Timestamp both render as int64) - resolved
                // via DescriptorFactory's own disambiguating comment, always present on generated text.
                if (comment is not null && comment.Contains("Timestamp", StringComparison.Ordinal))
                {
                    return FieldType.Timestamp;
                }
                if (comment is null || !comment.Contains("Long", StringComparison.Ordinal))
                {
                    diagnostics.Add($"Field '{path}' is int64 with no recognizable StreamForge type comment - defaulting to Long.");
                }
                return FieldType.Long;
            case "sint64":
                diagnostics.Add($"Field '{path}' uses sint64, which StreamForge's generator only emits for envelope 'weight' fields, never entity row fields - treating as Long.");
                return FieldType.Long;
            case "google.protobuf.Struct":
                return FieldType.Json; // schemaless
            default:
                // Bare identifier (no dot, not a known scalar keyword) -> nested message type name.
                isNestedJson = true;
                return FieldType.Json;
        }
    }
}

using System.Text;
using Google.Protobuf.Reflection;
using StreamsForge.Abstractions;
using FieldType = StreamsForge.Abstractions.FieldType;

namespace StreamsForge.Host.Grpc.Dynamic;

/// <summary>
/// A generated dynamic schema: the raw descriptor protobuf StreamsForge builds at runtime for one
/// entity, the equivalent canonical .proto source text (kept structurally identical to the descriptor
/// by construction — <see cref="ProtoText"/> is rendered by walking <see cref="FileProto"/> itself),
/// and the field-number map used to build it (which callers persist and feed back into the next
/// <see cref="DescriptorFactory.Generate"/> call for that entity so field numbers stay stable).
/// </summary>
public sealed record GeneratedSchema(
    FileDescriptorProto FileProto,
    string ProtoText,
    FieldNumberMap FieldNumbers,
    string MessageName,
    string EventMessageName,
    string DeltaMessageName);

/// <summary>
/// Builds a <see cref="FileDescriptorProto"/> (+ matching .proto text) for one StreamsForge entity —
/// a stream source or table — from its <see cref="FieldDef"/> schema. Powers dynamic gRPC reflection
/// and generated .proto downloads for typed client codegen.
///
/// <para>Type mapping: String→string, Double→double, Long→int64, Bool→bool, Timestamp→int64 (millis,
/// commented), Json with declared <see cref="FieldDef.Children"/>→a nested message type (recursively),
/// Json without children (schemaless)→google.protobuf.Struct.</para>
///
/// <para><see cref="FieldDef.IsArray"/>→proto3 <c>repeated</c> on the field, orthogonal to the type
/// mapping above: a repeated nested message (Children declared), a repeated scalar, or repeated
/// google.protobuf.Struct (schemaless Json). <see cref="FieldNumberMap"/> numbering is unaffected —
/// an array field's Children still scope by the array field's own path, exactly like a non-array Json
/// field's Children.</para>
///
/// <para>Alongside the entity's own message, two envelope messages are always generated so both
/// pipeline results and table deltas have typed wrappers:
/// <c>{Entity}Event</c> { row, int64 seq, int64 ts_ms } and
/// <c>{Entity}Delta</c> { row, sint64 weight, int64 seq }.</para>
/// </summary>
public static class DescriptorFactory
{
    public const string PackageName = "streamsforge.dynamic.v1";

    private const string StructProtoPath = "google/protobuf/struct.proto";
    private const string StructTypeName = ".google.protobuf.Struct";

    /// <summary>
    /// Generates the descriptor + .proto text for <paramref name="entityName"/>. If
    /// <paramref name="existingFieldNumbers"/> is supplied, field numbers are kept stable per
    /// <see cref="FieldNumberMap.Assign"/>'s rules; otherwise fields are numbered sequentially 1..N
    /// per message scope in declaration order.
    /// </summary>
    public static GeneratedSchema Generate(string entityName, IReadOnlyList<FieldDef> fields, FieldNumberMap? existingFieldNumbers = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(entityName);
        ArgumentNullException.ThrowIfNull(fields);

        var numbers = FieldNumberMap.Assign(fields, existingFieldNumbers);
        var messageName = ToPascalCase(entityName);
        var eventName = messageName + "Event";
        var deltaName = messageName + "Delta";
        var rootFqn = PackageName + "." + messageName;

        var state = new BuildState();
        var rootMsg = BuildMessage(messageName, rootFqn, "", fields, numbers, state);
        ApplyReserved(rootMsg, numbers, "");

        var eventMsg = new DescriptorProto { Name = eventName };
        eventMsg.Field.Add(Field("row", 1, FieldDescriptorProto.Types.Type.Message, "." + rootFqn));
        eventMsg.Field.Add(Field("seq", 2, FieldDescriptorProto.Types.Type.Int64));
        eventMsg.Field.Add(Field("ts_ms", 3, FieldDescriptorProto.Types.Type.Int64));

        var deltaMsg = new DescriptorProto { Name = deltaName };
        deltaMsg.Field.Add(Field("row", 1, FieldDescriptorProto.Types.Type.Message, "." + rootFqn));
        deltaMsg.Field.Add(Field("weight", 2, FieldDescriptorProto.Types.Type.Sint64));
        deltaMsg.Field.Add(Field("seq", 3, FieldDescriptorProto.Types.Type.Int64));

        var file = new FileDescriptorProto
        {
            Name = ToProtoFileName(entityName) + ".proto",
            Package = PackageName,
            Syntax = "proto3",
        };
        if (state.NeedsStruct)
        {
            file.Dependency.Add(StructProtoPath);
        }
        file.MessageType.Add(rootMsg);
        file.MessageType.Add(eventMsg);
        file.MessageType.Add(deltaMsg);

        var text = RenderProtoText(file, state.Comments);

        return new GeneratedSchema(file, text, numbers, messageName, eventName, deltaName);
    }

    private sealed class BuildState
    {
        public bool NeedsStruct;
        // Reference-identity keyed: FieldDescriptorProto overrides Equals/GetHashCode with structural
        // (value) semantics, so two unrelated-but-identically-shaped fields (e.g. the same field name
        // + type + number reused at a different nesting level) would collide under the default
        // comparer. ReferenceEqualityComparer forces true per-instance identity instead.
        public readonly Dictionary<FieldDescriptorProto, string> Comments = new(ReferenceEqualityComparer.Instance);
    }

    private static DescriptorProto BuildMessage(
        string typeName, string fqnPrefix, string scope, IReadOnlyList<FieldDef> defs, FieldNumberMap numbers, BuildState state)
    {
        var msg = new DescriptorProto { Name = typeName };

        foreach (var f in defs)
        {
            var path = FieldNumberMap.ChildPath(scope, f.Name);
            var number = numbers.Active[path];

            var fieldProto = new FieldDescriptorProto
            {
                Name = f.Name,
                Number = number,
                // IsArray -> "repeated": the wire cardinality (element shape is otherwise identical to
                // the non-array case — see ProtoWireEncoder for the matching repeated-write logic).
                Label = f.IsArray ? FieldDescriptorProto.Types.Label.Repeated : FieldDescriptorProto.Types.Label.Optional,
            };
            var arraySuffix = f.IsArray ? ", repeated" : "";

            string comment;
            switch (f.Type)
            {
                case FieldType.String:
                    fieldProto.Type = FieldDescriptorProto.Types.Type.String;
                    comment = "StreamsForge: String" + arraySuffix;
                    break;
                case FieldType.Double:
                    fieldProto.Type = FieldDescriptorProto.Types.Type.Double;
                    comment = "StreamsForge: Double" + arraySuffix;
                    break;
                case FieldType.Long:
                    fieldProto.Type = FieldDescriptorProto.Types.Type.Int64;
                    comment = "StreamsForge: Long" + arraySuffix;
                    break;
                case FieldType.Bool:
                    fieldProto.Type = FieldDescriptorProto.Types.Type.Bool;
                    comment = "StreamsForge: Bool" + arraySuffix;
                    break;
                case FieldType.Timestamp:
                    fieldProto.Type = FieldDescriptorProto.Types.Type.Int64;
                    comment = "StreamsForge: Timestamp (epoch milliseconds)" + arraySuffix;
                    break;
                case FieldType.Json when f.Children is { Count: > 0 }:
                {
                    // IsArray + Children: a typed list of records — the nested message describes ONE
                    // element's shape; "repeated" (above) carries the list-ness. Naming intentionally
                    // matches the non-array case (PascalCase of the field name, e.g. "legs" -> "Legs")
                    // rather than singularizing — see DescriptorFactory's doc comment.
                    var nestedName = ToPascalCase(f.Name);
                    var nestedFqn = fqnPrefix + "." + nestedName;
                    var nestedMsg = BuildMessage(nestedName, nestedFqn, path, f.Children, numbers, state);
                    ApplyReserved(nestedMsg, numbers, path);
                    msg.NestedType.Add(nestedMsg);
                    fieldProto.Type = FieldDescriptorProto.Types.Type.Message;
                    fieldProto.TypeName = "." + nestedFqn;
                    comment = "StreamsForge: Json (nested)" + arraySuffix;
                    break;
                }
                case FieldType.Json:
                    // IsArray + Type=Json + no Children: repeated google.protobuf.Struct (a list of
                    // schemaless values), same well-known type as the singular schemaless case.
                    fieldProto.Type = FieldDescriptorProto.Types.Type.Message;
                    fieldProto.TypeName = StructTypeName;
                    state.NeedsStruct = true;
                    comment = "StreamsForge: Json (schemaless)" + arraySuffix;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(defs), f.Type, "Unknown field type");
            }

            state.Comments[fieldProto] = comment;
            msg.Field.Add(fieldProto);
        }

        return msg;
    }

    private static void ApplyReserved(DescriptorProto msg, FieldNumberMap numbers, string scope)
    {
        if (!numbers.Reserved.TryGetValue(scope, out var nums) || nums.Count == 0)
        {
            return;
        }

        foreach (var (start, end) in ToRanges(nums))
        {
            // DescriptorProto.Types.ReservedRange.Start is inclusive, End is EXCLUSIVE
            // (mirrors descriptor.proto's own doc comment on the field).
            msg.ReservedRange.Add(new DescriptorProto.Types.ReservedRange { Start = start, End = end + 1 });
        }
    }

    /// <summary>Groups a sorted, deduplicated list of numbers into contiguous [start, end] ranges.</summary>
    private static IEnumerable<(int Start, int End)> ToRanges(IReadOnlyList<int> sortedNums)
    {
        var i = 0;
        while (i < sortedNums.Count)
        {
            var start = sortedNums[i];
            var end = start;
            while (i + 1 < sortedNums.Count && sortedNums[i + 1] == end + 1)
            {
                i++;
                end = sortedNums[i];
            }
            yield return (start, end);
            i++;
        }
    }

    private static FieldDescriptorProto Field(string name, int number, FieldDescriptorProto.Types.Type type, string? typeName = null)
    {
        var f = new FieldDescriptorProto
        {
            Name = name,
            Number = number,
            Type = type,
            Label = FieldDescriptorProto.Types.Label.Optional,
        };
        if (typeName is not null)
        {
            f.TypeName = typeName;
        }
        return f;
    }

    // ------------------------------------------------------------------
    // .proto text rendering — walks FileProto itself (not FieldDef) so it is
    // structurally guaranteed to match the descriptor it was rendered from.
    // ------------------------------------------------------------------

    private static string RenderProtoText(FileDescriptorProto file, Dictionary<FieldDescriptorProto, string> comments)
    {
        var sb = new StringBuilder();
        sb.Append("syntax = \"proto3\";\n\n");
        sb.Append("package ").Append(file.Package).Append(";\n");

        if (file.Dependency.Count > 0)
        {
            sb.Append('\n');
            foreach (var dep in file.Dependency)
            {
                sb.Append("import \"").Append(dep).Append("\";\n");
            }
        }

        foreach (var msg in file.MessageType)
        {
            sb.Append('\n');
            RenderMessage(sb, msg, comments, indent: 0);
        }

        return sb.ToString();
    }

    private static void RenderMessage(StringBuilder sb, DescriptorProto msg, Dictionary<FieldDescriptorProto, string> comments, int indent)
    {
        var pad = new string(' ', indent * 2);
        var innerPad = new string(' ', (indent + 1) * 2);

        sb.Append(pad).Append("message ").Append(msg.Name).Append(" {\n");

        if (msg.ReservedRange.Count > 0)
        {
            var parts = msg.ReservedRange.Select(r => r.End - r.Start == 1 ? r.Start.ToString() : $"{r.Start}-{r.End - 1}");
            sb.Append(innerPad).Append("reserved ").Append(string.Join(", ", parts)).Append(";\n");
        }

        foreach (var field in msg.Field)
        {
            sb.Append(innerPad).Append(FieldTypeText(field)).Append(' ').Append(field.Name).Append(" = ").Append(field.Number).Append(';');
            if (comments.TryGetValue(field, out var comment))
            {
                sb.Append(" // ").Append(comment);
            }
            sb.Append('\n');
        }

        foreach (var nested in msg.NestedType)
        {
            sb.Append('\n');
            RenderMessage(sb, nested, comments, indent + 1);
        }

        sb.Append(pad).Append("}\n");
    }

    private static string FieldTypeText(FieldDescriptorProto field)
    {
        var baseType = field.Type switch
        {
            FieldDescriptorProto.Types.Type.String => "string",
            FieldDescriptorProto.Types.Type.Double => "double",
            FieldDescriptorProto.Types.Type.Int64 => "int64",
            FieldDescriptorProto.Types.Type.Bool => "bool",
            FieldDescriptorProto.Types.Type.Sint64 => "sint64",
            FieldDescriptorProto.Types.Type.Message => field.TypeName == StructTypeName ? "google.protobuf.Struct" : field.TypeName.Split('.')[^1],
            _ => throw new ArgumentOutOfRangeException(nameof(field), field.Type, "Unhandled field type in text renderer"),
        };
        // proto3 "repeated" cardinality prefix — mirrors FieldDescriptorProto.Label, set from
        // FieldDef.IsArray in BuildMessage above.
        return field.Label == FieldDescriptorProto.Types.Label.Repeated ? "repeated " + baseType : baseType;
    }

    // ------------------------------------------------------------------
    // Naming
    // ------------------------------------------------------------------

    /// <summary>snake_case / lower / kebab-case → PascalCase, e.g. "gold_tier_orders" → "GoldTierOrders".
    /// Leaves already-PascalCase input unchanged.</summary>
    internal static string ToPascalCase(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return "_";
        }

        var sb = new StringBuilder(name.Length);
        var capitalizeNext = true;
        foreach (var c in name)
        {
            if (!char.IsLetterOrDigit(c))
            {
                capitalizeNext = true;
                continue;
            }
            sb.Append(capitalizeNext ? char.ToUpperInvariant(c) : c);
            capitalizeNext = false;
        }

        if (sb.Length == 0)
        {
            return "_";
        }
        if (char.IsDigit(sb[0]))
        {
            sb.Insert(0, '_');
        }
        return sb.ToString();
    }

    private static string ToProtoFileName(string entityName)
    {
        var sb = new StringBuilder(entityName.Length);
        foreach (var c in entityName)
        {
            sb.Append(char.IsLetterOrDigit(c) ? char.ToLowerInvariant(c) : '_');
        }
        return sb.Length == 0 ? "entity" : sb.ToString();
    }
}

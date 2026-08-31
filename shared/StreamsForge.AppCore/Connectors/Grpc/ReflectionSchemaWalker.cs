using Google.Protobuf.Reflection;
using StreamsForge.Abstractions;
using StreamsForge.Host.Grpc.Dynamic;
using FieldType = StreamsForge.Abstractions.FieldType;

namespace StreamsForge.AppCore.Connectors.Grpc;

/// <summary>
/// Recovers <c>(List&lt;FieldDef&gt;, FieldNumberMap)</c> for an entity's row schema from a set of
/// <see cref="FileDescriptorProto"/>s obtained via gRPC Server Reflection (v1alpha
/// <c>FileContainingSymbol</c>/<c>FileByFilename</c>) — the counterpart of <see cref="DescriptorFactory"/>'s
/// <c>BuildMessage</c>, walking the binary descriptor structure it produced instead of parsing text.
/// Plan 006 D-G: the other of the two schema-acquisition paths for a gRPC subscription source
/// (<c>GrpcSubscriberCore</c>'s "reflection" <c>SchemaSource</c> — mirrors what
/// <c>DynamicReflectionService</c> serves on the remote).
///
/// <para><b>Pure</b>: no I/O. The caller (<c>GrpcSubscriberCore</c>) drives the reflection RPC and hands
/// the resulting descriptor set here.</para>
///
/// <para><b>Locating the row message</b>: the descriptor set handed in is typically just the ONE file
/// the reflection RPC resolved for the requested symbol (plus its dependency closure, e.g.
/// google/protobuf/struct.proto), and that file always contains exactly the entity's own row message
/// plus its <c>{Entity}Event</c>/<c>{Entity}Delta</c> envelope siblings (see
/// <see cref="DescriptorFactory.Generate"/> - it always emits exactly those three top-level messages).
/// This method finds the row message structurally, by that Event/Delta-sibling shape, rather than by
/// re-deriving a name from <paramref name="entityKey" />'s "kind:ident" text purely: for a "source:{name}"
/// key, <paramref name="entityKey"/>'s ident IS the entity's readable name (so
/// <see cref="DescriptorFactory.ToPascalCase"/> of it exactly matches the generated message name and is
/// tried first, disambiguating the rare case where more than one row-message triplet is present); for
/// "table:{id}"/"pipeline:{id}" keys the ident is the entity's ID, which only coincides with its
/// generator-derived message name when the id happens to equal (a PascalCase-safe form of) the entity's
/// display name - a real but honestly-documented limitation of the v1alpha reflection protocol itself
/// (it has no "look up by arbitrary id" operation, only by exact symbol/filename), which is why a single
/// unambiguous row-message triplet in the supplied descriptor set is always accepted as a fallback.</para>
///
/// <para><b>Long vs Timestamp is unrecoverable from a binary descriptor alone</b>: both compile to a
/// bare proto3 <c>int64</c>, and <see cref="FieldDescriptorProto"/> (unlike the rendered .proto TEXT
/// <see cref="ProtoTextSchemaParser"/> reads) carries no field comments - reflection has strictly less
/// information than a downloaded .proto here. This is harmless at the wire level (both decode via a
/// plain int64/long read in <see cref="ProtoWireDecoder"/>), so every ambiguous int64 field defaults to
/// <see cref="FieldType.Long"/>, documented rather than silently guessed.</para>
/// </summary>
public static class ReflectionSchemaWalker
{
    public static (List<FieldDef>? Fields, FieldNumberMap? Numbers, IReadOnlyList<string> Diagnostics)
        FromDescriptors(IReadOnlyList<FileDescriptorProto> files, string entityKey)
    {
        var diagnostics = new List<string>();

        if (files is null || files.Count == 0)
        {
            diagnostics.Add("No descriptor files supplied.");
            return (null, null, diagnostics);
        }

        var root = FindRootMessage(files, entityKey, diagnostics);
        if (root is null)
        {
            diagnostics.Add($"Could not locate a StreamsForge entity row message for entity key '{entityKey}' among the supplied descriptors.");
            return (null, null, diagnostics);
        }

        var numbers = new FieldNumberMap();
        var fields = WalkMessage(root, "", numbers, diagnostics);
        return (fields, numbers, diagnostics);
    }

    private static DescriptorProto? FindRootMessage(IReadOnlyList<FileDescriptorProto> files, string entityKey, List<string> diagnostics)
    {
        var ident = entityKey;
        var colon = entityKey.IndexOf(':');
        if (colon >= 0 && colon < entityKey.Length - 1)
        {
            ident = entityKey[(colon + 1)..];
        }
        var expectedName = DescriptorFactory.ToPascalCase(ident);

        var candidates = new List<DescriptorProto>();
        foreach (var file in files)
        {
            var byName = file.MessageType.ToDictionary(m => m.Name, m => m);
            foreach (var msg in file.MessageType)
            {
                if (msg.Name.EndsWith("Event", StringComparison.Ordinal) || msg.Name.EndsWith("Delta", StringComparison.Ordinal))
                {
                    continue; // an envelope message, not a row message
                }
                if (byName.ContainsKey(msg.Name + "Event") && byName.ContainsKey(msg.Name + "Delta"))
                {
                    candidates.Add(msg);
                }
            }
        }

        if (candidates.Count == 0)
        {
            return null;
        }

        var exact = candidates.FirstOrDefault(c => c.Name == expectedName);
        if (exact is not null)
        {
            return exact;
        }

        if (candidates.Count == 1)
        {
            return candidates[0];
        }

        diagnostics.Add(
            $"Multiple candidate entity row messages found ({string.Join(", ", candidates.Select(c => c.Name))}) " +
            $"and none named '{expectedName}' (expected from entity key '{entityKey}') - cannot disambiguate.");
        return null;
    }

    /// <summary>Walks one message's fields (+ its <c>reserved</c> ranges) into <see cref="FieldDef"/>s,
    /// recursing into <see cref="DescriptorProto.NestedType"/> for Json fields with declared children -
    /// the inverse of <see cref="DescriptorFactory"/>'s <c>BuildMessage</c>.</summary>
    private static List<FieldDef> WalkMessage(DescriptorProto msg, string scope, FieldNumberMap numbers, List<string> diagnostics)
    {
        var fields = new List<FieldDef>();

        foreach (var fd in msg.Field)
        {
            var path = FieldNumberMap.ChildPath(scope, fd.Name);
            numbers.Active[path] = fd.Number;
            var isArray = fd.Label == FieldDescriptorProto.Types.Label.Repeated;

            switch (fd.Type)
            {
                case FieldDescriptorProto.Types.Type.String:
                    fields.Add(new FieldDef(fd.Name, FieldType.String, IsArray: isArray));
                    break;
                case FieldDescriptorProto.Types.Type.Double:
                    fields.Add(new FieldDef(fd.Name, FieldType.Double, IsArray: isArray));
                    break;
                case FieldDescriptorProto.Types.Type.Int64:
                    // See type-doc: Long/Timestamp are wire- and descriptor-identical; default Long.
                    fields.Add(new FieldDef(fd.Name, FieldType.Long, IsArray: isArray));
                    break;
                case FieldDescriptorProto.Types.Type.Bool:
                    fields.Add(new FieldDef(fd.Name, FieldType.Bool, IsArray: isArray));
                    break;
                case FieldDescriptorProto.Types.Type.Message:
                    if (fd.TypeName == ".google.protobuf.Struct")
                    {
                        fields.Add(new FieldDef(fd.Name, FieldType.Json, IsArray: isArray)); // schemaless
                    }
                    else
                    {
                        var nestedTypeName = fd.TypeName.Split('.')[^1];
                        var nested = msg.NestedType.FirstOrDefault(n => n.Name == nestedTypeName);
                        if (nested is null)
                        {
                            diagnostics.Add($"Nested message type '{fd.TypeName}' referenced by field '{path}' was not found among '{msg.Name}''s nested types.");
                            break;
                        }
                        var children = WalkMessage(nested, path, numbers, diagnostics);
                        fields.Add(new FieldDef(fd.Name, FieldType.Json, Children: children, IsArray: isArray));
                    }
                    break;
                default:
                    diagnostics.Add($"Unsupported field type '{fd.Type}' for field '{path}' - skipped.");
                    break;
            }
        }

        if (msg.ReservedRange.Count > 0)
        {
            var nums = new List<int>();
            foreach (var range in msg.ReservedRange)
            {
                // DescriptorProto.Types.ReservedRange.Start inclusive, End EXCLUSIVE (see DescriptorFactory.ApplyReserved).
                for (var n = range.Start; n < range.End; n++)
                {
                    nums.Add(n);
                }
            }
            nums.Sort();
            numbers.Reserved[scope] = nums;
        }

        return fields;
    }
}

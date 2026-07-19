using Google.Protobuf;
using Google.Protobuf.Reflection;
using Google.Protobuf.WellKnownTypes;

namespace StreamForge.Host.Grpc.Dynamic;

/// <summary>
/// Proves a <see cref="DescriptorFactory"/>-generated <see cref="FileDescriptorProto"/> is a
/// structurally valid descriptor by round-tripping it through
/// <see cref="FileDescriptor.BuildFromByteStrings(System.Collections.Generic.IEnumerable{ByteString})"/>
/// — the same machinery gRPC server reflection and protobuf runtimes use to resolve descriptors that
/// weren't baked in at compile time via protoc-generated code.
///
/// <para><b>Dependency wiring</b>: <c>BuildFromByteStrings</c> resolves a file's <c>dependency</c>
/// entries (by filename, e.g. "google/protobuf/struct.proto") against the OTHER byte strings in the
/// same call — it does not reach out to any registry or the filesystem. Two things matter:
/// (1) every dependency the file declares must have its own serialized FileDescriptorProto included
/// in the list, and (2) a dependency's bytes must appear BEFORE the file that depends on it — verified
/// empirically: passing them in the reverse order throws <c>ArgumentException: Dependency missing</c>
/// even though the required bytes are present in the list, just in the wrong position.</para>
/// </summary>
public static class DescriptorVerifier
{
    private const string StructProtoPath = "google/protobuf/struct.proto";

    /// <summary>Builds and returns the <see cref="FileDescriptor"/> for <paramref name="fileProto"/>,
    /// throwing if it's structurally invalid. Automatically includes google/protobuf/struct.proto's
    /// own descriptor bytes (ahead of the caller's file, per the ordering requirement above) when
    /// <paramref name="fileProto"/> declares that dependency.</summary>
    public static FileDescriptor Verify(FileDescriptorProto fileProto)
    {
        ArgumentNullException.ThrowIfNull(fileProto);

        var byteStrings = new List<ByteString>();
        if (fileProto.Dependency.Contains(StructProtoPath))
        {
            byteStrings.Add(StructReflection.Descriptor.SerializedData);
        }
        byteStrings.Add(fileProto.ToByteString());

        var built = FileDescriptor.BuildFromByteStrings(byteStrings);
        return built.Single(f => f.Name == fileProto.Name);
    }
}

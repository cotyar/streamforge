using Grpc.Core;
using Grpc.Reflection.V1Alpha;
using Google.Protobuf.Reflection;
using Microsoft.AspNetCore.Authorization;
using Orleans;
using StreamForge.Abstractions;
using StaticV1 = StreamForge.Host.Grpc.V1;
using DynamicV1 = StreamForge.Host.Grpc.Dynamic.V1;

namespace StreamForge.Host.Grpc.Dynamic;

/// <summary>
/// gRPC Server Reflection (grpc.reflection.v1alpha.ServerReflection) hand-implemented against
/// <c>Grpc.Reflection</c>'s generated <see cref="ServerReflection.ServerReflectionBase"/> — NOT the
/// built-in <c>Grpc.AspNetCore.Server.Reflection</c> package's <c>AddGrpcReflection()</c>/
/// <c>MapGrpcReflectionService()</c>, which only auto-discovers statically <c>MapGrpcService&lt;T&gt;()</c>-
/// registered services and has no hook for feeding it runtime-built descriptors.
///
/// <para><b>What the packages expose</b>: <c>Grpc.AspNetCore.Server.Reflection.dll</c> ships only two
/// extension methods (<c>AddGrpcReflection</c>, <c>MapGrpcReflectionService</c>) and an internal marker
/// service — no subclassable type. <c>Grpc.Reflection.dll</c> (the reference implementation package,
/// already an indirect dependency of the former, added here directly) DOES ship the protoc-generated
/// <c>ServerReflectionBase</c> abstract class (both v1 and v1alpha) with a single bidi-streaming method,
/// <c>ServerReflectionInfo</c> — the entire protocol (ListServices / FileContainingSymbol /
/// FileByFilename / ...) is one oneof-request/oneof-response RPC, not separate unary RPCs. The package
/// also ships ready-made <c>ReflectionServiceImpl</c>/<c>ReflectionV1ServiceImpl</c> implementations
/// constructible from a fixed <c>ServiceDescriptor[]</c>, but those can't mix in the per-request dynamic
/// entity descriptors this service needs, so the protocol logic below is hand-rolled against the base
/// class instead.</para>
///
/// <para><b>Static + dynamic together</b>: every request folds in BOTH the compile-time
/// <c>streamforge.v1</c>/<c>streamforge.dynamic.v1</c> (control-plane) descriptors — walked from the
/// generated <c>StreamforgeReflection.Descriptor</c>/<c>StreamforgeDynamicReflection.Descriptor</c> and
/// their dependency closures once at class-init, since those never change — AND a freshly-rebuilt
/// <see cref="DynamicDescriptorSet"/> for the current catalog's sources/tables/pipelines (rebuilt per
/// request; see that type's doc for why that's fine for a demo). <c>FileContainingSymbol</c> resolves
/// against message/service/enum full names across the merged set; <c>FileByFilename</c> against
/// filenames; both return the requested file's transitive dependency closure serialized
/// dependency-first (google/protobuf/struct.proto ahead of any dynamic entity file that imports it),
/// matching what <see cref="DescriptorVerifier"/>/<c>FileDescriptor.BuildFromByteStrings</c> requires.</para>
///
/// <para><b>Auth</b>: <see cref="AllowAnonymousAttribute"/> — reflection is metadata (schema shapes, not
/// row data), and standard reflection clients (grpcurl, Kreya, grpcui) call it with no credentials by
/// default. This deliberately diverges from the Viewer policy the rest of the gRPC surface uses.</para>
/// </summary>
[AllowAnonymous]
public sealed class DynamicReflectionService(IClusterClient client) : ServerReflection.ServerReflectionBase
{
    private static readonly IReadOnlyList<FileDescriptor> StaticFiles = BuildStaticFiles();

    private IRegistryGrain Registry => client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    public override async Task ServerReflectionInfo(
        IAsyncStreamReader<ServerReflectionRequest> requestStream,
        IServerStreamWriter<ServerReflectionResponse> responseStream,
        ServerCallContext context)
    {
        // Rebuilt fresh for EVERY individual request within the call (not once for the whole bidi
        // stream) — a reflection client conventionally keeps one ServerReflectionInfo call open across
        // an entire editing session (grpcurl/Kreya/grpcui all do), so pinning the catalog snapshot to
        // call-start would mean a schema edit made mid-session (e.g. PUT /api/sources/{name} adding a
        // field) never shows up for that client without it reconnecting. Reflection calls are rare
        // enough that rebuilding per request is exactly the "simplest cache policy" DynamicDescriptorSet
        // itself documents.
        while (await requestStream.MoveNext(context.CancellationToken))
        {
            var (files, serviceNames) = await BuildFileSetAsync(context.CancellationToken);
            var response = HandleRequest(requestStream.Current, files, serviceNames);
            await responseStream.WriteAsync(response);
        }
    }

    private async Task<(Dictionary<string, FileDescriptor> Files, List<string> ServiceNames)> BuildFileSetAsync(CancellationToken cancellationToken)
    {
        var dynamicEntities = await new DynamicDescriptorSet(Registry).BuildAsync(cancellationToken);

        var files = new Dictionary<string, FileDescriptor>(StringComparer.Ordinal);
        foreach (var f in StaticFiles)
        {
            files.TryAdd(f.Name, f);
        }

        foreach (var entity in dynamicEntities)
        {
            FileDescriptor fd;
            try
            {
                fd = DescriptorVerifier.Verify(entity.Schema.FileProto);
            }
            catch (DescriptorValidationException)
            {
                // An entity's display Name can contain characters DescriptorFactory.ToPascalCase
                // preserves verbatim (e.g. a pipeline named "VWAP by symbol (5s)" -> message name
                // "VWAPBySymbol(5s)") that are legal in the source name but NOT in a proto identifier —
                // the underlying protobuf runtime rejects those at FileDescriptor-build time. Skip this
                // one entity rather than failing the whole reflection call; it's the reflection-surface
                // analogue of "pipelines whose SQL currently compiles" for broken-SQL pipelines: not
                // every catalog entity can be reflected, and one bad name shouldn't take the rest down.
                continue;
            }

            if (!files.TryAdd(fd.Name, fd))
            {
                continue; // filename collision -- first entity wins, mirrors DynamicDescriptorSet's own guard
            }

            foreach (var dep in fd.Dependencies)
            {
                files.TryAdd(dep.Name, dep);
            }
        }

        var serviceNames = files.Values
            .SelectMany(f => f.Services.Select(s => s.FullName))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        return (files, serviceNames);
    }

    private static ServerReflectionResponse HandleRequest(
        ServerReflectionRequest request, IReadOnlyDictionary<string, FileDescriptor> files, IReadOnlyList<string> serviceNames)
    {
        var response = new ServerReflectionResponse
        {
            ValidHost = request.Host,
            OriginalRequest = request,
        };

        switch (request.MessageRequestCase)
        {
            case ServerReflectionRequest.MessageRequestOneofCase.ListServices:
                var list = new ListServiceResponse();
                list.Service.AddRange(serviceNames.Select(n => new ServiceResponse { Name = n }));
                response.ListServicesResponse = list;
                break;

            case ServerReflectionRequest.MessageRequestOneofCase.FileByFilename:
                response.FileDescriptorResponse = files.TryGetValue(request.FileByFilename, out var byName)
                    ? BuildFileDescriptorResponse(byName)
                    : null;
                if (response.FileDescriptorResponse is null)
                {
                    response.ErrorResponse = NotFound($"File not found: {request.FileByFilename}");
                }
                break;

            case ServerReflectionRequest.MessageRequestOneofCase.FileContainingSymbol:
                var owner = FindFileForSymbol(files, request.FileContainingSymbol);
                if (owner is not null)
                {
                    response.FileDescriptorResponse = BuildFileDescriptorResponse(owner);
                }
                else
                {
                    response.ErrorResponse = NotFound($"Symbol not found: {request.FileContainingSymbol}");
                }
                break;

            case ServerReflectionRequest.MessageRequestOneofCase.FileContainingExtension:
            case ServerReflectionRequest.MessageRequestOneofCase.AllExtensionNumbersOfType:
            default:
                // proto3 has no extensions in this codebase; not implemented rather than silently wrong.
                response.ErrorResponse = new ErrorResponse
                {
                    ErrorCode = (int)StatusCode.Unimplemented,
                    ErrorMessage = "Not supported by StreamForge dynamic reflection",
                };
                break;
        }

        return response;
    }

    private static ErrorResponse NotFound(string message) => new()
    {
        ErrorCode = (int)StatusCode.NotFound,
        ErrorMessage = message,
    };

    /// <summary>Requested file + its transitive dependency closure, serialized dependency-first (a
    /// dependency's bytes appear before any file that imports it) — required for
    /// <c>FileDescriptor.BuildFromByteStrings</c> to accept the set on the client side, per
    /// <see cref="DescriptorVerifier"/>'s doc comment on the same ordering requirement.</summary>
    private static FileDescriptorResponse BuildFileDescriptorResponse(FileDescriptor root)
    {
        var order = new List<FileDescriptor>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        void Visit(FileDescriptor fd)
        {
            if (!seen.Add(fd.Name))
            {
                return;
            }

            foreach (var dep in fd.Dependencies)
            {
                Visit(dep);
            }

            order.Add(fd);
        }

        Visit(root);

        var response = new FileDescriptorResponse();
        response.FileDescriptorProto.AddRange(order.Select(fd => fd.SerializedData));
        return response;
    }

    /// <summary>Full name may be a message (recursively including nested types), a service, or an enum
    /// type — the three symbol kinds this codebase's .proto files declare.</summary>
    private static FileDescriptor? FindFileForSymbol(IReadOnlyDictionary<string, FileDescriptor> files, string symbol)
    {
        symbol = symbol.TrimStart('.');
        foreach (var fd in files.Values)
        {
            if (fd.Services.Any(s => s.FullName == symbol))
            {
                return fd;
            }

            if (fd.EnumTypes.Any(e => e.FullName == symbol))
            {
                return fd;
            }

            if (ContainsMessage(fd.MessageTypes, symbol))
            {
                return fd;
            }
        }

        return null;

        static bool ContainsMessage(IList<MessageDescriptor> messages, string sym)
        {
            foreach (var m in messages)
            {
                if (m.FullName == sym || ContainsMessage(m.NestedTypes, sym))
                {
                    return true;
                }
            }

            return false;
        }
    }

    private static IReadOnlyList<FileDescriptor> BuildStaticFiles()
    {
        var files = new Dictionary<string, FileDescriptor>(StringComparer.Ordinal);

        void AddClosure(FileDescriptor fd)
        {
            if (!files.TryAdd(fd.Name, fd))
            {
                return;
            }

            foreach (var dep in fd.Dependencies)
            {
                AddClosure(dep);
            }
        }

        AddClosure(StaticV1.StreamforgeReflection.Descriptor);
        AddClosure(DynamicV1.StreamforgeDynamicReflection.Descriptor);

        return [.. files.Values];
    }
}

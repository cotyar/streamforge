using System.Diagnostics;
using StreamsForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Real-compile validity check: writes <see cref="ProtoFileBuilder.Build"/>'s output for the fattest
/// schema shape (nested Json messages + a schemaless Json/Struct field, combined into one entity, on
/// top of the appended DynamicStreamService streaming contract) to a scratch .csproj with a
/// <c>&lt;Protobuf&gt;</c> item (<c>GrpcServices="Client"</c>, matching what
/// <c>tools/generate-client.sh</c> generates for real users) and runs <c>dotnet build</c> on it via
/// Grpc.Tools' protoc integration. A clean build is the strongest available guarantee that: (1) the
/// appended block doesn't collide with DescriptorFactory's imports/format, (2) the combined file is
/// well-formed proto3 a client can compile standalone, and (3) the generated C# actually contains the
/// typed message/service classes <c>tools/generate-client.sh</c>'s wrapper depends on.
///
/// <para>Measured locally at ~2s (offline, warm NuGet cache for Google.Protobuf/Grpc.Net.Client/
/// Grpc.Tools 2.80.0 / 3.31.1 — the same versions StreamsForge.Host itself references transitively via
/// Grpc.AspNetCore), well under the 60s budget for a real-compile test, so this is preferred over the
/// hand-decode-only fallback for THIS assertion (structural validity). <see cref="ProtoWireCompatibilityTests"/>
/// covers the complementary "wire bytes match the declared field numbers" assertion by hand-decoding,
/// per this suite's brief, since that one doesn't need protoc at all — it's about ProtoWireEncoder vs.
/// FieldNumberMap, not about the .proto text.</para>
/// </summary>
[Trait("Category", "Slow")]
public class ProtoFileBuilderCompileTests
{
    [Fact]
    public void Fattest_schema_shape_plus_streaming_contract_compiles_standalone_with_protoc()
    {
        // Kitchen-sink schema (see TestHelpers.KitchenSinkFields): every scalar FieldType, two levels
        // of nested Json messages, and a schemaless Json/Struct field, all in one entity — deliberately
        // the heaviest single entity DescriptorFactory can produce (multiple nested message types + the
        // Struct well-known type import), so a clean compile here is the strongest single test of
        // appended-block safety.
        var schema = DescriptorFactory.Generate("kitchen_sink", TestHelpers.KitchenSinkFields);
        var protoText = ProtoFileBuilder.Build("source", "kitchen_sink", schema);

        // NOT Path.GetTempPath(): on macOS it returns a path through /var, which is itself a symlink
        // to /private/var. protoc's --proto_path matching is a dumb string-prefix check (see its own
        // error text below) that does NOT resolve symlinks, while MSBuild's Full path resolution for
        // the <Protobuf> item DOES end up on the /private/var side — the two disagree and protoc
        // rejects an otherwise perfectly valid project-relative path. AppContext.BaseDirectory (the
        // test's own bin/ output, under the repo) isn't behind a symlink, so it doesn't hit this.
        var scratchDir = Path.Combine(AppContext.BaseDirectory, "proto-compile-test-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(scratchDir);
        try
        {
            var protoPath = Path.Combine(scratchDir, schema.FileProto.Name);
            File.WriteAllText(protoPath, protoText);

            var csprojPath = Path.Combine(scratchDir, "compile-check.csproj");
            File.WriteAllText(csprojPath, $"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net10.0</TargetFramework>
                    <Nullable>enable</Nullable>
                    <ImplicitUsings>enable</ImplicitUsings>
                  </PropertyGroup>
                  <ItemGroup>
                    <PackageReference Include="Google.Protobuf" Version="3.31.1" />
                    <PackageReference Include="Grpc.Net.Client" Version="2.80.0" />
                    <PackageReference Include="Grpc.Tools" Version="2.80.0" PrivateAssets="All" />
                  </ItemGroup>
                  <ItemGroup>
                    <Protobuf Include="{schema.FileProto.Name}" GrpcServices="Client" />
                  </ItemGroup>
                </Project>
                """);

            var (exitCode, output) = RunDotnetBuild(csprojPath, scratchDir);
            Assert.True(exitCode == 0, $"protoc/dotnet build of the generated .proto failed:\n{output}");

            // The generated C# must actually contain the typed classes the CLI-generated wrapper
            // (tools/generate-client.sh's StreamsForgeClient.cs template) depends on.
            var generatedSources = Directory.GetFiles(Path.Combine(scratchDir, "obj"), "*.cs", SearchOption.AllDirectories);
            var generatedText = string.Join("\n", generatedSources.Select(File.ReadAllText));

            Assert.Contains("class KitchenSinkEvent", generatedText);
            Assert.Contains("class KitchenSinkDelta", generatedText);
            Assert.Contains("class DynamicFrame", generatedText);
            Assert.Contains("class EntitySubscribeRequest", generatedText);
            Assert.Contains("DynamicStreamServiceClient", generatedText);
        }
        finally
        {
            try { Directory.Delete(scratchDir, recursive: true); } catch { /* best effort cleanup */ }
        }
    }

    private static (int ExitCode, string Output) RunDotnetBuild(string csprojPath, string workingDirectory)
    {
        // Grpc.Tools computes protoc's -I/--proto_path relative to the process's current directory,
        // not just the .csproj's directory — without this, protoc rejects the (perfectly valid)
        // project-relative <Protobuf Include="kitchen_sink.proto"> with "File does not reside within
        // any path specified using --proto_path", found empirically running this test via `dotnet test`
        // (whose own working directory is the test host's, not the scratch dir).
        var psi = new ProcessStartInfo
        {
            FileName = ResolveDotnetPath(),
            ArgumentList = { "build", csprojPath, "--nologo" },
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        using var process = Process.Start(psi)!;
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();

        return (process.ExitCode, stdout + "\n" + stderr);
    }

    /// <summary>dotnet isn't guaranteed to be on PATH (it isn't in this repo's dev environment — see
    /// repo docs: use ~/.dotnet/dotnet). Prefer that well-known location, falling back to PATH so the
    /// test still works in environments where dotnet IS on PATH (e.g. most CI images).</summary>
    private static string ResolveDotnetPath()
    {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        var wellKnown = Path.Combine(home, ".dotnet", "dotnet");
        return File.Exists(wellKnown) ? wellKnown : "dotnet";
    }
}

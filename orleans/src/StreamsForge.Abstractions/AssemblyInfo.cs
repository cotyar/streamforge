// Bridges Orleans' code generator across the assembly boundary: StreamsForge.Abstractions carries
// Microsoft.Orleans.Sdk (the generator + runtime), while the [GenerateSerializer]/[Id] DTOs it
// depends on now live in shared/StreamsForge.Contracts (which only references the attribute
// package — see that project's csproj comment). This attribute tells the generator running here to
// also emit serializers/copiers for every type declared in Contracts' assembly, so cross-assembly
// serialization keeps working exactly as if the DTOs were still declared in this project.
[assembly: Orleans.GenerateCodeForDeclaringAssembly(typeof(StreamsForge.Abstractions.SourceDefinition))]

using Dapr.Actors.Client;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 W4 serialization decision (see dapr/ARCHITECTURE.md): every actor-invocation call site in
/// this project must opt into JSON serialization explicitly — the Dapr .NET SDK's ActorProxy default is
/// the legacy <c>DataContractSerializer</c>, which rejects plain records/DTOs without a parameterless
/// constructor and [DataContract]/[DataMember] attributes (exactly the shape of every request/response
/// type in Actors/I*Actor.cs, and of the shared Contracts DTOs, none of which carry those attributes —
/// they're plain C# records/classes meant for System.Text.Json). <see cref="Options"/> is the one shared
/// instance every <c>ActorProxy.Create&lt;T&gt;(...)</c> call in this project passes, so this decision
/// lives in exactly one place. The actor-SIDE half of the same decision is
/// <c>ActorRuntimeOptions.UseJsonSerialization</c>, set in Program.cs's <c>AddActors</c> call.
/// </summary>
public static class ActorProxyDefaults
{
    public static readonly ActorProxyOptions Options = new() { UseJsonSerialization = true };
}

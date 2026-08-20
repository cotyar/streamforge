using System.Runtime.CompilerServices;

// Plan 016 wave 5: lets GrpcEntityKeyResolutionTests exercise GrpcSubscriberCore's internal
// ResolveMessageIdentAsync (its 404/409/success handling) directly against an in-process fake REST
// server, rather than routing through the reconnecting subscribe loop and a real gRPC dial just to
// reach one method. Same shape and same justification as StreamForge.Engine/AssemblyInfo.cs.
[assembly: InternalsVisibleTo("StreamForge.AppCore.Tests")]

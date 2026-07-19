using System.Runtime.CompilerServices;

// Grants StreamForge.Engine.Tests direct access to this assembly's `internal` types — used by plan 003
// M1's per-op unit tests (Runtime/Ops/*) to instantiate and drive individual operator objects (state
// in/deltas in/deltas out) without going through TableExecutor/PipelineExecutor's public façade. Does not
// change PublicApi.cs or grant access to any other assembly (in particular NOT StreamForge.Host — the
// sibling agent's territory keeps consuming Engine through exactly the same public surface as before).
[assembly: InternalsVisibleTo("StreamForge.Engine.Tests")]

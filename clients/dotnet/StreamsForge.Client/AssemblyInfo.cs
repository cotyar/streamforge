using System.Runtime.CompilerServices;

// The conformance test needs RowCodec.FromJson to parse the shared JSON fixture into the same
// row shape the reducer works with; the contract tests exercise the public surface only.
[assembly: InternalsVisibleTo("StreamsForge.Client.Tests")]

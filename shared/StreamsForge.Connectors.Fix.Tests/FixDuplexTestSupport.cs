using StreamsForge.Abstractions;

namespace StreamsForge.Connectors.Fix.Tests;

/// <summary>Plan 019 wave E: the <c>fix-duplex</c> twin of <see cref="FixTestSupport"/> — a NEW file
/// rather than an edit to that one, per this wave's instruction to touch no existing test file. Reuses
/// <see cref="FixTestSupport.ValidConfig"/> for the shared <see cref="FixSourceConfig"/> shape (both kinds
/// take the same config type — see <c>SourceKinds.FixDuplex</c>'s own doc comment).</summary>
public static class FixDuplexTestSupport
{
    public static SourceDefinition FixDuplexSource(FixSourceConfig? config = null) => new()
    {
        Name = "fx-duplex",
        Kind = SourceKinds.FixDuplex,
        Fields = [new FieldDef("Symbol", FieldType.String)],
        Connector = new ConnectorConfig
        {
            Fix = config ?? FixTestSupport.ValidConfig(),
            Mapping = new MappingSpec
            {
                ItemsPath = "$",
                Fields = [new FieldMapEntry { Field = new FieldDef("Symbol", FieldType.String) }],
            },
        },
    };
}

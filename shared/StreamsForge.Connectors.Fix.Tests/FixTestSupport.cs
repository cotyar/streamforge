using QuickFix;
using StreamsForge.Abstractions;

namespace StreamsForge.Connectors.Fix.Tests;

/// <summary>Shared fixtures: a valid <see cref="FixSourceConfig"/>/<see cref="SourceDefinition"/> pair, and
/// a helper that turns a <c>|</c>-delimited field list into a standalone <see cref="Message"/> — no socket,
/// no session, just the in-memory parse <c>new Message(text, false)</c> does. That is plan 018-C's "fake
/// seam (no socket)": a <see cref="Message"/> is a plain value once its SOH-delimited text is parsed, so
/// <see cref="FixBridgeApplication.FromApp"/>/<see cref="FixBridgeApplication.ToAdmin"/> can be driven
/// directly with hand-built messages.</summary>
public static class FixTestSupport
{
    public static FixSourceConfig ValidConfig() => new()
    {
        Host = "fix.venue.example.com",
        Port = 9880,
        SenderCompId = "CLIENT",
        TargetCompId = "VENUE",
        BeginString = "FIX.4.4",
        HeartBtIntSeconds = 30,
        QueueCapacity = 100,
    };

    public static SourceDefinition FixSource(FixSourceConfig? config = null) => new()
    {
        Name = "fx",
        Kind = SourceKinds.Fix,
        Fields = [new FieldDef("Symbol", FieldType.String)],
        Connector = new ConnectorConfig
        {
            Fix = config ?? ValidConfig(),
            Mapping = new MappingSpec
            {
                ItemsPath = "$",
                Fields = [new FieldMapEntry { Field = new FieldDef("Symbol", FieldType.String) }],
            },
        },
    };

    /// <summary>Builds a standalone <see cref="Message"/> from <c>|</c>-delimited tag=value pairs, e.g.
    /// <c>"35=W|55=EUR/USD|268=1"</c> — no BeginString/BodyLength/Checksum required, since
    /// <c>validate: false</c> skips structural checks entirely.</summary>
    public static Message BuildMessage(string pipeDelimited)
    {
        const char soh = '\x01';
        var text = pipeDelimited.Replace('|', soh) + soh;
        return new Message(text, false);
    }

    public static SessionID FakeSessionId() => new("FIX.4.4", "CLIENT", "VENUE");
}

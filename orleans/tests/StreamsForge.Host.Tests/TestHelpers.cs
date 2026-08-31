using StreamsForge.Abstractions;

namespace StreamsForge.Host.Tests;

internal static class TestHelpers
{
    public static readonly List<FieldDef> FlatFields =
    [
        new FieldDef("symbol", FieldType.String),
        new FieldDef("price", FieldType.Double),
        new FieldDef("qty", FieldType.Long),
        new FieldDef("active", FieldType.Bool),
        new FieldDef("traded_at", FieldType.Timestamp),
    ];

    public static readonly List<FieldDef> NestedJsonFields =
    [
        new FieldDef("symbol", FieldType.String),
        new FieldDef("payload", FieldType.Json, Children:
        [
            new FieldDef("user", FieldType.Json, Children:
            [
                new FieldDef("id", FieldType.String),
                new FieldDef("tier", FieldType.String),
            ]),
            new FieldDef("amount", FieldType.Double),
        ]),
    ];

    public static readonly List<FieldDef> SchemalessJsonFields =
    [
        new FieldDef("symbol", FieldType.String),
        new FieldDef("meta", FieldType.Json), // no Children -> google.protobuf.Struct
    ];

    /// <summary>The fattest single-entity shape DescriptorFactory can produce with realistic,
    /// non-colliding field names (mirrors the seeded "app_events" source in
    /// MarketDataProfiles.SeedSources): every FieldType (String/Double/Long/Bool/Timestamp/Json),
    /// two levels of nested Json messages, AND a schemaless Json/Struct field, all in one schema. Used
    /// by ProtoFileBuilderCompileTests (real protoc compile) and ProtoWireCompatibilityTests (hand-decode).
    /// NOTE: deliberately NOT NestedJsonFields+SchemalessJsonFields concatenated — both of those start
    /// with a field named "symbol", and DescriptorFactory does not currently detect/reject duplicate
    /// field names within a message scope (FieldNumberMap keys by path, so two same-named siblings
    /// silently collide onto the same field number and DescriptorFactory emits an invalid descriptor
    /// with a repeated field name) — a latent pre-existing gap, out of scope here, avoided by using
    /// unique names throughout.</summary>
    /// <summary>A repeated nested-message field: "legs" is IsArray + Children (each element shaped like
    /// a small leg record) — DescriptorFactory should render this as "repeated Legs legs". Used by the
    /// Repeated*/Multileg* test suites (Phase L1 typed leg arrays).</summary>
    public static readonly List<FieldDef> RepeatedNestedFields =
    [
        new FieldDef("trade_id", FieldType.String),
        new FieldDef("legs", FieldType.Json, Children:
        [
            new FieldDef("leg_no", FieldType.Long),
            new FieldDef("ccy", FieldType.String),
            new FieldDef("notional", FieldType.Double),
            new FieldDef("active", FieldType.Bool),
            new FieldDef("as_of", FieldType.Timestamp),
        ], IsArray: true),
    ];

    /// <summary>A repeated scalar field: "tags" is IsArray with no Children -> repeated string.</summary>
    public static readonly List<FieldDef> RepeatedScalarFields =
    [
        new FieldDef("id", FieldType.String),
        new FieldDef("tags", FieldType.String, IsArray: true),
    ];

    /// <summary>A repeated schemaless Json field: "blobs" is IsArray + Type=Json + no Children ->
    /// repeated google.protobuf.Struct.</summary>
    public static readonly List<FieldDef> RepeatedStructFields =
    [
        new FieldDef("id", FieldType.String),
        new FieldDef("blobs", FieldType.Json, IsArray: true),
    ];

    public static readonly List<FieldDef> KitchenSinkFields =
    [
        new FieldDef("event_type", FieldType.String),
        new FieldDef("active", FieldType.Bool),
        new FieldDef("occurred_at", FieldType.Timestamp),
        new FieldDef("payload", FieldType.Json, Children:
        [
            new FieldDef("user", FieldType.Json, Children:
            [
                new FieldDef("id", FieldType.String),
                new FieldDef("tier", FieldType.String),
            ]),
            new FieldDef("order", FieldType.Json, Children:
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("qty", FieldType.Long),
                new FieldDef("price", FieldType.Double),
            ]),
        ]),
        new FieldDef("meta", FieldType.Json), // schemaless -> google.protobuf.Struct
    ];
}

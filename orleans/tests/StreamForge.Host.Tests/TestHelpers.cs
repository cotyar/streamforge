using StreamForge.Abstractions;

namespace StreamForge.Host.Tests;

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
}

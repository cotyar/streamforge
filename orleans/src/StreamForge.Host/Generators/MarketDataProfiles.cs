using System.Collections.Concurrent;
using StreamForge.Abstractions;
using StreamForge.Engine;

namespace StreamForge.Host.Generators;

/// <summary>Synthetic market-data generators: shared per-symbol random-walk mid prices, per-profile event shapes.</summary>
public static class MarketDataProfiles
{
    public static readonly string[] Symbols = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "META", "TSLA", "JPM"];
    private static readonly string[] Venues = ["NYSE", "NASDAQ", "ARCA"];
    private static readonly ConcurrentDictionary<string, double> MidPrices = new();

    private static double NextMid(string symbol) => MidPrices.AddOrUpdate(
        symbol,
        _ => 100 + Random.Shared.NextDouble() * 900,
        (_, current) => Math.Clamp(current * (1 + (Random.Shared.NextDouble() * 2 - 1) * 0.001), 1, 100_000));

    private static string RandomSymbol() => Symbols[Random.Shared.Next(Symbols.Length)];

    private static string RandomVenue() => Venues[Random.Shared.Next(Venues.Length)];

    private static string RandomSide() => Random.Shared.NextDouble() < 0.5 ? "BUY" : "SELL";

    private static long RoundLotQty() => Random.Shared.NextInt64(1, 51) * 10; // 10..500

    private static readonly string[] Tiers = ["gold", "silver", "bronze"];
    private static readonly string[] AppEventTypes = ["order.placed", "order.amended", "order.cancelled"];
    private static readonly string[] TagPool = ["web", "mobile", "api", "priority", "retry"];

    private static readonly string[] Ccys = ["USD", "EUR", "GBP", "JPY"];
    private static readonly string[] OptionStrategyProducts = ["STRADDLE", "STRANGLE", "FLY"];

    private static string RandomCcy() => Ccys[Random.Shared.Next(Ccys.Length)];

    /// <summary>A round-ish notional, e.g. 12,000,000 — plausible for an IR swap leg.</summary>
    private static double RandomNotional() => Random.Shared.Next(1, 100) * 1_000_000.0;

    /// <summary>Random non-empty subset of <see cref="TagPool"/>, as a JSON array value (List&lt;object?&gt;
    /// of string leaves) — used to exercise the JSON value domain's list side in the demo payload.</summary>
    private static List<object?> RandomTagSubset()
    {
        var tags = TagPool.Where(_ => Random.Shared.NextDouble() < 0.5).Cast<object?>().ToList();
        if (tags.Count == 0) tags.Add(TagPool[Random.Shared.Next(TagPool.Length)]);
        return tags;
    }

    /// <summary>Generates one synthetic event for the source's profile ("trades" | "quotes" | "orders" |
    /// "json-events" | else generic). The generic profile synthesizes values from the source's declared
    /// field schema — including nested objects for <see cref="FieldType.Json"/> fields that declare
    /// <see cref="FieldDef.Children"/> — so user-defined sources emit data matching their drilled-in shape.</summary>
    public static EventRecord GenerateEvent(SourceDefinition def)
    {
        var evt = new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = def.Name,
        };

        switch (def.GeneratorProfile)
        {
            case "trades":
            {
                var symbol = RandomSymbol();
                var mid = NextMid(symbol);
                evt["symbol"] = symbol;
                evt["price"] = Math.Round(mid * (1 + (Random.Shared.NextDouble() * 2 - 1) * 0.0005), 2);
                evt["qty"] = RoundLotQty();
                evt["side"] = RandomSide();
                evt["venue"] = RandomVenue();
                break;
            }

            case "quotes":
            {
                var symbol = RandomSymbol();
                var mid = NextMid(symbol);
                var spread = Math.Max(0.01, mid * 0.0005);
                evt["symbol"] = symbol;
                evt["bid"] = Math.Round(mid - spread, 2);
                evt["ask"] = Math.Round(mid + spread, 2);
                evt["bidSize"] = RoundLotQty() * 10;
                evt["askSize"] = RoundLotQty() * 10;
                evt["venue"] = RandomVenue();
                break;
            }

            case "orders":
            {
                var symbol = RandomSymbol();
                var mid = NextMid(symbol);
                var roll = Random.Shared.NextDouble();
                var status = roll < 0.6 ? "NEW" : roll < 0.9 ? "FILLED" : "CANCELLED";
                evt["symbol"] = symbol;
                evt["orderId"] = Guid.NewGuid().ToString("n")[..8];
                evt["side"] = RandomSide();
                evt["qty"] = RoundLotQty();
                evt["limitPrice"] = Math.Round(mid * (1 + (Random.Shared.NextDouble() * 2 - 1) * 0.005), 2);
                evt["status"] = status;
                break;
            }

            case "json-events":
            {
                var symbol = RandomSymbol();
                var mid = NextMid(symbol);
                var userId = $"u-{Random.Shared.Next(1, 1000)}";
                var tier = Tiers[Random.Shared.Next(Tiers.Length)];

                // Values restricted to the JSON dialect's domain: Dictionary<string, object?>,
                // List<object?>, string, double, long, bool — no other CLR types.
                var payload = new Dictionary<string, object?>
                {
                    ["user"] = new Dictionary<string, object?>
                    {
                        ["id"] = userId,
                        ["tier"] = tier,
                    },
                    ["order"] = new Dictionary<string, object?>
                    {
                        ["symbol"] = symbol,
                        ["qty"] = RoundLotQty(),
                        ["price"] = Math.Round(mid * (1 + (Random.Shared.NextDouble() * 2 - 1) * 0.0005), 2),
                    },
                    ["tags"] = RandomTagSubset(),
                };

                evt["eventType"] = AppEventTypes[Random.Shared.Next(AppEventTypes.Length)];
                evt["payload"] = payload;
                break;
            }

            case "multileg":
            {
                // Alternates between two multileg instrument families sharing one typed "legs" array
                // schema (see SeedSources' combined Leg field list): IR swaps (2 fixed-shape legs) and
                // option strategies (2-4 legs). Each leg dict only sets the fields relevant to its own
                // family — the rest are simply absent, which both EventRecord and ProtoWireEncoder
                // already treat as "omit that field for this element" (proto3 default semantics).
                var tradeId = $"T-{Random.Shared.Next(100_000, 999_999)}";
                evt["trade_id"] = tradeId;

                if (Random.Shared.NextDouble() < 0.5)
                {
                    var ccy = RandomCcy();
                    evt["product"] = "IRS";
                    evt["notional_ccy"] = ccy;
                    evt["legs"] = new List<object?>
                    {
                        new Dictionary<string, object?>
                        {
                            ["leg_no"] = 1L,
                            ["pay_rcv"] = "PAY",
                            ["notional"] = RandomNotional(),
                            ["ccy"] = ccy,
                            ["rate_type"] = "FIXED",
                            ["rate"] = Math.Round(Random.Shared.NextDouble() * 5, 3),
                        },
                        new Dictionary<string, object?>
                        {
                            ["leg_no"] = 2L,
                            ["pay_rcv"] = "RCV",
                            ["notional"] = RandomNotional(),
                            ["ccy"] = ccy,
                            ["rate_type"] = "FLOAT",
                            ["rate"] = Math.Round(Random.Shared.NextDouble() * 5, 3),
                        },
                    };
                }
                else
                {
                    var product = OptionStrategyProducts[Random.Shared.Next(OptionStrategyProducts.Length)];
                    var legCount = product == "FLY" ? Random.Shared.Next(3, 5) : 2; // STRADDLE/STRANGLE: 2, FLY: 3-4
                    var symbol = RandomSymbol();
                    var mid = NextMid(symbol);
                    var expiryTs = DateTimeOffset.UtcNow.AddDays(Random.Shared.Next(7, 180)).ToUnixTimeMilliseconds();

                    evt["product"] = product;
                    var legs = new List<object?>();
                    for (var legNo = 1; legNo <= legCount; legNo++)
                    {
                        var strikeOffset = (legNo - (legCount + 1) / 2.0) * 0.02;
                        legs.Add(new Dictionary<string, object?>
                        {
                            ["leg_no"] = (long)legNo,
                            ["cp"] = legNo % 2 == 0 ? "PUT" : "CALL",
                            ["strike"] = Math.Round(mid * (1 + strikeOffset), 2),
                            ["expiry_ts"] = expiryTs,
                            ["ratio"] = 1.0,
                        });
                    }
                    evt["legs"] = legs;
                }
                break;
            }

            default: // generic
            {
                // Honor the source's declared schema (incl. drilled-in JSON shape); fall back to a
                // key/value shape when no fields are declared.
                var fields = def.Fields.Where(f => !string.IsNullOrWhiteSpace(f.Name)).ToList();
                if (fields.Count == 0)
                {
                    evt["key"] = $"k{Random.Shared.Next(1, 6)}";
                    evt["value"] = Math.Round(Random.Shared.NextDouble() * 100, 4);
                }
                else
                {
                    foreach (var f in fields)
                        evt[f.Name] = SynthValue(f);
                }
                break;
            }
        }

        return evt;
    }

    /// <summary>A random value matching a declared field's type. Json fields with declared children
    /// become a nested object of those children (recursively); childless Json fields get a small opaque blob.</summary>
    private static object? SynthValue(FieldDef field) => field.Type switch
    {
        FieldType.String => $"{field.Name}-{Random.Shared.Next(1, 1000)}",
        FieldType.Double => Math.Round(Random.Shared.NextDouble() * 1000, 4),
        FieldType.Long => Random.Shared.NextInt64(0, 10_000),
        FieldType.Bool => Random.Shared.NextDouble() < 0.5,
        FieldType.Timestamp => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        FieldType.Json => field.Children is { Count: > 0 } children
            ? children.Where(c => !string.IsNullOrWhiteSpace(c.Name))
                .ToDictionary(c => c.Name, SynthValue)
            : new Dictionary<string, object?> { ["value"] = Random.Shared.Next(1, 100) },
        _ => null,
    };

    /// <summary>Default demo sources seeded on first registry initialization.</summary>
    public static List<SourceDefinition> SeedSources() =>
    [
        new SourceDefinition
        {
            Name = "trades",
            Description = "Synthetic trade prints",
            GeneratorProfile = "trades",
            EventsPerSecond = 8,
            Enabled = true,
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("price", FieldType.Double),
                new FieldDef("qty", FieldType.Long),
                new FieldDef("side", FieldType.String),
                new FieldDef("venue", FieldType.String),
            ],
        },
        new SourceDefinition
        {
            Name = "quotes",
            Description = "Synthetic top-of-book quotes",
            GeneratorProfile = "quotes",
            EventsPerSecond = 10,
            Enabled = true,
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("bid", FieldType.Double),
                new FieldDef("ask", FieldType.Double),
                new FieldDef("bidSize", FieldType.Long),
                new FieldDef("askSize", FieldType.Long),
                new FieldDef("venue", FieldType.String),
            ],
        },
        new SourceDefinition
        {
            Name = "orders",
            Description = "Synthetic order lifecycle events",
            GeneratorProfile = "orders",
            EventsPerSecond = 4,
            Enabled = true,
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("orderId", FieldType.String),
                new FieldDef("side", FieldType.String),
                new FieldDef("qty", FieldType.Long),
                new FieldDef("limitPrice", FieldType.Double),
                new FieldDef("status", FieldType.String),
            ],
        },
        new SourceDefinition
        {
            Name = "app_events",
            Description = "Synthetic application events with a nested JSON payload (user/order/tags)",
            GeneratorProfile = "json-events",
            EventsPerSecond = 3,
            Enabled = true,
            Fields =
            [
                new FieldDef("eventType", FieldType.String),
                // Declared nested shape of the payload, matching what the "json-events" profile emits.
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
                    new FieldDef("tags", FieldType.Json),
                ]),
            ],
        },
        new SourceDefinition
        {
            Name = "structures",
            Description = "Synthetic multileg instruments: IR swaps and option strategies (Phase L1 typed leg arrays)",
            GeneratorProfile = "multileg",
            EventsPerSecond = 3,
            Enabled = true,
            Fields =
            [
                new FieldDef("trade_id", FieldType.String),
                // "IRS" (interest-rate swap) or one of OptionStrategyProducts (STRADDLE/STRANGLE/FLY).
                new FieldDef("product", FieldType.String),
                // Swaps only; absent (omitted) for option-strategy events.
                new FieldDef("notional_ccy", FieldType.String),
                // Typed leg list: IsArray + Children -> DescriptorFactory emits a repeated nested
                // message. The Leg shape is a union of both instrument families' leg fields — each
                // generated leg dict only populates the subset relevant to its own product, leaving the
                // rest absent (proto3 "missing = omitted" semantics, same as any other field).
                new FieldDef("legs", FieldType.Json, Children:
                [
                    new FieldDef("leg_no", FieldType.Long),
                    // -- IR swap leg fields --
                    new FieldDef("pay_rcv", FieldType.String),
                    new FieldDef("notional", FieldType.Double),
                    new FieldDef("ccy", FieldType.String),
                    new FieldDef("rate_type", FieldType.String),
                    new FieldDef("rate", FieldType.Double),
                    // -- option-strategy leg fields --
                    new FieldDef("cp", FieldType.String),
                    new FieldDef("strike", FieldType.Double),
                    new FieldDef("expiry_ts", FieldType.Timestamp),
                    new FieldDef("ratio", FieldType.Double),
                ], IsArray: true),
            ],
        },
    ];
}

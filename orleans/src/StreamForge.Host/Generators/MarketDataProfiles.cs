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

    /// <summary>Random non-empty subset of <see cref="TagPool"/>, as a JSON array value (List&lt;object?&gt;
    /// of string leaves) — used to exercise the JSON value domain's list side in the demo payload.</summary>
    private static List<object?> RandomTagSubset()
    {
        var tags = TagPool.Where(_ => Random.Shared.NextDouble() < 0.5).Cast<object?>().ToList();
        if (tags.Count == 0) tags.Add(TagPool[Random.Shared.Next(TagPool.Length)]);
        return tags;
    }

    /// <summary>Generates one synthetic event for the given profile ("trades" | "quotes" | "orders" |
    /// "json-events" | else generic).</summary>
    public static EventRecord GenerateEvent(string profile, string sourceName)
    {
        var evt = new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = sourceName,
        };

        switch (profile)
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

            default: // generic
            {
                evt["key"] = $"k{Random.Shared.Next(1, 6)}";
                evt["value"] = Math.Round(Random.Shared.NextDouble() * 100, 4);
                break;
            }
        }

        return evt;
    }

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
                new FieldDef("payload", FieldType.Json),
            ],
        },
    ];
}

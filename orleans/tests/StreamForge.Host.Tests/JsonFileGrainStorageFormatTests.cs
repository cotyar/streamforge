using System.Text.Json;
using Orleans.Runtime;
using StreamForge.Host.Storage;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 011 wave C: <see cref="JsonFileGrainStorage"/> stopped writing indented JSON (it is a storage
/// format, not a document — every table's whole state file is rewritten on every flush tick, so the
/// indentation was per-flush bytes and per-flush serializer work for nobody's benefit).
///
/// Two things need proving, not assuming. (1) The write really is compact. (2) READS ARE UNAFFECTED
/// IN BOTH DIRECTIONS — an existing data dir written by any earlier (indented) build must still load,
/// or the change would silently wipe every current install's persisted tables on first activation.
/// That second one is the whole risk of the change, so it is tested against a file this test writes
/// indented itself rather than against an assumption about what JsonSerializer ignores.
/// </summary>
public class JsonFileGrainStorageFormatTests : IDisposable
{
    private sealed class Payload
    {
        public Dictionary<string, string> Rows { get; set; } = [];
        public long Seq { get; set; }
    }

    /// <summary>Minimal IGrainState — Orleans' own implementations are internal, and the storage provider
    /// only ever touches these three members.</summary>
    private sealed class TestGrainState<T> : IGrainState<T>
    {
        public T State { get; set; } = default!;
        public string? ETag { get; set; }
        public bool RecordExists { get; set; }
    }

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "sf-jsonstorage-format-tests", Guid.NewGuid().ToString("n"));

    public void Dispose()
    {
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static GrainId SomeGrainId() => GrainId.Create("table", "soak_states");

    private string StateFilePath(string stateName) =>
        Directory.GetFiles(Path.Combine(_dataDir, "state"), $"{stateName}.*.json").Single();

    [Fact]
    public async Task WrittenStateFileIsCompactJsonWithNoPrettyPrintingWhitespace()
    {
        var storage = new JsonFileGrainStorage("test", _dataDir);
        var state = new TestGrainState<Payload>
        {
            State = new Payload { Rows = { ["a"] = "1", ["b"] = "2" }, Seq = 7 },
        };

        await storage.WriteStateAsync("table", SomeGrainId(), state);

        var text = await File.ReadAllTextAsync(StateFilePath("table"));
        Assert.DoesNotContain("\n", text);        // WriteIndented emits one member per line
        Assert.DoesNotContain(": ", text);        // ...and a space after every ':'
        Assert.Contains("\"Seq\":7", text);
    }

    [Fact]
    public async Task StateFileWrittenByAnEarlierIndentedBuildStillDeserializes()
    {
        var storage = new JsonFileGrainStorage("test", _dataDir);
        var grainId = SomeGrainId();

        // Establish the file at the exact path the provider uses, then overwrite it with the INDENTED
        // encoding a pre-011 build would have produced — same bytes-modulo-whitespace, nothing else.
        var original = new Payload { Rows = { ["a"] = "1", ["b"] = "2" }, Seq = 42 };
        await storage.WriteStateAsync("table", grainId, new TestGrainState<Payload> { State = original });
        var path = StateFilePath("table");
        await File.WriteAllTextAsync(path, JsonSerializer.Serialize(original, new JsonSerializerOptions { WriteIndented = true }));
        Assert.Contains("\n", await File.ReadAllTextAsync(path)); // guard: the fixture really is indented

        var reloaded = new TestGrainState<Payload>();
        await storage.ReadStateAsync("table", grainId, reloaded);

        Assert.True(reloaded.RecordExists);
        Assert.Equal(42, reloaded.State.Seq);
        Assert.Equal("1", reloaded.State.Rows["a"]);
        Assert.Equal("2", reloaded.State.Rows["b"]);
    }

    [Fact]
    public async Task CompactWriteRoundTripsThroughTheProvidersOwnReader()
    {
        var storage = new JsonFileGrainStorage("test", _dataDir);
        var grainId = SomeGrainId();
        await storage.WriteStateAsync("table", grainId, new TestGrainState<Payload>
        {
            State = new Payload { Rows = { ["only"] = "row" }, Seq = 3 },
        });

        var reloaded = new TestGrainState<Payload>();
        await storage.ReadStateAsync("table", grainId, reloaded);

        Assert.True(reloaded.RecordExists);
        Assert.Equal(3, reloaded.State.Seq);
        Assert.Equal("row", reloaded.State.Rows["only"]);
    }
}

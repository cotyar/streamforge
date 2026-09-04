using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors;
using StreamsForge.AppCore.Connectors.Polling;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Per-file isolation in <see cref="ConnectorPollCycle.ExecuteFolder"/>. This is a DATA-LOSS
/// regression suite, not a cosmetic one: <c>ExecuteFolder</c> used to return an aggregate
/// <see cref="PollCycleResult.Error"/> whenever ANY file failed to parse, while still ledgering every file
/// it HAD parsed — and the drivers' emit policy is "a failed cycle emits nothing". So one malformed file
/// meant the good files beside it were marked as read AND their rows dropped, permanently. The shape these
/// tests pin: good rows come back with Error null and a <see cref="PollCycleResult.Note"/> naming the file
/// that failed; only the good files enter the ledger, so the bad one is retried (and lands) the moment it
/// is fixed; and "folder not found" — where nothing was read and nothing is being hidden — stays an Error.
/// Modeled on <see cref="ConnectorPollCycleCoercionTests"/>.</summary>
public class ConnectorPollCycleFolderIsolationTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("sf-folder-isolation-").FullName;

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
        GC.SuppressFinalize(this);
    }

    private static SourceDefinition FolderSource(string path) => new()
    {
        Name = "folder_src",
        Kind = SourceKinds.Folder,
        Fields = [new FieldDef("id", FieldType.String), new FieldDef("price", FieldType.Double)],
        Connector = new ConnectorConfig
        {
            Folder = new FolderPollConfig { Path = path, Format = FileFormats.Ndjson },
            Mapping = new MappingSpec
            {
                ItemsPath = "$",
                Fields =
                [
                    new FieldMapEntry { Field = new FieldDef("id", FieldType.String) },
                    new FieldMapEntry { Field = new FieldDef("price", FieldType.Double) },
                ],
            },
        },
    };

    private string Write(string fileName, string content)
    {
        var path = Path.Combine(_dir, fileName);
        File.WriteAllText(path, content);
        return path;
    }

    [Fact]
    public void A_malformed_file_keeps_the_good_files_rows_and_reports_them_as_a_Note_not_an_Error()
    {
        var goodA = Write("a-good.ndjson", "{\"id\":\"a1\",\"price\":1.5}\n");
        var goodB = Write("c-good.ndjson", "{\"id\":\"c1\",\"price\":3.5}\n");
        var bad = Write("bad.ndjson", "{ this is not json\n");

        var ledger = new FileLedger();
        var result = ConnectorPollCycle.ExecuteFolder(FolderSource(_dir), ledger, new DedupTracker(), 1000);

        // The whole point: the good files' rows survive the bad one.
        Assert.Null(result.Error);
        Assert.Equal(2, result.Rows.Count);
        Assert.Contains(result.Rows, r => (string?)r["id"] == "a1");
        Assert.Contains(result.Rows, r => (string?)r["id"] == "c1");

        Assert.NotNull(result.Note);
        Assert.Contains("bad.ndjson", result.Note);
        Assert.Contains("retried next cycle", result.Note);
        // The good file names must NOT be reported as failures.
        Assert.DoesNotContain("a-good.ndjson", result.Note);

        // Ledger holds ONLY the good files — the bad one is deliberately left out so the next cycle
        // re-reads it. That asymmetry is what makes "retried next cycle" true rather than aspirational.
        var persisted = ledger.ToPersistable();
        Assert.Equal(2, persisted.Count);
        Assert.Contains(goodA, persisted.Keys);
        Assert.Contains(goodB, persisted.Keys);
        Assert.DoesNotContain(bad, persisted.Keys);
    }

    [Fact]
    public void A_second_cycle_over_the_same_folder_re_reports_only_the_still_broken_file()
    {
        Write("a-good.ndjson", "{\"id\":\"a1\",\"price\":1.5}\n");
        Write("bad.ndjson", "{ this is not json\n");

        var ledger = new FileLedger();
        var def = FolderSource(_dir);
        var dedup = new DedupTracker();

        var first = ConnectorPollCycle.ExecuteFolder(def, ledger, dedup, 1000);
        Assert.Single(first.Rows);

        // Nothing changed on disk: the good file is ledgered so it is skipped, the bad file is not so it is
        // re-parsed — and fails again. Zero rows, still not an Error, still a Note.
        var second = ConnectorPollCycle.ExecuteFolder(def, ledger, dedup, 2000);

        Assert.Null(second.Error);
        Assert.Empty(second.Rows);
        Assert.NotNull(second.Note);
        Assert.Contains("bad.ndjson", second.Note);
    }

    [Fact]
    public void Fixing_the_malformed_file_lands_its_rows_on_the_next_cycle_with_no_Note()
    {
        Write("a-good.ndjson", "{\"id\":\"a1\",\"price\":1.5}\n");
        var bad = Write("bad.ndjson", "{ this is not json\n");

        var ledger = new FileLedger();
        var def = FolderSource(_dir);
        var dedup = new DedupTracker();

        var first = ConnectorPollCycle.ExecuteFolder(def, ledger, dedup, 1000);
        Assert.Single(first.Rows);
        Assert.NotNull(first.Note);

        File.WriteAllText(bad, "{\"id\":\"b1\",\"price\":2.5}\n");

        var second = ConnectorPollCycle.ExecuteFolder(def, ledger, dedup, 2000);

        Assert.Null(second.Error);
        Assert.Null(second.Note);
        Assert.Single(second.Rows);
        Assert.Equal("b1", second.Rows[0]["id"]);
        // And the previously-good file is not re-emitted — the ledger did its job for it all along.
        Assert.DoesNotContain(second.Rows, r => (string?)r["id"] == "a1");
    }

    [Fact]
    public void A_missing_folder_is_still_an_Error_not_a_Note()
    {
        var missing = Path.Combine(_dir, "no-such-subdir");

        var result = ConnectorPollCycle.ExecuteFolder(FolderSource(missing), new FileLedger(), new DedupTracker(), 1000);

        Assert.NotNull(result.Error);
        Assert.Contains("folder not found", result.Error);
        Assert.Null(result.Note);
        Assert.Empty(result.Rows);
    }

    [Fact]
    public void An_all_good_folder_reports_no_Note_at_all()
    {
        Write("a-good.ndjson", "{\"id\":\"a1\",\"price\":1.5}\n");
        Write("b-good.ndjson", "{\"id\":\"b1\",\"price\":2.5}\n");

        var result = ConnectorPollCycle.ExecuteFolder(FolderSource(_dir), new FileLedger(), new DedupTracker(), 1000);

        Assert.Null(result.Error);
        Assert.Null(result.Note);
        Assert.Equal(2, result.Rows.Count);
    }
}

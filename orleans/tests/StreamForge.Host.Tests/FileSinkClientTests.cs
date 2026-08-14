using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Formats;
using StreamForge.AppCore.Sinks;
using StreamForge.AppCore.Transports;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 012: <see cref="FileSinkClient"/> — the egress twin of the file source kind. Unlike
/// <see cref="NatsSinkClientTests"/> this one can exercise the SUCCESS path for real (a temp file is a
/// real destination in a way a NATS broker isn't here), so these pin what actually lands on disk as well
/// as the fire-and-forget failure contract.
/// </summary>
public class FileSinkClientTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "sf-filesink-" + Guid.NewGuid().ToString("N"));

    public FileSinkClientTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private string PathFor(string name) => Path.Combine(_dir, name);

    private static NatsTableDeltaMessage Delta(string symbol, long qty, long weight = 1) => new()
    {
        Table = "positions",
        Seq = 1,
        Row = new Dictionary<string, object?> { ["symbol"] = symbol, ["qty"] = qty },
        Weight = weight,
    };

    [Fact]
    public async Task Writes_a_header_once_then_one_line_per_row()
    {
        var path = PathFor("out.csv");
        await using (var client = new FileSinkClient(new FileSinkConfig { Path = path }, "table", "positions"))
        {
            await client.PublishAsync(Delta("ACME", 5), CancellationToken.None);
            await client.PublishAsync(Delta("WIDGET", 2, weight: -1), CancellationToken.None);

            Assert.Equal(2, client.Counters.Published);
            Assert.Equal(0, client.Counters.Failed);
        }

        Assert.Equal("symbol,qty,_weight\r\nACME,5,1\r\nWIDGET,2,-1\r\n", await File.ReadAllTextAsync(path));
    }

    [Fact]
    public async Task A_table_deltas_weight_is_a_column_so_a_retraction_survives_the_export()
    {
        var path = PathFor("weights.csv");
        await using (var client = new FileSinkClient(new FileSinkConfig { Path = path }, "table", "positions"))
        {
            await client.PublishAsync(Delta("ACME", 5, weight: -1), CancellationToken.None);
        }

        var parsed = FormatParsers.ParseCsv(await File.ReadAllTextAsync(path));
        Assert.Equal(-1, parsed[0].GetProperty("_weight").GetInt64());
    }

    [Fact]
    public async Task Creates_missing_directories_and_expands_name_in_the_path()
    {
        var path = Path.Combine(_dir, "nested", "deeper", "{name}.csv");
        await using (var client = new FileSinkClient(new FileSinkConfig { Path = path }, "table", "positions"))
        {
            await client.PublishAsync(Delta("ACME", 1), CancellationToken.None);
        }

        Assert.True(File.Exists(Path.Combine(_dir, "nested", "deeper", "positions.csv")));
    }

    [Fact]
    public async Task Appending_to_an_existing_file_reuses_its_header_instead_of_writing_a_second_one()
    {
        // The restart case: a new client, over a file some earlier client already wrote — including one
        // whose column ORDER differs from what this client's first row would have produced.
        var path = PathFor("restart.csv");
        await File.WriteAllTextAsync(path, "qty,symbol,_weight\r\n1,OLD,1\r\n");

        await using (var client = new FileSinkClient(new FileSinkConfig { Path = path }, "table", "positions"))
        {
            await client.PublishAsync(Delta("ACME", 5), CancellationToken.None);
        }

        Assert.Equal("qty,symbol,_weight\r\n1,OLD,1\r\n5,ACME,1\r\n", await File.ReadAllTextAsync(path));
    }

    [Fact]
    public async Task Explicit_columns_win_and_pick_their_own_order_and_subset()
    {
        var path = PathFor("cols.csv");
        await using (var client = new FileSinkClient(
            new FileSinkConfig { Path = path, Columns = "qty, symbol" }, "table", "positions"))
        {
            await client.PublishAsync(Delta("ACME", 5), CancellationToken.None);

            // _weight was excluded by the operator's own column list — that is a dropped column, and a
            // dropped column is counted, never silent.
            Assert.Equal(1, client.Counters.Failed);
            Assert.Contains("_weight", client.Counters.LastError);
        }

        Assert.Equal("qty,symbol\r\n5,ACME\r\n", await File.ReadAllTextAsync(path));
    }

    [Fact]
    public async Task A_column_that_only_appears_in_a_later_row_is_dropped_and_counted_not_appended()
    {
        var path = PathFor("ragged.csv");
        await using (var client = new FileSinkClient(new FileSinkConfig { Path = path }, "pipeline", "p1"))
        {
            await client.PublishAsync(new NatsPipelineRowMessage { Row = new Dictionary<string, object?> { ["a"] = 1L } }, CancellationToken.None);
            await client.PublishAsync(new NatsPipelineRowMessage { Row = new Dictionary<string, object?> { ["a"] = 2L, ["b"] = 9L } }, CancellationToken.None);

            Assert.Equal(2, client.Counters.Published);
            Assert.Equal(1, client.Counters.Failed);
            Assert.Contains("b", client.Counters.LastError);
        }

        // Crucially: the second line still has ONE cell. A file whose rows disagree on width is a file
        // no CSV reader can trust.
        Assert.Equal("a\r\n1\r\n2\r\n", await File.ReadAllTextAsync(path));
    }

    [Fact]
    public async Task Ndjson_format_writes_the_same_record_the_nats_sink_publishes()
    {
        var path = PathFor("out.ndjson");
        await using (var client = new FileSinkClient(
            new FileSinkConfig { Path = path, Format = FileFormats.Ndjson }, "table", "positions"))
        {
            await client.PublishAsync(Delta("ACME", 5), CancellationToken.None);
        }

        var items = FormatParsers.ParseNdjson(await File.ReadAllTextAsync(path));
        Assert.Single(items);
        Assert.Equal("positions", items[0].GetProperty("table").GetString());
        Assert.Equal("ACME", items[0].GetProperty("row").GetProperty("symbol").GetString());
    }

    [Fact]
    public async Task An_unwritable_path_never_throws_it_counts()
    {
        // The destination is an existing DIRECTORY — open fails, every time, for a reason no retry fixes.
        Directory.CreateDirectory(PathFor("iam-a-dir"));
        await using var client = new FileSinkClient(new FileSinkConfig { Path = PathFor("iam-a-dir") }, "table", "t");

        await client.PublishAsync(Delta("ACME", 1), CancellationToken.None);

        Assert.Equal(0, client.Counters.Published);
        Assert.Equal(1, client.Counters.Failed);
        Assert.NotNull(client.Counters.LastError);
    }

    [Fact]
    public async Task An_already_cancelled_token_is_shutdown_not_failure()
    {
        await using var client = new FileSinkClient(new FileSinkConfig { Path = PathFor("cancelled.csv") }, "table", "t");
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await client.PublishAsync(Delta("ACME", 1), cts.Token);

        Assert.Equal(0, client.Counters.Published);
        Assert.Equal(0, client.Counters.Failed);
    }

    // ---- registry wiring ----

    [Fact]
    public void The_file_kind_is_registered_and_only_counts_as_configured_with_a_path()
    {
        var transport = SinkTransports.Find(SinkKinds.File);

        Assert.NotNull(transport);
        Assert.False(transport.IsConfigured(new SinkSpec { Kind = SinkKinds.File }));
        Assert.False(transport.IsConfigured(new SinkSpec { Kind = SinkKinds.File, File = new FileSinkConfig() }));
        Assert.True(transport.IsConfigured(new SinkSpec { Kind = SinkKinds.File, File = new FileSinkConfig { Path = "/tmp/x.csv" } }));
    }

    [Fact]
    public void An_enabled_configured_file_sink_is_selected_for_publishing()
    {
        var active = SinkSelection.Active(
        [
            new SinkSpec { Kind = SinkKinds.File, Enabled = true, File = new FileSinkConfig { Path = PathFor("a.csv") } },
            new SinkSpec { Kind = SinkKinds.File, Enabled = false, File = new FileSinkConfig { Path = PathFor("b.csv") } },
            new SinkSpec { Kind = SinkKinds.File, Enabled = true, File = new FileSinkConfig() },
        ]);

        Assert.Single(active);
    }

    [Fact]
    public void The_descriptor_offers_csv_and_ndjson_and_holds_no_secrets()
    {
        var descriptor = SinkTransports.Find(SinkKinds.File)!.Describe();

        Assert.Equal("file", descriptor.ConfigProperty);
        Assert.Equal([FileFormats.Csv, FileFormats.Ndjson], descriptor.Fields.Single(f => f.Key == "format").Options);
        // 'json' can never be offered here: an append-only writer cannot close the array.
        Assert.DoesNotContain(FileFormats.JsonArray, descriptor.Fields.Single(f => f.Key == "format").Options!);
        Assert.DoesNotContain(descriptor.Fields, f => f.Type == TransportFieldTypes.Secret);
    }
}

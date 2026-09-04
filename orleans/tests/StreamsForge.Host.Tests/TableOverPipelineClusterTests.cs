using Orleans.TestingHost;
using StreamsForge.Abstractions;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Table-over-pipeline: a TABLE may name a PIPELINE as one of its relations. Proves the whole path
/// end-to-end against a real cluster — a disabled file source, a pipeline that filters it, and a table
/// whose only input is that pipeline — plus the relation-name uniqueness refusals and the
/// Parallelism &gt;= 2 (TableIngestGrain) variant.
///
/// <para><b>Why the counts are exact, not "at least".</b> The source is a <c>file</c> connector with a
/// dedup key, so its 300 rows are emitted once; the pipeline's WHERE keeps exactly 150 of them; the table
/// is LATEST BY (id), so a re-delivery would update a key rather than add one. Any number other than 150
/// is a real defect, which is what makes the assertion worth making.</para>
///
/// <para><b>Setup order is load-bearing and is NOT the same as a table-over-source test's.</b> A pipeline
/// has no replay ring and no attach protocol — a table subscribing late to a pipeline sees only what is
/// published afterwards, permanently. So everything downstream must be Running BEFORE the source is
/// enabled: source upserted DISABLED (it must exist for the pipeline's SQL to compile) -> pipeline
/// created and started -> table created and started -> source upserted ENABLED, which is the only call
/// that starts polling. Enabling the source earlier does not make this test flaky, it makes it fail.</para>
///
/// <para>Own single-silo <see cref="TestCluster"/> reusing the <c>internal</c>
/// <see cref="ConnectorTestSiloConfigurator"/>/<see cref="ConnectorTestClientConfigurator"/> from
/// <c>ConnectorGrainClusterTests</c> — the same cross-file reuse <c>SourceExactCountClusterTests</c> and
/// <c>TableJournalClusterTests</c> already do. <see cref="PollUntilAsync{T}"/> is a copy of the
/// identically-named private helper in those files, per this repo's one-owner-per-file rule.</para>
///
/// <para><b>The filter is <c>seq &lt; 150</c>, not <c>seq % 2 = 0</c></b>: this dialect has no modulo
/// operator (verified against the tokenizer and the grammar in <c>.claude/skills/sf-sql/SKILL.md</c>).
/// <c>&lt;</c> keeps exactly the same count with an equally exact expected id set.</para>
/// </summary>
public sealed class TableOverPipelineClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _scratchDir = null!;

    private const int TotalRows = 300;
    private const int KeptRows = 150;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<ConnectorTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ConnectorTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        _scratchDir = Directory.CreateTempSubdirectory("sf-table-over-pipeline-").FullName;
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        try
        {
            Directory.Delete(_scratchDir, recursive: true);
        }
        catch
        {
            // best-effort cleanup
        }
    }

    // ---- shared shape: id Long, seq Long, value String; dedup key "id" (same as SourceExactCountClusterTests) ----

    private static List<FieldDef> Fields() =>
    [
        new FieldDef("id", FieldType.Long),
        new FieldDef("seq", FieldType.Long),
        new FieldDef("value", FieldType.String),
    ];

    private static MappingSpec Mapping() => new()
    {
        ItemsPath = "$",
        DedupKeyField = "id",
        Fields =
        [
            new FieldMapEntry { Field = new FieldDef("id", FieldType.Long) },
            new FieldMapEntry { Field = new FieldDef("seq", FieldType.Long) },
            new FieldMapEntry { Field = new FieldDef("value", FieldType.String) },
        ],
    };

    private static SourceDefinition MakeFileSource(string name, string path, bool enabled) => new()
    {
        Name = name,
        Kind = SourceKinds.File,
        Enabled = enabled,
        Fields = Fields(),
        Connector = new ConnectorConfig
        {
            Schedule = new ScheduleSpec { IntervalMs = 1000 },
            File = new FilePollConfig { Path = path, Format = FileFormats.Ndjson },
            Mapping = Mapping(),
        },
    };

    private static SourceDefinition CloneEnabled(SourceDefinition def) => new()
    {
        Name = def.Name,
        Kind = def.Kind,
        Enabled = true,
        Fields = def.Fields,
        Connector = def.Connector,
    };

    private static string BuildNdjson(IEnumerable<long> ids) =>
        string.Concat(ids.Select(id => $"{{\"id\":{id},\"seq\":{id},\"value\":\"v\"}}\n"));

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(200);
        }
        return last;
    }

    private IRegistryGrain Registry => _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    /// <summary>The full disabled-source -> running-pipeline -> running-table -> enable-source setup this
    /// class's doc comment describes. Returns the two definitions AS STORED (the pipeline after its
    /// compile filled OutputFields, the table after its compile split StreamInputs/PipelineInputs), which
    /// is what the assertions are about.</summary>
    private async Task<(PipelineDefinition Pipeline, TableDefinition Table)> BuildChainAsync(string suffix, int parallelism)
    {
        var sourceName = "tpsrc_" + suffix;
        var pipelineName = "tppipe_" + suffix;
        var tableName = "tptbl_" + suffix;

        var filePath = Path.Combine(_scratchDir, sourceName + ".ndjson");
        await File.WriteAllTextAsync(filePath, BuildNdjson(Enumerable.Range(0, TotalRows).Select(i => (long)i)));

        var registry = Registry;
        var disabled = MakeFileSource(sourceName, filePath, enabled: false);
        await registry.UpsertSourceAsync(disabled);

        var pipeline = await registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = pipelineName,
            Sql = $"SELECT id, seq, value FROM {sourceName} WHERE seq < {KeptRows}",
        });
        await registry.SetPipelineStatusAsync(pipeline.Id, PipelineStatus.Running);

        var table = await registry.CreateTableAsync(new TableDefinition
        {
            Name = tableName,
            Sql = $"SELECT id, seq, value FROM {pipelineName} LATEST BY (id)",
            Parallelism = parallelism,
        });
        await registry.SetTableStatusAsync(table.Id, PipelineStatus.Running);

        await registry.UpsertSourceAsync(CloneEnabled(disabled));

        return (pipeline, table);
    }

    private static void AssertIdSet(List<TableRowDto> rows, IEnumerable<long> expectedIds)
    {
        var actual = rows.Select(r => Convert.ToInt64(r.Row["id"])).OrderBy(x => x).ToList();
        Assert.Equal(expectedIds.OrderBy(x => x).ToList(), actual);
    }

    // ================================================================================================
    // (a) the whole path, Parallelism == 1 (TableGrain classic)
    // ================================================================================================

    [Fact]
    public async Task A_table_reads_a_pipeline_by_name_and_lands_exactly_the_rows_that_pipeline_emits()
    {
        var (pipeline, table) = await BuildChainAsync(Guid.NewGuid().ToString("n")[..8], parallelism: 1);

        // The pipeline now publishes an output SCHEMA, which is the whole reason it can be a relation.
        Assert.Equal(3, pipeline.OutputFields.Count);
        Assert.Equal(["id", "seq", "value"], pipeline.OutputFields.Select(f => f.Name).ToArray());

        // The compiled stream-relation list was split: the pipeline is a pipeline input, and the table
        // reads NO source directly (the source is the pipeline's input, not this table's).
        Assert.Equal([pipeline.Name], table.PipelineInputs.ToArray());
        Assert.Empty(table.StreamInputs);
        Assert.Empty(table.TableInputs);

        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == KeptRows, deadlineSeconds: 45);

        // Settle, then assert the count did not keep climbing past the pipeline's filter.
        await Task.Delay(1500);
        Assert.Equal(KeptRows, await grain.GetRowCountAsync());
        AssertIdSet(await grain.GetRowsAsync(2000, 0), Enumerable.Range(0, KeptRows).Select(i => (long)i));
    }

    // ================================================================================================
    // (c) the same, Parallelism == 2 — covers TableIngestGrain's own pipeline branch
    // ================================================================================================

    [Fact]
    public async Task A_partitioned_table_reads_a_pipeline_through_its_ingest_grains()
    {
        var (pipeline, table) = await BuildChainAsync(Guid.NewGuid().ToString("n")[..8], parallelism: 2);

        Assert.Equal([pipeline.Name], table.PipelineInputs.ToArray());
        Assert.Equal(2, table.Parallelism);

        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == KeptRows, deadlineSeconds: 45);

        await Task.Delay(1500);
        Assert.Equal(KeptRows, await grain.GetRowCountAsync());
        AssertIdSet(await grain.GetRowsAsync(2000, 0), Enumerable.Range(0, KeptRows).Select(i => (long)i));
    }

    // ================================================================================================
    // (b) relation-name uniqueness, both directions
    // ================================================================================================

    [Fact]
    public async Task A_pipeline_cannot_take_the_name_of_an_existing_source()
    {
        var registry = Registry;
        var name = "tpclash_src_" + Guid.NewGuid().ToString("n")[..8];
        var filePath = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(filePath, BuildNdjson([0L]));
        await registry.UpsertSourceAsync(MakeFileSource(name, filePath, enabled: false));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            registry.CreatePipelineAsync(new PipelineDefinition { Name = name, Sql = $"SELECT id FROM {name}" }));
        Assert.Contains("already used by a stream source", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_source_cannot_take_the_name_of_an_existing_pipeline()
    {
        var registry = Registry;
        var suffix = Guid.NewGuid().ToString("n")[..8];
        var sourceName = "tpclash_base_" + suffix;
        var pipelineName = "tpclash_pipe_" + suffix;

        var filePath = Path.Combine(_scratchDir, sourceName + ".ndjson");
        await File.WriteAllTextAsync(filePath, BuildNdjson([0L]));
        await registry.UpsertSourceAsync(MakeFileSource(sourceName, filePath, enabled: false));
        await registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = pipelineName,
            Sql = $"SELECT id, seq, value FROM {sourceName}",
        });

        var clashPath = Path.Combine(_scratchDir, pipelineName + ".ndjson");
        await File.WriteAllTextAsync(clashPath, BuildNdjson([0L]));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            registry.UpsertSourceAsync(MakeFileSource(pipelineName, clashPath, enabled: false)));
        Assert.Contains("already used by a pipeline", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_table_cannot_take_the_name_of_an_existing_pipeline()
    {
        var registry = Registry;
        var suffix = Guid.NewGuid().ToString("n")[..8];
        var sourceName = "tpclash2_base_" + suffix;
        var pipelineName = "tpclash2_pipe_" + suffix;

        var filePath = Path.Combine(_scratchDir, sourceName + ".ndjson");
        await File.WriteAllTextAsync(filePath, BuildNdjson([0L]));
        await registry.UpsertSourceAsync(MakeFileSource(sourceName, filePath, enabled: false));
        await registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = pipelineName,
            Sql = $"SELECT id, seq, value FROM {sourceName}",
        });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            registry.CreateTableAsync(new TableDefinition
            {
                Name = pipelineName,
                Sql = $"SELECT id, seq, value FROM {sourceName}",
            }));
        Assert.Contains("already used by a pipeline", ex.Message, StringComparison.Ordinal);
    }

    // ================================================================================================
    // OutputFields is filled on the ordinary create path — the fact the whole feature rests on.
    // (The BOOT backfill for pipelines persisted before the field existed is covered in
    // PipelineLineageBackfillTests, beside the SourceNames backfill it extends.)
    // ================================================================================================

    [Fact]
    public async Task A_pipeline_that_does_not_compile_publishes_no_output_schema_and_so_offers_no_relation()
    {
        var registry = Registry;
        var suffix = Guid.NewGuid().ToString("n")[..8];
        var pipelineName = "tpdraft_pipe_" + suffix;

        // Draft SQL over a source that does not exist: create is deliberately NOT blocked (draft-friendly),
        // but nothing is published — so a table naming this pipeline gets an unknown-relation diagnostic
        // rather than compiling against an empty schema.
        var pipeline = await registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = pipelineName,
            Sql = "SELECT id FROM no_such_source_" + suffix,
        });
        Assert.Empty(pipeline.OutputFields);

        var table = await registry.CreateTableAsync(new TableDefinition
        {
            Name = "tpdraft_tbl_" + suffix,
            Sql = $"SELECT id FROM {pipelineName}",
        });
        Assert.Empty(table.PipelineInputs);
        Assert.Empty(table.OutputFields);
        Assert.Empty(table.StreamInputs);
        // Deliberately NOT asserting a non-empty table.Error: CreateTableAsync is draft-friendly and does
        // not store compile diagnostics on create (verified — only Start does). The signal that the
        // relation did not resolve is the empty derived state above, which is the thing this feature
        // actually depends on.
        Assert.Equal(PipelineStatus.Stopped, table.Status);
    }
}

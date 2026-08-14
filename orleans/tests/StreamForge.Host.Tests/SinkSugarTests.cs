using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore;
using StreamForge.AppCore.Config;
using StreamForge.AppCore.Sql;
using StreamForge.Engine;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 014 wave K: <see cref="SinkSugar"/> — the <c>INSERT INTO &lt;sink&gt; SELECT …</c> pre-parse
/// strip, and the write-path resolution of that target against an entity's <see cref="SinkSpec"/> list.
///
/// <para>The first test here is the one that matters most and the one everything else is allowed to be
/// interesting around: EVERY seeded query passes through the desugarer as the same string instance. A
/// pre-parse rewrite of every statement the platform stores is exactly the kind of change that quietly
/// eats a query it did not understand, so "did not touch it" is asserted by reference, not by equality.
/// </para>
///
/// <para>The wiring is covered where this repo covers endpoint behavior: there is no HTTP-level test
/// harness (see ConfigEndpoints.cs's class doc), so the endpoints' unknown-sink 400 is
/// <see cref="SinkSugar.ApplyTo"/>'s diagnostic, tested directly, and the config-import site is driven
/// end to end through <see cref="ConfigImportService.RunImportAsync"/> against an in-memory catalog —
/// which proves the sugar reaches storage, not just that the parser works.</para>
/// </summary>
public class SinkSugarTests
{
    // ------------------------------------------------------------------
    // Identity: nothing that is not sugar may be touched.
    // ------------------------------------------------------------------

    [Fact]
    public void Every_seeded_pipeline_and_table_query_passes_through_untouched()
    {
        var seeded = SeedCatalog.Pipelines().Select(p => p.Sql)
            .Concat(SeedCatalog.Tables().Select(t => t.Sql))
            .ToList();

        Assert.NotEmpty(seeded);
        foreach (var sql in seeded)
        {
            var result = SinkSugar.Desugar(sql);
            Assert.Same(sql, result.Sql);
            Assert.Null(result.SinkName);
            Assert.Empty(result.Diagnostics);
        }
    }

    [Theory]
    // The dialect's statement shapes, plus the two ways "insert into" shows up in a query that is not
    // sugar at all: inside a string literal, and inside a later subquery.
    [InlineData("SELECT 1")]
    [InlineData("SELECT symbol, price FROM trades")]
    [InlineData("WITH hot AS (SELECT symbol FROM trades) SELECT symbol FROM hot")]
    [InlineData("SELECT order_id, stage FROM order_events LATEST BY (order_id)")]
    [InlineData("SELECT 'insert into warehouse' AS note FROM trades")]
    [InlineData("SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM hot) -- insert into warehouse")]
    [InlineData("   \n\t SELECT 1")]
    [InlineData("-- destination: decided in the Sinks tab\nSELECT 1")]
    [InlineData("INSERTED INTO warehouse SELECT 1")]
    public void A_statement_that_does_not_start_with_INSERT_is_returned_byte_identical(string sql)
    {
        var result = SinkSugar.Desugar(sql);

        Assert.Same(sql, result.Sql);
        Assert.Null(result.SinkName);
        Assert.Empty(result.Diagnostics);
    }

    [Fact]
    public void An_INSERT_INTO_that_is_not_at_the_start_is_text_not_sugar()
    {
        // The trailing occurrence is inside a UNION branch — the match is anchored at the statement's
        // first token precisely so that this is a query about the word, not a second destination.
        const string sql = "SELECT 1 UNION ALL SELECT 2 -- INSERT INTO warehouse";
        Assert.Same(sql, SinkSugar.Desugar(sql).Sql);
    }

    // ------------------------------------------------------------------
    // The strip itself.
    // ------------------------------------------------------------------

    [Fact]
    public void Strips_the_target_and_reports_the_sink_name()
    {
        var result = SinkSugar.Desugar("INSERT INTO warehouse SELECT a, b FROM trades");

        Assert.Equal("warehouse", result.SinkName);
        Assert.Equal("SELECT a, b FROM trades", result.Sql);
        Assert.Empty(result.Diagnostics);
    }

    [Theory]
    [InlineData("insert into warehouse SELECT 1", "SELECT 1")]
    [InlineData("Insert Into warehouse select 1", "select 1")]
    [InlineData("INSERT    INTO     warehouse   SELECT 1", "SELECT 1")]
    [InlineData("INSERT\nINTO\n  warehouse\nSELECT 1", "SELECT 1")]
    [InlineData("\n  INSERT INTO warehouse\r\n\tSELECT 1", "SELECT 1")]
    public void Keywords_are_case_insensitive_and_any_whitespace_separates_them(string sql, string stripped)
    {
        var result = SinkSugar.Desugar(sql);

        Assert.Equal("warehouse", result.SinkName);
        // The stripped SQL keeps the query's own casing and shape — only the prefix is removed.
        Assert.Equal(stripped, result.Sql);
        Assert.Empty(result.Diagnostics);
    }

    [Fact]
    public void A_WITH_query_may_carry_a_destination_too()
    {
        var result = SinkSugar.Desugar(
            "INSERT INTO warehouse WITH hot AS (SELECT symbol FROM trades) SELECT symbol FROM hot");

        Assert.Equal("warehouse", result.SinkName);
        Assert.StartsWith("WITH hot AS", result.Sql);
    }

    [Theory]
    [InlineData("INSERT INTO warehouse_2 SELECT 1", "warehouse_2")]
    [InlineData("INSERT INTO _wh SELECT 1", "_wh")]
    [InlineData("INSERT INTO WareHouse SELECT 1", "WareHouse")]
    public void The_target_is_a_bare_identifier_shaped_exactly_like_every_other_name_in_the_dialect(
        string sql, string expected) =>
        Assert.Equal(expected, SinkSugar.Desugar(sql).SinkName);

    [Fact]
    public void A_leading_line_comment_does_not_hide_the_sugar_and_is_dropped_with_the_prefix()
    {
        // Decision, pinned: leading trivia is what the Engine's tokenizer calls trivia (whitespace and
        // '--' line comments), so a commented statement still desugars — and the comment goes with the
        // prefix, since everything before the query keyword is what the strip removes.
        var result = SinkSugar.Desugar("-- nightly load\nINSERT INTO warehouse SELECT 1");

        Assert.Equal("warehouse", result.SinkName);
        Assert.Equal("SELECT 1", result.Sql);
    }

    [Fact]
    public void A_block_comment_is_not_trivia_because_this_dialect_has_none()
    {
        // Not sugar, and deliberately not a diagnostic of ours either: '/*' is already invalid in this
        // dialect, and the tokenizer's own error about it is the better message.
        const string sql = "/* nightly load */ INSERT INTO warehouse SELECT 1";
        var result = SinkSugar.Desugar(sql);

        Assert.Same(sql, result.Sql);
        Assert.Null(result.SinkName);
        Assert.Empty(result.Diagnostics);
        Assert.False(SqlCompiler.Compile(sql, new Dictionary<string, SourceSchema>()).Ok);
    }

    // ------------------------------------------------------------------
    // Malformed sugar: a diagnostic, and the ORIGINAL statement back.
    // ------------------------------------------------------------------

    [Theory]
    [InlineData("INSERT INTO SELECT 1")]                        // no target (SELECT is not an identifier target...)
    [InlineData("INSERT INTO")]                                 // ...nor is nothing at all
    [InlineData("INSERT INTO 7up SELECT 1")]                    // not an identifier
    [InlineData("INSERT INTO warehouse")]                       // target, no query
    [InlineData("INSERT INTO warehouse VALUES (1, 2)")]         // VALUES is not a stream
    [InlineData("INSERT INTO warehouse (a, b) SELECT a, b FROM trades")] // no column list
    [InlineData("INSERT INTO warehouse, archive SELECT 1")]     // one target only
    [InlineData("INSERT warehouse SELECT 1")]                   // INSERT without INTO
    [InlineData("INSERT INTO \"my warehouse\" SELECT 1")]       // quoted identifier: not in this dialect
    [InlineData("INSERT INTO [warehouse] SELECT 1")]            // bracketed: likewise
    public void Malformed_sugar_diagnoses_and_returns_the_statement_unchanged(string sql)
    {
        var result = SinkSugar.Desugar(sql);

        Assert.Same(sql, result.Sql);
        Assert.Null(result.SinkName);
        Assert.Single(result.Diagnostics);
    }

    [Fact]
    public void The_INSERT_INTO_SELECT_case_is_diagnosed_as_a_missing_target_not_a_missing_query()
    {
        // 'SELECT' is a perfectly good identifier by shape, so without the keyword check the strip would
        // take it as the sink name and then report that no query followed — the second symptom, not the
        // mistake.
        var result = SinkSugar.Desugar("INSERT INTO SELECT 1");
        Assert.Contains("needs a sink name", result.Diagnostics[0]);
    }

    [Fact]
    public void The_quoted_target_diagnostic_says_what_to_do_about_it()
    {
        var result = SinkSugar.Desugar("INSERT INTO \"my warehouse\" SELECT 1");
        Assert.Contains("bare identifier", result.Diagnostics[0]);
        Assert.Contains("rename the sink", result.Diagnostics[0]);
    }

    // ------------------------------------------------------------------
    // ApplyTo — resolving the target against the entity's sinks (the endpoints' whole 400 decision).
    // ------------------------------------------------------------------

    [Fact]
    public void ApplyTo_enables_the_named_sink_and_leaves_the_others_alone()
    {
        var sinks = new List<SinkSpec> { Sink("archive", enabled: false), Sink("warehouse", enabled: false) };

        var result = SinkSugar.ApplyTo("INSERT INTO warehouse SELECT 1", sinks, "pipeline");

        Assert.Empty(result.Diagnostics);
        Assert.Equal("SELECT 1", result.Sql);
        Assert.True(sinks[1].Enabled);
        Assert.False(sinks[0].Enabled);
    }

    [Fact]
    public void ApplyTo_rejects_an_unknown_target_by_name_rather_than_guessing_at_a_positional_sink()
    {
        var sinks = new List<SinkSpec> { Sink("archive", enabled: false) };

        var result = SinkSugar.ApplyTo("INSERT INTO warehouse SELECT 1", sinks, "pipeline");

        Assert.Equal("no sink named 'warehouse' on this pipeline — add it in Sinks first", Assert.Single(result.Diagnostics));
        Assert.Equal("INSERT INTO warehouse SELECT 1", result.Sql);
        Assert.False(sinks[0].Enabled);
    }

    [Fact]
    public void ApplyTo_names_the_entity_the_operator_is_looking_at()
    {
        var result = SinkSugar.ApplyTo("INSERT INTO warehouse SELECT 1", [], "table");
        Assert.Contains("on this table", Assert.Single(result.Diagnostics));
    }

    [Fact]
    public void ApplyTo_matches_the_sink_name_ordinally()
    {
        // Same rule by which a source name in the very same statement resolves against the catalog.
        var sinks = new List<SinkSpec> { Sink("Warehouse", enabled: false) };

        Assert.Single(SinkSugar.ApplyTo("INSERT INTO warehouse SELECT 1", sinks, "pipeline").Diagnostics);
        Assert.False(sinks[0].Enabled);
        Assert.Empty(SinkSugar.ApplyTo("INSERT INTO Warehouse SELECT 1", sinks, "pipeline").Diagnostics);
        Assert.True(sinks[0].Enabled);
    }

    [Fact]
    public void ApplyTo_leaves_the_sinks_alone_when_there_is_no_sugar()
    {
        var sinks = new List<SinkSpec> { Sink("warehouse", enabled: false) };

        var result = SinkSugar.ApplyTo("SELECT 1", sinks, "pipeline");

        Assert.Empty(result.Diagnostics);
        Assert.False(sinks[0].Enabled);
    }

    [Fact]
    public void ApplyTo_never_resolves_the_empty_name_every_pre_014_sink_carries()
    {
        // SinkSpec.Name is "" on every sink authored before wave A. There is no syntax that produces an
        // empty target (the identifier read requires a character), so those sinks are simply unaddressable
        // until named — which is the honest outcome, and the reason the unknown-name error exists.
        var sinks = new List<SinkSpec> { Sink("", enabled: false) };

        Assert.Single(SinkSugar.ApplyTo("INSERT INTO warehouse SELECT 1", sinks, "pipeline").Diagnostics);
        Assert.False(sinks[0].Enabled);
    }

    [Fact]
    public void ApplyTo_passes_a_malformed_sugar_diagnostic_straight_through()
    {
        var sinks = new List<SinkSpec> { Sink("warehouse", enabled: false) };

        var result = SinkSugar.ApplyTo("INSERT INTO warehouse VALUES (1)", sinks, "pipeline");

        Assert.Single(result.Diagnostics);
        Assert.False(sinks[0].Enabled);
    }

    // ------------------------------------------------------------------
    // The config-import wiring site, end to end against an in-memory catalog.
    // ------------------------------------------------------------------

    [Fact]
    public async Task Import_stores_the_stripped_query_and_enables_the_named_sink()
    {
        var catalog = new MemoryCatalog();
        catalog.Sources.Add(TradesSource());
        var doc = new ConfigDocument
        {
            Pipelines =
            [
                new ConfigPipeline
                {
                    Name = "to-warehouse",
                    Sql = "INSERT INTO warehouse SELECT symbol FROM trades",
                    Sinks = [Sink("warehouse", enabled: false)],
                },
            ],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", catalog, apply: true);

        Assert.True(report.Ok);
        var stored = Assert.Single(catalog.Pipelines);
        Assert.Equal("SELECT symbol FROM trades", stored.Sql);
        Assert.True(Assert.Single(stored.Sinks).Enabled);
    }

    [Fact]
    public async Task Import_reports_an_unknown_sink_target_as_an_error_and_applies_nothing()
    {
        var catalog = new MemoryCatalog();
        catalog.Sources.Add(TradesSource());
        var doc = new ConfigDocument
        {
            Pipelines =
            [
                new ConfigPipeline
                {
                    Name = "to-warehouse",
                    Sql = "INSERT INTO warehouse SELECT symbol FROM trades",
                    Sinks = [Sink("archive", enabled: false)],
                },
            ],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", catalog, apply: true);

        Assert.False(report.Ok);
        var entry = Assert.Single(report.Entries);
        Assert.Equal("error", entry.Action);
        Assert.Equal("no sink named 'warehouse' on this pipeline — add it in Sinks first", Assert.Single(entry.Diagnostics));
        Assert.Empty(catalog.Pipelines);
    }

    [Fact]
    public async Task Import_in_validate_mode_reports_the_same_unknown_target_it_would_hit_on_apply()
    {
        var catalog = new MemoryCatalog();
        catalog.Sources.Add(TradesSource());
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable
                {
                    Name = "warehoused",
                    Sql = "INSERT INTO warehouse SELECT symbol FROM trades",
                    Sinks = [],
                },
            ],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", catalog, apply: false);

        Assert.False(report.Ok);
        Assert.Equal("no sink named 'warehouse' on this table — add it in Sinks first", Assert.Single(Assert.Single(report.Entries).Diagnostics));
        Assert.Empty(catalog.Tables);
    }

    // ------------------------------------------------------------------
    // Fixtures.
    // ------------------------------------------------------------------

    private static SinkSpec Sink(string name, bool enabled) => new()
    {
        Kind = SinkKinds.Nats,
        Name = name,
        Enabled = enabled,
        Nats = new NatsPubConfig { Url = "nats://localhost:4222", Subject = "sf.out" },
    };

    private static SourceDefinition TradesSource() => new()
    {
        Name = "trades",
        Kind = SourceKinds.Generator,
        Fields = [new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double)],
    };

    /// <summary>The smallest <see cref="ICatalogFacade"/> that can run an import: lists in memory, no
    /// compile mirroring (ConfigImportService compiles before it writes, and nothing here reads
    /// OutputFields back). Deliberately its own class rather than a share of
    /// ConfigEndpointsLogicTests' richer fake — that one is private to its file, and a pre-existing test
    /// file is not something this wave edits.</summary>
    private sealed class MemoryCatalog : ICatalogFacade
    {
        private int _nextId;

        public List<SourceDefinition> Sources { get; } = [];
        public List<PipelineDefinition> Pipelines { get; } = [];
        public List<TableDefinition> Tables { get; } = [];

        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(new List<SourceDefinition>(Sources));
        public Task<SourceDefinition?> GetSourceAsync(string name) => Task.FromResult(Sources.FirstOrDefault(s => s.Name == name));
        public Task UpsertSourceAsync(SourceDefinition def)
        {
            Sources.RemoveAll(s => s.Name == def.Name);
            Sources.Add(def);
            return Task.CompletedTask;
        }

        public Task<bool> DeleteSourceAsync(string name) => Task.FromResult(Sources.RemoveAll(s => s.Name == name) > 0);

        public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(new List<PipelineDefinition>(Pipelines));
        public Task<PipelineDefinition?> GetPipelineAsync(string id) => Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));
        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def)
        {
            def.Id = $"pl-{++_nextId}";
            Pipelines.Add(def);
            return Task.FromResult(def);
        }

        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def)
        {
            var idx = Pipelines.FindIndex(p => p.Id == def.Id);
            if (idx < 0) return Task.FromResult<PipelineDefinition?>(null);
            Pipelines[idx] = def;
            return Task.FromResult<PipelineDefinition?>(def);
        }

        public Task<bool> DeletePipelineAsync(string id) => Task.FromResult(Pipelines.RemoveAll(p => p.Id == id) > 0);
        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status)
        {
            var p = Pipelines.FirstOrDefault(p => p.Id == id);
            if (p is not null) p.Status = status;
            return Task.FromResult(p);
        }

        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(new List<TableDefinition>(Tables));
        public Task<TableDefinition?> GetTableAsync(string id) => Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));
        public Task<TableDefinition> CreateTableAsync(TableDefinition def)
        {
            def.Id = $"tb-{++_nextId}";
            Tables.Add(def);
            return Task.FromResult(def);
        }

        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def)
        {
            var idx = Tables.FindIndex(t => t.Id == def.Id);
            if (idx < 0) return Task.FromResult<TableDefinition?>(null);
            Tables[idx] = def;
            return Task.FromResult<TableDefinition?>(def);
        }

        public Task<bool> DeleteTableAsync(string id) => Task.FromResult(Tables.RemoveAll(t => t.Id == id) > 0);
        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status)
        {
            var t = Tables.FirstOrDefault(t => t.Id == id);
            if (t is not null) t.Status = status;
            return Task.FromResult(t);
        }

        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotImplementedException();
    }
}

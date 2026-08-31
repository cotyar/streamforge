using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.AppCore.Config;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 006 (W4): unit tests for the pure/near-pure logic behind SourcesEndpoints.cs —
/// <see cref="SourceValidation.Validate"/> (kind-aware validation, every rule accept + reject),
/// <see cref="SecretsMasker"/>'s mask-on-read/merge-on-write round-trip invariant (D-H's critical
/// "GET -> PUT must not clobber stored secrets" guarantee), the gRPC entity-key regex, and the
/// status 204-vs-200-vs-404 decision (<see cref="SourceSchemaService.DecideStatusOutcome"/>). There
/// is no HTTP-level test harness in this repo (see ConfigEndpoints.cs's class doc comment) — this
/// file is the whole test surface for plan 006's sources REST endpoints.
/// </summary>
public class SourcesEndpointsLogicTests
{
    private static SourceDefinition Def(string kind = SourceKinds.Generator, ConnectorConfig? connector = null, string name = "s") => new()
    {
        Name = name,
        Fields = [new FieldDef("price", FieldType.Double)],
        Kind = kind,
        Connector = connector,
        EventsPerSecond = 5,
    };

    // ------------------------------------------------------------------
    // Generic rules (all kinds).
    // ------------------------------------------------------------------

    [Fact]
    public void Validate_accepts_a_well_formed_generator_source()
    {
        Assert.Empty(SourceValidation.Validate(Def()));
    }

    [Fact]
    public void Validate_rejects_missing_name()
    {
        var def = Def();
        def.Name = "  ";
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("name is required"));
    }

    [Fact]
    public void Validate_rejects_empty_fields_for_every_kind()
    {
        var def = Def(SourceKinds.Url, new ConnectorConfig { Url = new UrlPollConfig { Url = "http://x" } });
        def.Fields = [];
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("at least one field is required"));
    }

    [Fact]
    public void Validate_rejects_unknown_kind()
    {
        var def = Def("not-a-kind");
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("not recognized"));
    }

    [Fact]
    public void Validate_reports_every_problem_at_once()
    {
        var def = Def();
        def.Name = "";
        def.Fields = [];
        def.EventsPerSecond = 0;

        var errors = SourceValidation.Validate(def);
        Assert.Equal(3, errors.Count);
    }

    // ------------------------------------------------------------------
    // generator kind (existing behavior, unchanged).
    // ------------------------------------------------------------------

    [Fact]
    public void Validate_generator_requires_positive_eventsPerSecond()
    {
        var def = Def();
        def.EventsPerSecond = 0;
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("eventsPerSecond must be > 0"));
    }

    [Fact]
    public void Validate_generator_does_not_require_a_connector()
    {
        Assert.Empty(SourceValidation.Validate(Def()));
    }

    // ------------------------------------------------------------------
    // url kind.
    // ------------------------------------------------------------------

    [Fact]
    public void Validate_url_requires_connector()
    {
        var def = Def(SourceKinds.Url);
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("requires a connector configuration"));
    }

    [Fact]
    public void Validate_url_requires_a_non_empty_url()
    {
        var def = Def(SourceKinds.Url, new ConnectorConfig { Url = new UrlPollConfig { Url = "" } });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("connector.url.url is required"));
    }

    [Theory]
    [InlineData("not-a-url")]
    [InlineData("ftp://example.test/file")]
    [InlineData("/relative/path")]
    public void Validate_url_rejects_non_absolute_http_urls(string url)
    {
        var def = Def(SourceKinds.Url, new ConnectorConfig { Url = new UrlPollConfig { Url = url } });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("absolute http(s) URL"));
    }

    [Fact]
    public void Validate_url_accepts_a_well_formed_config()
    {
        var def = Def(SourceKinds.Url, new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "https://example.test/api" },
            Schedule = new ScheduleSpec { IntervalMs = 5000 },
        });
        Assert.Empty(SourceValidation.Validate(def));
    }

    [Fact]
    public void Validate_url_allows_absent_schedule_default_30s()
    {
        var def = Def(SourceKinds.Url, new ConnectorConfig { Url = new UrlPollConfig { Url = "http://x" } });
        Assert.Empty(SourceValidation.Validate(def));
    }

    [Fact]
    public void Validate_url_rejects_an_invalid_schedule()
    {
        var def = Def(SourceKinds.Url, new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://x" },
            Schedule = new ScheduleSpec { IntervalMs = 10 }, // below the 1s floor
        });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("connector.schedule:"));
    }

    // ------------------------------------------------------------------
    // file kind.
    // ------------------------------------------------------------------

    [Fact]
    public void Validate_file_requires_path()
    {
        var def = Def(SourceKinds.File, new ConnectorConfig { File = new FilePollConfig { Path = "" } });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("connector.file.path is required"));
    }

    [Fact]
    public void Validate_file_rejects_unknown_format()
    {
        var def = Def(SourceKinds.File, new ConnectorConfig { File = new FilePollConfig { Path = "/tmp/x", Format = "xml" } });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("format 'xml' is not recognized"));
    }

    [Theory]
    [InlineData(FileFormats.Ndjson)]
    [InlineData(FileFormats.JsonArray)]
    [InlineData(FileFormats.Csv)]
    public void Validate_file_accepts_every_known_format(string format)
    {
        var def = Def(SourceKinds.File, new ConnectorConfig { File = new FilePollConfig { Path = "/tmp/x", Format = format } });
        Assert.Empty(SourceValidation.Validate(def));
    }

    // ------------------------------------------------------------------
    // folder kind.
    // ------------------------------------------------------------------

    [Fact]
    public void Validate_folder_requires_path_and_format()
    {
        var def = Def(SourceKinds.Folder, new ConnectorConfig { Folder = new FolderPollConfig { Path = "", Format = "bogus" } });
        var errors = SourceValidation.Validate(def);
        Assert.Contains(errors, e => e.Contains("connector.folder.path is required"));
        Assert.Contains(errors, e => e.Contains("format 'bogus' is not recognized"));
    }

    [Fact]
    public void Validate_folder_accepts_a_well_formed_config()
    {
        var def = Def(SourceKinds.Folder, new ConnectorConfig { Folder = new FolderPollConfig { Path = "/tmp", Format = FileFormats.Csv, Glob = "*.csv" } });
        Assert.Empty(SourceValidation.Validate(def));
    }

    // ------------------------------------------------------------------
    // grpc kind.
    // ------------------------------------------------------------------

    [Fact]
    public void Validate_grpc_requires_address_and_entityKey()
    {
        var def = Def(SourceKinds.Grpc, new ConnectorConfig { Grpc = new GrpcSubConfig { Address = "", EntityKey = "" } });
        var errors = SourceValidation.Validate(def);
        Assert.Contains(errors, e => e.Contains("connector.grpc.address is required"));
        Assert.Contains(errors, e => e.Contains("entityKey must match"));
    }

    [Theory]
    [InlineData("source:trades", true)]
    [InlineData("pipeline:p1", true)]
    [InlineData("table:t1", true)]
    [InlineData("bogus:trades", false)]
    [InlineData("source:", false)]
    [InlineData("trades", false)]
    [InlineData("", false)]
    public void GrpcEntityKeyPattern_matches_the_documented_shape(string key, bool expected)
    {
        Assert.Equal(expected, key.Length > 0 && SourceValidation.GrpcEntityKeyPattern.IsMatch(key));
    }

    [Fact]
    public void Validate_grpc_proto_schemaSource_requires_protoText()
    {
        var def = Def(SourceKinds.Grpc, new ConnectorConfig { Grpc = new GrpcSubConfig { Address = "http://x", EntityKey = "source:t", SchemaSource = "proto", ProtoText = null } });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("protoText is required"));
    }

    [Fact]
    public void Validate_grpc_rejects_unknown_schemaSource()
    {
        var def = Def(SourceKinds.Grpc, new ConnectorConfig { Grpc = new GrpcSubConfig { Address = "http://x", EntityKey = "source:t", SchemaSource = "xml" } });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("schemaSource must be"));
    }

    [Fact]
    public void Validate_grpc_username_requires_restAddress()
    {
        var def = Def(SourceKinds.Grpc, new ConnectorConfig
        {
            Grpc = new GrpcSubConfig { Address = "http://x", EntityKey = "source:t", Username = "editor", Password = "pw", RestAddress = null },
        });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("restAddress is required"));
    }

    [Fact]
    public void Validate_grpc_accepts_a_well_formed_reflection_config()
    {
        var def = Def(SourceKinds.Grpc, new ConnectorConfig
        {
            Grpc = new GrpcSubConfig { Address = "http://localhost:5299", EntityKey = "source:trades", Username = "editor", Password = "pw", RestAddress = "http://localhost:5199" },
        });
        Assert.Empty(SourceValidation.Validate(def));
    }

    // ------------------------------------------------------------------
    // Mapping validation (Connector.Mapping — the structured, already-parsed form).
    // ------------------------------------------------------------------

    [Fact]
    public void Validate_mapping_rejects_an_invalid_path()
    {
        var def = Def(SourceKinds.Url, new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://x" },
            Mapping = new MappingSpec
            {
                ItemsPath = "$..bad",
                Fields = [new FieldMapEntry { Field = new FieldDef("price", FieldType.Double) }],
            },
        });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("connector.mapping.itemsPath"));
    }

    [Fact]
    public void Validate_mapping_rejects_dedupKeyField_not_among_mapped_fields()
    {
        var def = Def(SourceKinds.Url, new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://x" },
            Mapping = new MappingSpec
            {
                DedupKeyField = "missing",
                Fields = [new FieldMapEntry { Field = new FieldDef("price", FieldType.Double) }],
            },
        });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("dedupKeyField 'missing' is not among the mapped fields"));
    }

    [Fact]
    public void Validate_mapping_rejects_timestampField_not_among_mapped_fields()
    {
        var def = Def(SourceKinds.Url, new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://x" },
            Mapping = new MappingSpec
            {
                TimestampField = "missing",
                Fields = [new FieldMapEntry { Field = new FieldDef("price", FieldType.Double) }],
            },
        });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("timestampField 'missing' is not among the mapped fields"));
    }

    [Fact]
    public void Validate_mapping_accepts_a_well_formed_spec()
    {
        var def = Def(SourceKinds.Url, new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://x" },
            Mapping = new MappingSpec
            {
                ItemsPath = "$.data[*]",
                DedupKeyField = "id",
                TimestampField = "ts",
                Fields =
                [
                    new FieldMapEntry { SourcePath = "id", Field = new FieldDef("id", FieldType.String) },
                    new FieldMapEntry { SourcePath = "ts", Field = new FieldDef("ts", FieldType.Timestamp) },
                    new FieldMapEntry { Field = new FieldDef("price", FieldType.Double) },
                ],
            },
        });
        Assert.Empty(SourceValidation.Validate(def));
    }

    [Fact]
    public void Validate_mapping_is_ignored_for_grpc_kind()
    {
        // grpc is a persistent subscription: Mapping/Schedule are ignored by the driver (D-B), so
        // an invalid mapping (or schedule) on a grpc-kind source must not fail validation.
        var def = Def(SourceKinds.Grpc, new ConnectorConfig
        {
            Grpc = new GrpcSubConfig { Address = "http://x", EntityKey = "source:t" },
            Mapping = new MappingSpec { ItemsPath = "$..bad" }, // would be invalid on url/file/folder
            Schedule = new ScheduleSpec { IntervalMs = 1 },     // below the 1s floor — also ignored
        });
        Assert.Empty(SourceValidation.Validate(def));
    }

    // ------------------------------------------------------------------
    // D-H: mask-on-read / merge-on-write round-trip invariant.
    // ------------------------------------------------------------------

    [Fact]
    public void GetThenPut_round_trip_does_not_clobber_the_stored_secret()
    {
        var stored = Def(SourceKinds.Url, new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://x", Headers = { ["Authorization"] = "Bearer real-secret-123" } },
        });

        // Simulate GET (masked) -> the SPA edits something unrelated -> PUT the whole object back.
        var fetched = SecretsMasker.Mask(stored);
        Assert.Equal(SourceKinds.SecretMask, fetched.Connector!.Url!.Headers["Authorization"]);

        var effective = SecretsMasker.MergeSecrets(fetched, stored);

        Assert.Equal("Bearer real-secret-123", effective.Connector!.Url!.Headers["Authorization"]);
        // The response the endpoint hands back to the caller is masked again — the real value
        // never appears in a PUT response body either.
        Assert.Equal(SourceKinds.SecretMask, SecretsMasker.Mask(effective).Connector!.Url!.Headers["Authorization"]);
    }

    [Fact]
    public void MergeSecrets_lets_a_genuinely_new_secret_value_through()
    {
        var stored = Def(SourceKinds.Url, new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://x", Headers = { ["Authorization"] = "Bearer old-secret" } },
        });

        var incoming = Def(SourceKinds.Url, new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://x", Headers = { ["Authorization"] = "Bearer new-secret" } },
        });

        var effective = SecretsMasker.MergeSecrets(incoming, stored);
        Assert.Equal("Bearer new-secret", effective.Connector!.Url!.Headers["Authorization"]);
    }

    [Fact]
    public void MergeSecrets_on_create_leaves_a_mask_typed_by_mistake_as_is()
    {
        // POST (create): there is no stored source to restore from — MergeSecrets(def, null) is a
        // documented no-op, so a literal "***" typed into a brand-new source is left alone (there
        // is nothing to "keep").
        var incoming = Def(SourceKinds.Url, new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://x", Headers = { ["Authorization"] = SourceKinds.SecretMask } },
        });

        var effective = SecretsMasker.MergeSecrets(incoming, null);
        Assert.Equal(SourceKinds.SecretMask, effective.Connector!.Url!.Headers["Authorization"]);
    }

    [Fact]
    public void GetThenPut_round_trip_preserves_grpc_password_and_token()
    {
        var stored = Def(SourceKinds.Grpc, new ConnectorConfig
        {
            Grpc = new GrpcSubConfig { Address = "http://x", EntityKey = "source:t", Password = "hunter2", Token = "static-tok" },
        });

        var fetched = SecretsMasker.Mask(stored);
        Assert.Equal(SourceKinds.SecretMask, fetched.Connector!.Grpc!.Password);
        Assert.Equal(SourceKinds.SecretMask, fetched.Connector!.Grpc!.Token);

        var effective = SecretsMasker.MergeSecrets(fetched, stored);
        Assert.Equal("hunter2", effective.Connector!.Grpc!.Password);
        Assert.Equal("static-tok", effective.Connector!.Grpc!.Token);
    }

    // ------------------------------------------------------------------
    // Status 204-vs-200-vs-404 decision.
    // ------------------------------------------------------------------

    [Fact]
    public void DecideStatusOutcome_is_NotFound_when_the_source_does_not_exist()
    {
        Assert.Equal(SourceStatusOutcome.NotFound, SourceSchemaService.DecideStatusOutcome(sourceExists: false, status: null));
        // Even a non-null status can't happen for a nonexistent source in practice, but the
        // decision itself must still prioritize NotFound if it somehow were supplied.
        Assert.Equal(SourceStatusOutcome.NotFound, SourceSchemaService.DecideStatusOutcome(sourceExists: false, status: new ConnectorRuntimeStatus()));
    }

    [Fact]
    public void DecideStatusOutcome_is_NoContent_for_a_generator_kind_source()
    {
        // Generator-kind sources have no ConnectorRuntimeStatus tracked (IConnectorStatusFacade
        // returns null) — 204, not 404, since the source itself does exist.
        Assert.Equal(SourceStatusOutcome.NoContent, SourceSchemaService.DecideStatusOutcome(sourceExists: true, status: null));
    }

    [Fact]
    public void DecideStatusOutcome_is_Ok_for_a_connector_kind_source_with_tracked_status()
    {
        var status = new ConnectorRuntimeStatus { SourceName = "s", LastStatus = "ok", ConsecutiveFailures = 0 };
        Assert.Equal(SourceStatusOutcome.Ok, SourceSchemaService.DecideStatusOutcome(sourceExists: true, status: status));
    }

    // ------------------------------------------------------------------
    // POST /api/sources/schema/mapping-validate (pure — direct call, no HTTP harness).
    // ------------------------------------------------------------------

    [Fact]
    public void ValidateMappingDocument_reports_ok_and_preview_rows_for_a_good_document_and_sample()
    {
        var request = new MappingValidateRequest
        {
            Document = """{"itemsPath":"$.data[*]","fields":[{"field":{"name":"price","type":"Double"}}]}""",
            Sample = """{"data":[{"price":1.5},{"price":2.5}]}""",
        };

        var result = SourceSchemaService.ValidateMappingDocument(request);

        Assert.True(result.Ok);
        Assert.Empty(result.Diagnostics);
        Assert.Equal(2, result.PreviewRows.Count);
        Assert.Equal(1.5, result.PreviewRows[0]["price"]);
    }

    [Fact]
    public void ValidateMappingDocument_reports_diagnostics_for_a_bad_document()
    {
        var request = new MappingValidateRequest { Document = "{not valid json or yaml: [" };

        var result = SourceSchemaService.ValidateMappingDocument(request);

        Assert.False(result.Ok);
        Assert.NotEmpty(result.Diagnostics);
        Assert.Empty(result.PreviewRows);
    }

    [Fact]
    public void ValidateMappingDocument_flags_a_sample_that_is_not_valid_json()
    {
        var request = new MappingValidateRequest
        {
            Document = """{"fields":[{"field":{"name":"price","type":"Double"}}]}""",
            Sample = "not json",
        };

        var result = SourceSchemaService.ValidateMappingDocument(request);

        Assert.False(result.Ok);
        Assert.Contains(result.Diagnostics, d => d.Contains("sample is not valid JSON"));
    }
}

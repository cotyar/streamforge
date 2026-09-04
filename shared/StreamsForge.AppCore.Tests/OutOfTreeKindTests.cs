using StreamsForge.Abstractions;
using StreamsForge.AppCore.Config;
using StreamsForge.AppCore.Plugins;
using StreamsForge.AppCore.Sinks;
using StreamsForge.AppCore.Transports;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>
/// The seams an OUT-OF-TREE connector stands on: the open <c>Settings</c> bag it keeps its config in
/// (<see cref="ConnectorConfig.Settings"/>), the descriptor-driven masking that keeps a credential in that
/// bag from exporting in plaintext, and the plugin loader that installs the whole thing without this repo
/// referencing it.
///
/// <para>In the SHARED test project on purpose: an out-of-tree kind is configured, masked and imported by
/// code both flavors run, and a masking rule that held on one flavor but not the other would leak a
/// credential nobody would look for.</para>
///
/// <para><b>Registration hygiene</b>, same convention as the other fake-transport suites in this repo:
/// the registries are process-global and permanent, so the fakes register exactly once from the static
/// constructor under names distinctive enough not to collide ("quuxbag", "quuxbag-sink").</para>
/// </summary>
public class OutOfTreeKindTests
{
    private const string BagKind = "quuxbag";
    private const string BagSinkKind = "quuxbag-sink";

    static OutOfTreeKindTests()
    {
        InboundTransports.Register(new BagTransport());
        SinkTransports.Register(new BagSinkTransport());
    }

    // ------------------------------------------------------------------
    // SettingsBag readers.
    // ------------------------------------------------------------------

    [Fact]
    public void Readers_fall_back_rather_than_throw_on_absent_blank_and_unparseable_values()
    {
        var bag = new Dictionary<string, string>
        {
            ["daemon"] = "  tcp:7500  ",
            ["blank"] = "   ",
            ["port"] = "not-a-number",
            ["retries"] = "3",
            ["verbose"] = "TRUE",
        };

        Assert.Equal("tcp:7500", SettingsBag.Get(bag, "daemon"));          // trimmed
        Assert.Equal("fallback", SettingsBag.Get(bag, "blank", "fallback"));
        Assert.Equal("fallback", SettingsBag.Get(bag, "absent", "fallback"));
        Assert.Null(SettingsBag.GetOrNull(bag, "blank"));                   // blank and absent are both "not set"
        Assert.Equal(7, SettingsBag.GetInt(bag, "port", 7));                // unparseable → the caller's fallback
        Assert.Equal(3, SettingsBag.GetInt(bag, "retries"));
        Assert.True(SettingsBag.GetBool(bag, "verbose"));                   // the console writes "true"/"false"
        Assert.False(SettingsBag.GetBool(bag, "absent"));
    }

    [Fact]
    public void Require_reports_a_blank_field_once_and_still_returns_the_value()
    {
        var errors = new List<string>();
        var bag = new Dictionary<string, string> { ["daemon"] = "", ["subject"] = "orders.>" };

        Assert.Equal("", SettingsBag.Require(bag, "daemon", "Daemon", errors));
        Assert.Equal("orders.>", SettingsBag.Require(bag, "subject", "Subject", errors));

        Assert.Equal(["Daemon is required"], errors);
    }

    // ------------------------------------------------------------------
    // Masking: which keys are secret comes from the DESCRIPTOR, since a string
    // dictionary carries no [Secret] attributes for SecretWalk to find.
    // ------------------------------------------------------------------

    [Fact]
    public void Mask_hides_only_the_keys_the_descriptor_declares_secret()
    {
        var def = SourceWithSettings(new() { ["daemon"] = "tcp:7500", ["password"] = "hunter2", ["blank"] = "" });

        var masked = SecretsMasker.Mask(def);

        Assert.Equal("tcp:7500", masked.Connector!.Settings["daemon"]);      // an address is not a credential
        Assert.Equal(SourceKinds.SecretMask, masked.Connector.Settings["password"]);
        Assert.Equal("", masked.Connector.Settings["blank"]);                // masking an absent secret would fabricate one
        Assert.Equal("hunter2", def.Connector!.Settings["password"]);        // never mutates its input
    }

    [Fact]
    public void Mask_hides_everything_when_the_kind_is_not_registered_here()
    {
        // Fail closed: with no descriptor nothing can tell a hostname from a password, and exporting a
        // credential in plaintext is the worse half of that guess. An operator sees an unhelpful export;
        // they do not see a leak.
        var def = SourceWithSettings(new() { ["host"] = "db-01", ["password"] = "hunter2" }, kind: "kind-from-a-plugin-nobody-installed");

        var masked = SecretsMasker.Mask(def);

        Assert.Equal(SourceKinds.SecretMask, masked.Connector!.Settings["host"]);
        Assert.Equal(SourceKinds.SecretMask, masked.Connector.Settings["password"]);
    }

    [Fact]
    public void A_written_mask_keeps_the_stored_value_and_a_real_edit_replaces_it()
    {
        var stored = SourceWithSettings(new() { ["daemon"] = "tcp:7500", ["password"] = "hunter2" });
        var incoming = SourceWithSettings(new() { ["daemon"] = "tcp:9000", ["password"] = SourceKinds.SecretMask });

        var merged = SecretsMasker.MergeSecrets(incoming, stored);

        Assert.Equal("tcp:9000", merged.Connector!.Settings["daemon"]);      // the GET→edit→PUT cycle's real edit
        Assert.Equal("hunter2", merged.Connector.Settings["password"]);      // "***" means keep
        Assert.True(SecretsMasker.HasMaskedValues(incoming));
        Assert.False(SecretsMasker.HasMaskedValues(merged));
    }

    [Fact]
    public void A_masked_key_the_stored_config_never_had_is_left_standing()
    {
        // Same "nothing to keep" rule an unmatched URL header key follows — there is no stored value to
        // restore, and inventing one would be worse than a visible "***".
        var stored = SourceWithSettings(new() { ["daemon"] = "tcp:7500" });
        var incoming = SourceWithSettings(new() { ["password"] = SourceKinds.SecretMask });

        var merged = SecretsMasker.MergeSecrets(incoming, stored);

        Assert.Equal(SourceKinds.SecretMask, merged.Connector!.Settings["password"]);
    }

    [Fact]
    public void The_sink_half_masks_and_merges_by_the_same_rules()
    {
        List<SinkSpec> stored =
        [
            new() { Kind = BagSinkKind, Settings = new() { ["endpoint"] = "https://out", ["apiKey"] = "k-123" } },
        ];
        var masked = SecretsMasker.MaskSinks(stored);

        Assert.Equal("https://out", masked[0].Settings["endpoint"]);
        Assert.Equal(SourceKinds.SecretMask, masked[0].Settings["apiKey"]);
        Assert.True(SecretsMasker.HasMaskedSinkValues(masked));

        var merged = SecretsMasker.MergeSinkSecrets(masked, stored);

        Assert.Equal("k-123", merged[0].Settings["apiKey"]);
        Assert.False(SecretsMasker.HasMaskedSinkValues(merged));
    }

    // ------------------------------------------------------------------
    // The plugin loader.
    // ------------------------------------------------------------------

    [Fact]
    public void A_missing_plugin_directory_is_silence_not_an_error()
    {
        // The overwhelmingly common case — no plugins installed — must not produce a startup line, let
        // alone a failure.
        Assert.Empty(StreamsForgePlugins.LoadFrom(Path.Combine(Path.GetTempPath(), $"sf-plugins-{Guid.NewGuid():N}")));
    }

    [Fact]
    public void A_file_that_is_not_a_managed_assembly_is_reported_and_skipped()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"sf-plugins-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(Path.Combine(dir, "not-really.dll"), "this is not a PE image");

            var report = StreamsForgePlugins.LoadFrom(dir);

            // Reported, so "I copied a DLL and nothing happened" is diagnosable — and skipped, so one bad
            // file in the directory cannot keep a host from starting.
            Assert.Single(report);
            Assert.Contains("not-really.dll", report[0]);
            Assert.Contains("skipped", report[0]);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void A_plugin_registering_a_kind_that_already_exists_fails_only_itself()
    {
        // The duplicate-kind guard belongs to the registry; what matters here is that the loader turns it
        // into a reported line rather than an exception out of host startup. Exercised directly on the
        // plugin type (loading a DLL would need a second assembly on disk to prove the same one thing).
        var plugin = new DuplicateKindPlugin();
        var ex = Record.Exception(() => InboundTransports.Register(new BagTransport()));

        Assert.IsType<InvalidOperationException>(ex);
        Assert.Contains(BagKind, ex.Message);
        Assert.Equal("duplicate-kind", plugin.Name);
    }

    // ------------------------------------------------------------------
    // The plugin loader — embedded console UI modules (one DLL instead of a DLL plus a loose .js).
    // ------------------------------------------------------------------

    [Fact]
    public void A_plugin_dll_with_an_embedded_ui_plugins_resource_is_reported_and_served_from_the_assembly()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"sf-plugins-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        try
        {
            File.Copy(FixturePluginDllPath(), Path.Combine(dir, "test-kind-fixture.dll"));

            var report = StreamsForgePlugins.LoadFrom(dir);

            Assert.Contains(report, l => l.StartsWith("plugin 'test-kind-plugin' (", StringComparison.Ordinal) && l.EndsWith("registered", StringComparison.Ordinal));
            Assert.Contains("plugin 'test-kind-plugin' provides ui module 'test-kind.js'", report);
            Assert.Contains("plugin 'test-kind-plugin' provides ui module 'test-kind-ts.tsx'", report);

            var module = StreamsForgePlugins.UiModules.Last(m => m.FileName == "test-kind.js");
            Assert.Equal("ui-plugins/test-kind.js", module.ResourceName);

            using var stream = module.Assembly.GetManifestResourceStream(module.ResourceName);
            Assert.NotNull(stream);
            using var reader = new StreamReader(stream!);
            Assert.Contains("registerTransportEditor('ui-fixture-kind'", reader.ReadToEnd(), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>Locates the fixture plugin built by the sibling
    /// <c>StreamsForge.AppCore.Tests.PluginFixture</c> project (referenced with
    /// <c>ReferenceOutputAssembly="false"</c> so it never loads into THIS test process on its own — see
    /// that project's csproj for why).</summary>
    private static string FixturePluginDllPath()
    {
        // "bin" only — "obj/**/ref" and "obj/**/refint" hold the compiler's metadata-only reference
        // assemblies for incremental builds, same simple name, and loading one of THOSE throws "Reference
        // assemblies cannot be loaded for execution" (no method bodies).
        var fixtureBinDir = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "StreamsForge.AppCore.Tests.PluginFixture", "bin"));
        return Directory.EnumerateFiles(fixtureBinDir, "StreamsForge.AppCore.Tests.PluginFixture.dll", SearchOption.AllDirectories)
                   .OrderByDescending(File.GetLastWriteTimeUtc)
                   .FirstOrDefault()
               ?? throw new FileNotFoundException($"PluginFixture DLL not found under '{fixtureBinDir}' — build the solution first.");
    }

    // ------------------------------------------------------------------
    // Fakes.
    // ------------------------------------------------------------------

    private static SourceDefinition SourceWithSettings(Dictionary<string, string> settings, string kind = BagKind) =>
        new()
        {
            Name = "s",
            Kind = kind,
            Fields = [new FieldDef("id", FieldType.String)],
            Connector = new ConnectorConfig { Settings = settings },
        };

    /// <summary>An inbound kind with no typed config class at all — everything it needs lives in the
    /// settings bag, exactly as an out-of-tree connector's would.</summary>
    private sealed class BagTransport : IInboundTransport
    {
        public string Kind => BagKind;

        public void Validate(SourceDefinition def, List<string> errors) =>
            SettingsBag.Require(def.Connector?.Settings, "daemon", "Daemon", errors);

        public string FormatOf(SourceDefinition def) => FileFormats.Ndjson;

        public IInboundSubscription Open(SourceDefinition def) => throw new NotSupportedException();

        public TransportDescriptor Describe() => new()
        {
            Kind = BagKind,
            Label = "Quux bag",
            ConfigProperty = "settings",
            Fields =
            [
                new TransportField { Key = "daemon", Label = "Daemon", Required = true },
                new TransportField { Key = "password", Label = "Password", Type = TransportFieldTypes.Secret },
                new TransportField { Key = "blank", Label = "Blank", Type = TransportFieldTypes.Secret },
            ],
        };
    }

    private sealed class BagSinkTransport : ISinkTransport
    {
        public string Kind => BagSinkKind;

        public bool IsConfigured(SinkSpec spec) => SettingsBag.Get(spec.Settings, "endpoint").Length > 0;

        public ISinkClient Create(SinkSpec spec, string entityKind, string entityName, Action<string, Exception>? onFailure) =>
            throw new NotSupportedException();

        public TransportDescriptor Describe() => new()
        {
            Kind = BagSinkKind,
            Label = "Quux bag sink",
            ConfigProperty = "settings",
            Fields =
            [
                new TransportField { Key = "endpoint", Label = "Endpoint", Required = true },
                new TransportField { Key = "apiKey", Label = "API key", Type = TransportFieldTypes.Secret },
            ],
        };
    }

    private sealed class DuplicateKindPlugin : IStreamsForgePlugin
    {
        public string Name => "duplicate-kind";

        public void Register() => InboundTransports.Register(new BagTransport());
    }
}

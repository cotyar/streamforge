using System.Reflection;
using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 010: the guard for "every declared secret is actually masked", not for any one credential field.
///
/// <para>Before this change each secret slot was named by hand three times per direction in
/// <see cref="SecretsMasker"/> (mask / merge / has-masked). A slot missing from one of those lists leaked a
/// plaintext credential through a GET or a config export, silently — the exact class of defect the catalog
/// round-trip guard was added for on the update path. These tests populate EVERY
/// <see cref="SecretAttribute"/>-marked property in the contracts by reflection and assert none of the
/// planted values survives a mask, so a transport added tomorrow is covered automatically and a revert to
/// field-by-field masking fails immediately.</para>
/// </summary>
public class SecretWalkTests
{
    /// <summary>Sets every [Secret] string property reachable from <paramref name="root"/> to a value
    /// derived from its own name, and returns those values — so an assertion can name which slot leaked.</summary>
    private static List<string> PlantSecrets(object root)
    {
        var planted = new List<string>();
        foreach (var slot in SecretWalk.Slots(root))
        {
            var value = $"secret-{planted.Count}";
            slot.Set(value);
            planted.Add(value);
        }
        return planted;
    }

    /// <summary>A connector with every per-transport config populated — the shape the walk has to cover.
    /// Deliberately built by naming the properties (not reflection) so that a NEW config container added to
    /// <see cref="ConnectorConfig"/> without being added here shows up as a coverage gap in
    /// <see cref="EveryConnectorConfigContainerIsWalked"/> below rather than passing vacuously.</summary>
    private static ConnectorConfig FullConnector() => new()
    {
        Url = new UrlPollConfig { Url = "https://example/api" },
        File = new FilePollConfig { Path = "/tmp/x.ndjson" },
        Folder = new FolderPollConfig { Path = "/tmp" },
        Grpc = new GrpcSubConfig { Address = "http://localhost:5299", EntityKey = "source:s" },
        Nats = new NatsSubConfig { Url = "nats://localhost:4222", Subject = "t.>" },
        Mapping = new MappingSpec(),
        Schedule = new ScheduleSpec { IntervalMs = 30_000 },
    };

    [Fact]
    public void Mask_LeavesNoPlantedSecretValueBehind()
    {
        var def = new SourceDefinition { Name = "s", Kind = SourceKinds.Nats, Connector = FullConnector() };
        var planted = PlantSecrets(def.Connector!);
        Assert.NotEmpty(planted);

        var masked = SecretsMasker.Mask(def);

        var survivors = SecretWalk.Slots(masked.Connector).Select(s => s.Value).Where(v => planted.Contains(v!)).ToList();
        Assert.Empty(survivors);
        Assert.All(SecretWalk.Slots(masked.Connector), s => Assert.Equal(SourceKinds.SecretMask, s.Value));
    }

    [Fact]
    public void Mask_DoesNotTouchPropertiesWithoutTheAttribute()
    {
        // Masking is opt-in: Username is an identifier, not a credential, on both Grpc and Nats — the
        // pre-plan-010 code made that distinction by hand and the walk must preserve it exactly.
        var def = new SourceDefinition
        {
            Name = "s",
            Kind = SourceKinds.Nats,
            Connector = new ConnectorConfig
            {
                Nats = new NatsSubConfig { Url = "nats://h:4222", Subject = "t", Username = "reader", Token = "tok" },
                Grpc = new GrpcSubConfig { Address = "a", EntityKey = "source:s", Username = "svc", Password = "pw" },
            },
        };

        var masked = SecretsMasker.Mask(def);

        Assert.Equal("reader", masked.Connector!.Nats!.Username);
        Assert.Equal("svc", masked.Connector.Grpc!.Username);
        Assert.Equal("nats://h:4222", masked.Connector.Nats.Url);
        Assert.Equal(SourceKinds.SecretMask, masked.Connector.Nats.Token);
        Assert.Equal(SourceKinds.SecretMask, masked.Connector.Grpc.Password);
    }

    [Fact]
    public void Mask_LeavesAnEmptySecretAlone()
    {
        // Masking an absent secret would fabricate one — the caller could not then tell "no token set"
        // from "a token is set and hidden".
        var def = new SourceDefinition
        {
            Name = "s",
            Kind = SourceKinds.Nats,
            Connector = new ConnectorConfig { Nats = new NatsSubConfig { Url = "nats://h:4222", Subject = "t" } },
        };

        var masked = SecretsMasker.Mask(def);

        Assert.Null(masked.Connector!.Nats!.Token);
        Assert.Null(masked.Connector.Nats.Password);
        Assert.Null(masked.Connector.Nats.Credentials);
    }

    [Fact]
    public void MergeSecrets_RestoresEveryMaskedSlotFromStored()
    {
        var stored = new SourceDefinition { Name = "s", Kind = SourceKinds.Nats, Connector = FullConnector() };
        var plantedValues = PlantSecrets(stored.Connector!);

        // What a client sends back after a GET: the whole object, with every secret still the mask.
        var incoming = SecretsMasker.Mask(stored);

        var merged = SecretsMasker.MergeSecrets(incoming, stored);

        Assert.Equal(plantedValues, [.. SecretWalk.Slots(merged.Connector).Select(s => s.Value)]);
    }

    [Fact]
    public void MergeSecrets_LeavesTheMaskStandingWhenThereIsNothingStoredToKeep()
    {
        // No stored counterpart object (e.g. the source did not have a nats config before) means there is
        // no value to "keep" — writing null would silently erase what the client thought it preserved.
        var incoming = new SourceDefinition
        {
            Name = "s",
            Kind = SourceKinds.Nats,
            Connector = new ConnectorConfig { Nats = new NatsSubConfig { Url = "u", Subject = "t", Token = SourceKinds.SecretMask } },
        };
        var stored = new SourceDefinition { Name = "s", Kind = SourceKinds.Grpc, Connector = new ConnectorConfig() };

        var merged = SecretsMasker.MergeSecrets(incoming, stored);

        Assert.Equal(SourceKinds.SecretMask, merged.Connector!.Nats!.Token);
    }

    [Fact]
    public void HasMaskedValues_SeesEverySlot()
    {
        var def = new SourceDefinition { Name = "s", Kind = SourceKinds.Nats, Connector = FullConnector() };
        Assert.False(SecretsMasker.HasMaskedValues(def));

        foreach (var slot in SecretWalk.Slots(def.Connector))
        {
            // One slot at a time: a per-slot assertion catches a walk that reaches only the first config
            // container, which a whole-graph assertion would not.
            slot.Set(SourceKinds.SecretMask);
            Assert.True(SecretsMasker.HasMaskedValues(def));
            slot.Set(null);
        }
    }

    [Fact]
    public void SinkSecrets_MaskAndMergeAndDetectThroughTheSameWalk()
    {
        var stored = new List<SinkSpec>
        {
            new() { Kind = SinkKinds.Nats, Enabled = true, Nats = new NatsPubConfig { Url = "nats://h:4222", Subject = "sf.out", Token = "t0", Password = "p0", Credentials = "c0" } },
        };

        var masked = SecretsMasker.MaskSinks(stored);
        Assert.All(SecretWalk.Slots(masked[0]), s => Assert.Equal(SourceKinds.SecretMask, s.Value));
        Assert.True(SecretsMasker.HasMaskedSinkValues(masked));
        Assert.False(SecretsMasker.HasMaskedSinkValues(stored));

        var merged = SecretsMasker.MergeSinkSecrets(masked, stored);
        Assert.Equal("t0", merged[0].Nats!.Token);
        Assert.Equal("p0", merged[0].Nats!.Password);
        Assert.Equal("c0", merged[0].Nats!.Credentials);
        Assert.Equal("sf.out", merged[0].Nats!.Subject); // not a secret: untouched throughout
    }

    [Fact]
    public void TheKnownSecretSlotsStillCarryTheAttribute()
    {
        // The other tests in this file assert "everything the walk finds is masked", which stays true — and
        // vacuous — if a property silently loses its [Secret]. This one names the slots that existed before
        // plan 010 turned them into declarations, so removing an attribute is a failing test rather than a
        // quietly-unmasked credential. A NEW transport's fields do not belong here; they are covered by the
        // walk itself.
        var expected = new HashSet<(Type, string)>
        {
            (typeof(GrpcSubConfig), nameof(GrpcSubConfig.Password)),
            (typeof(GrpcSubConfig), nameof(GrpcSubConfig.Token)),
            (typeof(NatsSubConfig), nameof(NatsSubConfig.Token)),
            (typeof(NatsSubConfig), nameof(NatsSubConfig.Password)),
            (typeof(NatsSubConfig), nameof(NatsSubConfig.Credentials)),
            (typeof(NatsPubConfig), nameof(NatsPubConfig.Token)),
            (typeof(NatsPubConfig), nameof(NatsPubConfig.Password)),
            (typeof(NatsPubConfig), nameof(NatsPubConfig.Credentials)),
        };

        foreach (var (type, name) in expected)
        {
            var prop = type.GetProperty(name);
            Assert.NotNull(prop);
            Assert.True(
                prop!.IsDefined(typeof(SecretAttribute), inherit: true),
                $"{type.Name}.{name} lost its [Secret] — it would be exported in plaintext.");
        }
    }

    [Fact]
    public void EveryConnectorConfigContainerIsWalked()
    {
        // Coverage check for FullConnector above: if a future transport adds a config property to
        // ConnectorConfig and nobody populates it here, this fails rather than letting the mask tests pass
        // over a container they never visit.
        var full = FullConnector();
        var unpopulated = typeof(ConnectorConfig)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType.IsClass && p.PropertyType != typeof(string) && p.GetValue(full) is null)
            .Select(p => p.Name)
            .ToList();

        Assert.True(
            unpopulated.Count == 0,
            $"ConnectorConfig.{string.Join(", ", unpopulated)} is not populated in FullConnector() — add it there so its " +
            "[Secret] fields are actually covered by the masking tests in this file.");
    }
}

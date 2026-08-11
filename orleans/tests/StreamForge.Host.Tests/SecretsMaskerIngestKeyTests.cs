using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>Plan 009 A1.2: <see cref="SecretsMasker"/>'s extension to Ingest.Keys[].Hash/.Salt — the
/// same mask-on-read/merge-on-write round-trip invariant <c>SecretsMaskerTests</c> already covers for
/// connector secrets (D-H), now covering push-key material too. New file rather than editing
/// <c>SecretsMaskerTests.cs</c>, which is pinned to the pre-009 connector-only surface.</summary>
public class SecretsMaskerIngestKeyTests
{
    private static SourceDefinition IngestSourceWithKey(string hash = "real-hash", string salt = "real-salt") => new()
    {
        Name = "s",
        Kind = SourceKinds.Ingest,
        Fields = [new FieldDef("price", FieldType.Double)],
        Ingest = new IngestConfig
        {
            Keys = [new IngestKey { Id = "k1", Hash = hash, Salt = salt, Label = "prod", CreatedAtMs = 100, LastUsedMs = 200 }],
        },
    };

    [Fact]
    public void Mask_replaces_nonempty_Hash_and_Salt_with_the_mask()
    {
        var masked = SecretsMasker.Mask(IngestSourceWithKey());

        var key = Assert.Single(masked.Ingest!.Keys);
        Assert.Equal(SourceKinds.SecretMask, key.Hash);
        Assert.Equal(SourceKinds.SecretMask, key.Salt);
        // Non-secret fields are untouched.
        Assert.Equal("k1", key.Id);
        Assert.Equal("prod", key.Label);
        Assert.Equal(200, key.LastUsedMs);
    }

    [Fact]
    public void Mask_never_mutates_the_original()
    {
        var original = IngestSourceWithKey();
        SecretsMasker.Mask(original);

        Assert.Equal("real-hash", original.Ingest!.Keys[0].Hash);
        Assert.Equal("real-salt", original.Ingest.Keys[0].Salt);
    }

    [Fact]
    public void Mask_leaves_an_empty_Hash_Salt_alone()
    {
        var def = IngestSourceWithKey(hash: "", salt: "");
        var masked = SecretsMasker.Mask(def);

        Assert.Equal("", masked.Ingest!.Keys[0].Hash);
        Assert.Equal("", masked.Ingest.Keys[0].Salt);
    }

    [Fact]
    public void Mask_is_a_noop_when_the_source_has_no_Ingest_config()
    {
        var def = new SourceDefinition { Name = "s", Kind = SourceKinds.Generator, Fields = [new FieldDef("x", FieldType.String)] };
        var masked = SecretsMasker.Mask(def);
        Assert.Null(masked.Ingest);
    }

    [Fact]
    public void MergeSecrets_restores_the_stored_Hash_Salt_when_incoming_is_masked()
    {
        var stored = IngestSourceWithKey();
        var incoming = IngestSourceWithKey(hash: SourceKinds.SecretMask, salt: SourceKinds.SecretMask);

        var merged = SecretsMasker.MergeSecrets(incoming, stored);

        Assert.Equal("real-hash", merged.Ingest!.Keys[0].Hash);
        Assert.Equal("real-salt", merged.Ingest.Keys[0].Salt);
    }

    [Fact]
    public void MergeSecrets_matches_keys_by_Id_not_position()
    {
        var stored = new SourceDefinition
        {
            Name = "s",
            Kind = SourceKinds.Ingest,
            Fields = [new FieldDef("price", FieldType.Double)],
            Ingest = new IngestConfig
            {
                Keys =
                [
                    new IngestKey { Id = "k-old", Hash = "old-hash", Salt = "old-salt" },
                    new IngestKey { Id = "k-new", Hash = "new-hash", Salt = "new-salt" },
                ],
            },
        };
        var incoming = new SourceDefinition
        {
            Name = "s",
            Kind = SourceKinds.Ingest,
            Fields = [new FieldDef("price", FieldType.Double)],
            Ingest = new IngestConfig
            {
                // Reordered relative to `stored` — Id is what must drive the match, not index.
                Keys =
                [
                    new IngestKey { Id = "k-new", Hash = SourceKinds.SecretMask, Salt = SourceKinds.SecretMask },
                    new IngestKey { Id = "k-old", Hash = SourceKinds.SecretMask, Salt = SourceKinds.SecretMask },
                ],
            },
        };

        var merged = SecretsMasker.MergeSecrets(incoming, stored);

        Assert.Equal("new-hash", merged.Ingest!.Keys.Single(k => k.Id == "k-new").Hash);
        Assert.Equal("old-hash", merged.Ingest.Keys.Single(k => k.Id == "k-old").Hash);
    }

    [Fact]
    public void MergeSecrets_leaves_a_masked_key_with_no_stored_counterpart_as_is()
    {
        // A key present in incoming but absent from stored (e.g. an edit racing a generate/revoke) —
        // nothing to restore FROM, so the mask stays exactly as SecretsMasker's connector-secret
        // behavior already does for an unmatched header key.
        var stored = IngestSourceWithKey(); // has "k1" only
        var incoming = new SourceDefinition
        {
            Name = "s",
            Kind = SourceKinds.Ingest,
            Fields = [new FieldDef("price", FieldType.Double)],
            Ingest = new IngestConfig { Keys = [new IngestKey { Id = "k-unmatched", Hash = SourceKinds.SecretMask, Salt = SourceKinds.SecretMask }] },
        };

        var merged = SecretsMasker.MergeSecrets(incoming, stored);

        Assert.Equal(SourceKinds.SecretMask, merged.Ingest!.Keys[0].Hash);
    }

    [Fact]
    public void MergeSecrets_is_a_noop_when_stored_has_no_Ingest_config()
    {
        var incoming = IngestSourceWithKey(hash: SourceKinds.SecretMask, salt: SourceKinds.SecretMask);
        var merged = SecretsMasker.MergeSecrets(incoming, new SourceDefinition { Name = "s", Kind = SourceKinds.Ingest, Fields = [] });

        Assert.Equal(SourceKinds.SecretMask, merged.Ingest!.Keys[0].Hash); // nothing to restore from
    }

    [Fact]
    public void HasMaskedValues_detects_a_masked_ingest_key()
    {
        Assert.True(SecretsMasker.HasMaskedValues(IngestSourceWithKey(hash: SourceKinds.SecretMask)));
        Assert.True(SecretsMasker.HasMaskedValues(IngestSourceWithKey(salt: SourceKinds.SecretMask)));
        Assert.False(SecretsMasker.HasMaskedValues(IngestSourceWithKey()));
    }
}

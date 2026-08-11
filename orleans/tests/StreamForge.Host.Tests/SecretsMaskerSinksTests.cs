using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 009 B2: unit tests for <see cref="SecretsMasker"/>'s Sinks-shaped additions
/// (<see cref="SecretsMasker.MaskSinks"/>/<see cref="SecretsMasker.MaskPipeline"/>/
/// <see cref="SecretsMasker.MaskTable"/>/<see cref="SecretsMasker.MergeSinkSecrets"/>/
/// <see cref="SecretsMasker.HasMaskedSinkValues"/>) — the D-H secrets-lite convention extended from
/// source connector credentials to SinkSpec.Nats credentials on pipelines/tables. Mirrors
/// SourcesEndpointsLogicTests' D-H round-trip tests for sources.
/// </summary>
public class SecretsMaskerSinksTests
{
    private static SinkSpec Nats(string? token = "tok-secret", string? password = null, string? credentials = null, string? username = "identifier-not-a-secret") => new()
    {
        Kind = SinkKinds.Nats,
        Enabled = true,
        Nats = new NatsPubConfig
        {
            Url = "nats://localhost:4222",
            Subject = "sf.out",
            Token = token,
            Password = password,
            Credentials = credentials,
            Username = username,
        },
    };

    [Fact]
    public void MaskSinks_MasksNonEmptyTokenPasswordCredentials_ButNotUsername()
    {
        var sinks = new List<SinkSpec> { Nats(token: "tok", password: "pw", credentials: "creds-file-contents") };

        var masked = SecretsMasker.MaskSinks(sinks);

        Assert.Equal(SourceKinds.SecretMask, masked[0].Nats!.Token);
        Assert.Equal(SourceKinds.SecretMask, masked[0].Nats!.Password);
        Assert.Equal(SourceKinds.SecretMask, masked[0].Nats!.Credentials);
        Assert.Equal("identifier-not-a-secret", masked[0].Nats!.Username);
    }

    [Fact]
    public void MaskSinks_LeavesEmptySecretsAsIs_NeverFabricatesOne()
    {
        var sinks = new List<SinkSpec> { Nats(token: null, password: null, credentials: null) };

        var masked = SecretsMasker.MaskSinks(sinks);

        Assert.Null(masked[0].Nats!.Token);
        Assert.Null(masked[0].Nats!.Password);
        Assert.Null(masked[0].Nats!.Credentials);
    }

    [Fact]
    public void MaskSinks_NeverMutatesTheInput()
    {
        var sinks = new List<SinkSpec> { Nats(token: "tok") };

        SecretsMasker.MaskSinks(sinks);

        Assert.Equal("tok", sinks[0].Nats!.Token);
    }

    [Fact]
    public void MaskPipeline_MasksSinksAndLeavesEverythingElseIntact()
    {
        var def = new PipelineDefinition { Id = "p1", Name = "pipe", Sql = "SELECT 1", Sinks = [Nats(token: "tok")] };

        var masked = SecretsMasker.MaskPipeline(def);

        Assert.Equal("pipe", masked.Name);
        Assert.Equal("SELECT 1", masked.Sql);
        Assert.Equal(SourceKinds.SecretMask, masked.Sinks[0].Nats!.Token);
        // Original untouched.
        Assert.Equal("tok", def.Sinks[0].Nats!.Token);
    }

    [Fact]
    public void MaskTable_MasksSinksAndLeavesEverythingElseIntact()
    {
        var def = new TableDefinition { Id = "t1", Name = "tbl", Sql = "SELECT 1", Sinks = [Nats(token: "tok")] };

        var masked = SecretsMasker.MaskTable(def);

        Assert.Equal("tbl", masked.Name);
        Assert.Equal(SourceKinds.SecretMask, masked.Sinks[0].Nats!.Token);
        Assert.Equal("tok", def.Sinks[0].Nats!.Token);
    }

    [Fact]
    public void GetThenPut_round_trip_does_not_clobber_the_stored_sink_credential()
    {
        var stored = new List<SinkSpec> { Nats(token: "tok-real-secret") };

        var fetched = SecretsMasker.MaskSinks(stored);
        Assert.Equal(SourceKinds.SecretMask, fetched[0].Nats!.Token);

        var effective = SecretsMasker.MergeSinkSecrets(fetched, stored);

        Assert.Equal("tok-real-secret", effective[0].Nats!.Token);
    }

    [Fact]
    public void MergeSinkSecrets_LetsAGenuinelyNewCredentialThrough()
    {
        var stored = new List<SinkSpec> { Nats(token: "old-tok") };
        var incoming = new List<SinkSpec> { Nats(token: "new-tok") };

        var effective = SecretsMasker.MergeSinkSecrets(incoming, stored);

        Assert.Equal("new-tok", effective[0].Nats!.Token);
    }

    [Fact]
    public void MergeSinkSecrets_OnCreate_LeavesAMaskTypedByMistakeAsIs()
    {
        // No stored list to restore from (create path) — same "nothing to keep" rule sources' own
        // MergeSecrets(def, null) documents.
        var incoming = new List<SinkSpec> { Nats(token: SourceKinds.SecretMask) };

        var effective = SecretsMasker.MergeSinkSecrets(incoming, null);

        Assert.Equal(SourceKinds.SecretMask, effective[0].Nats!.Token);
    }

    [Fact]
    public void MergeSinkSecrets_UnmatchedIndex_LeavesTheMaskAsIs()
    {
        // incoming has 2 sinks, stored only has 1 — the second incoming sink has nothing stored to
        // restore from, mirrors an unmatched header key in MergeSecrets.
        var stored = new List<SinkSpec> { Nats(token: "only-one") };
        var incoming = new List<SinkSpec> { Nats(token: "only-one"), Nats(token: SourceKinds.SecretMask) };

        var effective = SecretsMasker.MergeSinkSecrets(incoming, stored);

        Assert.Equal(SourceKinds.SecretMask, effective[1].Nats!.Token);
    }

    [Fact]
    public void HasMaskedSinkValues_DetectsAnyMaskedCredentialSlot()
    {
        Assert.True(SecretsMasker.HasMaskedSinkValues([Nats(token: SourceKinds.SecretMask)]));
        Assert.True(SecretsMasker.HasMaskedSinkValues([Nats(token: "real", password: SourceKinds.SecretMask)]));
        Assert.False(SecretsMasker.HasMaskedSinkValues([Nats(token: "real")]));
        Assert.False(SecretsMasker.HasMaskedSinkValues(null));
        Assert.False(SecretsMasker.HasMaskedSinkValues([]));
    }
}

using StreamsForge.Abstractions;
using StreamsForge.AppCore.Config;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Plan 009 B1: <see cref="SecretsMasker"/>'s coverage for <c>NatsSubConfig</c> — Token/
/// Password/Credentials are secrets (masked/merged), Url/Subject/QueueGroup/Format/Username are not.
/// Sibling to orleans/tests/StreamsForge.Host.Tests/SecretsMaskerTests.cs's grpc/url coverage (not mine
/// to edit — new-files-only convention).</summary>
public class SecretsMaskerNatsTests
{
    private static SourceDefinition NatsSource(
        string? token = "tok-secret", string? password = "pw-secret", string? credentials = "-----BEGIN NATS CREDS-----", string? username = "svc-account") => new()
    {
        Name = "n",
        Kind = SourceKinds.Nats,
        Connector = new ConnectorConfig
        {
            Nats = new NatsSubConfig
            {
                Url = "nats://localhost:4222", Subject = "trades.>", QueueGroup = "workers",
                Token = token, Password = password, Credentials = credentials, Username = username,
            },
        },
    };

    // ------------------------------------------------------------------
    // Mask.
    // ------------------------------------------------------------------

    [Fact]
    public void Mask_replaces_token_password_and_credentials()
    {
        var masked = SecretsMasker.Mask(NatsSource());

        Assert.Equal(SourceKinds.SecretMask, masked.Connector!.Nats!.Token);
        Assert.Equal(SourceKinds.SecretMask, masked.Connector.Nats.Password);
        Assert.Equal(SourceKinds.SecretMask, masked.Connector.Nats.Credentials);
    }

    [Fact]
    public void Mask_leaves_url_subject_queueGroup_and_username_untouched()
    {
        var masked = SecretsMasker.Mask(NatsSource());

        Assert.Equal("nats://localhost:4222", masked.Connector!.Nats!.Url);
        Assert.Equal("trades.>", masked.Connector.Nats.Subject);
        Assert.Equal("workers", masked.Connector.Nats.QueueGroup);
        Assert.Equal("svc-account", masked.Connector.Nats.Username); // not a secret, same convention as GrpcSubConfig.Username
    }

    [Fact]
    public void Mask_leaves_null_secrets_null_rather_than_fabricating_one()
    {
        var masked = SecretsMasker.Mask(NatsSource(token: null, password: null, credentials: null));

        Assert.Null(masked.Connector!.Nats!.Token);
        Assert.Null(masked.Connector.Nats.Password);
        Assert.Null(masked.Connector.Nats.Credentials);
    }

    [Fact]
    public void Mask_does_not_mutate_the_original()
    {
        var source = NatsSource();
        SecretsMasker.Mask(source);

        Assert.Equal("tok-secret", source.Connector!.Nats!.Token);
        Assert.Equal("pw-secret", source.Connector.Nats.Password);
    }

    // ------------------------------------------------------------------
    // HasMaskedValues.
    // ------------------------------------------------------------------

    [Fact]
    public void HasMaskedValues_is_false_before_masking_and_true_after()
    {
        var source = NatsSource();
        Assert.False(SecretsMasker.HasMaskedValues(source));
        Assert.True(SecretsMasker.HasMaskedValues(SecretsMasker.Mask(source)));
    }

    // ------------------------------------------------------------------
    // MergeSecrets — the GET(masked) -> edit -> PUT round trip must not clobber stored secrets.
    // ------------------------------------------------------------------

    [Fact]
    public void MergeSecrets_restores_stored_token_password_and_credentials_for_masked_incoming_values()
    {
        var stored = NatsSource();
        var incoming = SecretsMasker.Mask(NatsSource());

        var merged = SecretsMasker.MergeSecrets(incoming, stored);

        Assert.Equal("tok-secret", merged.Connector!.Nats!.Token);
        Assert.Equal("pw-secret", merged.Connector.Nats.Password);
        Assert.Equal("-----BEGIN NATS CREDS-----", merged.Connector.Nats.Credentials);
    }

    [Fact]
    public void MergeSecrets_leaves_a_genuinely_new_non_masked_token_untouched()
    {
        var stored = NatsSource();
        var incoming = NatsSource(token: "brand-new-token");

        var merged = SecretsMasker.MergeSecrets(incoming, stored);

        Assert.Equal("brand-new-token", merged.Connector!.Nats!.Token);
    }

    [Fact]
    public void MergeSecrets_round_trips_mask_then_merge_back_to_the_original_values()
    {
        var original = NatsSource();
        var masked = SecretsMasker.Mask(original);
        var restored = SecretsMasker.MergeSecrets(masked, original);

        Assert.Equal(original.Connector!.Nats!.Token, restored.Connector!.Nats!.Token);
        Assert.Equal(original.Connector.Nats.Password, restored.Connector.Nats.Password);
        Assert.Equal(original.Connector.Nats.Credentials, restored.Connector.Nats.Credentials);
    }

    [Fact]
    public void MergeSecrets_with_no_stored_source_leaves_the_mask_as_is()
    {
        var incoming = SecretsMasker.Mask(NatsSource());
        var merged = SecretsMasker.MergeSecrets(incoming, null);

        Assert.Equal(SourceKinds.SecretMask, merged.Connector!.Nats!.Token);
        Assert.Equal(SourceKinds.SecretMask, merged.Connector.Nats.Password);
        Assert.Equal(SourceKinds.SecretMask, merged.Connector.Nats.Credentials);
    }
}

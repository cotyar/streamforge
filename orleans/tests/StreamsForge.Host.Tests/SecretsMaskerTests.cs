using StreamsForge.Abstractions;
using StreamsForge.AppCore.Config;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Plan 006 (D-H): <see cref="SecretsMasker"/> — mask/merge/round-trip and the
/// empty-value/no-stored-counterpart edge cases.</summary>
public class SecretsMaskerTests
{
    private static SourceDefinition UrlAndGrpcSource(string headerValue = "Bearer real-secret", string password = "p@ss", string token = "tok") => new()
    {
        Name = "web",
        Kind = SourceKinds.Url,
        Connector = new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://x", Headers = { ["Authorization"] = headerValue, ["X-Empty"] = "" } },
            Grpc = new GrpcSubConfig { Address = "http://y", Password = password, Token = token },
        },
    };

    // ------------------------------------------------------------------
    // Mask.
    // ------------------------------------------------------------------

    [Fact]
    public void Mask_replaces_non_empty_header_values_and_grpc_password_token()
    {
        var masked = SecretsMasker.Mask(UrlAndGrpcSource());

        Assert.Equal(SourceKinds.SecretMask, masked.Connector!.Url!.Headers["Authorization"]);
        Assert.Equal(SourceKinds.SecretMask, masked.Connector.Grpc!.Password);
        Assert.Equal(SourceKinds.SecretMask, masked.Connector.Grpc.Token);
    }

    [Fact]
    public void Mask_leaves_empty_values_empty_rather_than_fabricating_a_secret()
    {
        var masked = SecretsMasker.Mask(UrlAndGrpcSource());
        Assert.Equal("", masked.Connector!.Url!.Headers["X-Empty"]);
    }

    [Fact]
    public void Mask_leaves_null_password_and_token_null()
    {
        var source = UrlAndGrpcSource();
        source.Connector!.Grpc!.Password = null;
        source.Connector.Grpc.Token = null;

        var masked = SecretsMasker.Mask(source);

        Assert.Null(masked.Connector!.Grpc!.Password);
        Assert.Null(masked.Connector.Grpc.Token);
    }

    [Fact]
    public void Mask_of_a_generator_source_with_no_connector_is_a_harmless_clone()
    {
        var source = new SourceDefinition { Name = "gen" };
        var masked = SecretsMasker.Mask(source);

        Assert.Equal("gen", masked.Name);
        Assert.Null(masked.Connector);
        Assert.NotSame(source, masked);
    }

    [Fact]
    public void Mask_does_not_mutate_the_original()
    {
        var source = UrlAndGrpcSource();
        SecretsMasker.Mask(source);

        Assert.Equal("Bearer real-secret", source.Connector!.Url!.Headers["Authorization"]);
        Assert.Equal("p@ss", source.Connector.Grpc!.Password);
        Assert.Equal("tok", source.Connector.Grpc.Token);
    }

    // ------------------------------------------------------------------
    // HasMaskedValues.
    // ------------------------------------------------------------------

    [Fact]
    public void HasMaskedValues_is_false_for_a_source_with_real_secrets()
    {
        Assert.False(SecretsMasker.HasMaskedValues(UrlAndGrpcSource()));
    }

    [Fact]
    public void HasMaskedValues_is_true_after_masking()
    {
        Assert.True(SecretsMasker.HasMaskedValues(SecretsMasker.Mask(UrlAndGrpcSource())));
    }

    [Fact]
    public void HasMaskedValues_is_false_for_a_generator_source()
    {
        Assert.False(SecretsMasker.HasMaskedValues(new SourceDefinition { Name = "gen" }));
    }

    // ------------------------------------------------------------------
    // MergeSecrets.
    // ------------------------------------------------------------------

    [Fact]
    public void MergeSecrets_substitutes_stored_values_for_masked_ones()
    {
        var stored = UrlAndGrpcSource();
        var incoming = SecretsMasker.Mask(UrlAndGrpcSource());

        var merged = SecretsMasker.MergeSecrets(incoming, stored);

        Assert.Equal("Bearer real-secret", merged.Connector!.Url!.Headers["Authorization"]);
        Assert.Equal("p@ss", merged.Connector.Grpc!.Password);
        Assert.Equal("tok", merged.Connector.Grpc.Token);
    }

    [Fact]
    public void MergeSecrets_leaves_a_genuinely_new_non_masked_value_untouched()
    {
        var stored = UrlAndGrpcSource();
        var incoming = UrlAndGrpcSource(headerValue: "Bearer brand-new-value");

        var merged = SecretsMasker.MergeSecrets(incoming, stored);

        Assert.Equal("Bearer brand-new-value", merged.Connector!.Url!.Headers["Authorization"]);
    }

    [Fact]
    public void MergeSecrets_round_trips_mask_then_merge_back_to_the_original_values()
    {
        var original = UrlAndGrpcSource();
        var masked = SecretsMasker.Mask(original);
        var restored = SecretsMasker.MergeSecrets(masked, original);

        Assert.Equal(original.Connector!.Url!.Headers["Authorization"], restored.Connector!.Url!.Headers["Authorization"]);
        Assert.Equal(original.Connector.Grpc!.Password, restored.Connector.Grpc!.Password);
        Assert.Equal(original.Connector.Grpc.Token, restored.Connector.Grpc.Token);
    }

    [Fact]
    public void MergeSecrets_with_no_stored_source_leaves_the_mask_as_is()
    {
        var incoming = SecretsMasker.Mask(UrlAndGrpcSource());
        var merged = SecretsMasker.MergeSecrets(incoming, null);

        Assert.Equal(SourceKinds.SecretMask, merged.Connector!.Url!.Headers["Authorization"]);
        Assert.Equal(SourceKinds.SecretMask, merged.Connector.Grpc!.Password);
    }

    [Fact]
    public void MergeSecrets_with_a_masked_header_key_absent_from_stored_leaves_the_mask_as_is()
    {
        var stored = new SourceDefinition
        {
            Name = "web",
            Kind = SourceKinds.Url,
            Connector = new ConnectorConfig { Url = new UrlPollConfig { Url = "http://x" } }, // no headers at all
        };
        var incoming = new SourceDefinition
        {
            Name = "web",
            Kind = SourceKinds.Url,
            Connector = new ConnectorConfig { Url = new UrlPollConfig { Url = "http://x", Headers = { ["New-Header"] = SourceKinds.SecretMask } } },
        };

        var merged = SecretsMasker.MergeSecrets(incoming, stored);

        // Nothing stored to substitute -- the mask is left literal (documented edge case).
        Assert.Equal(SourceKinds.SecretMask, merged.Connector!.Url!.Headers["New-Header"]);
    }

    [Fact]
    public void MergeSecrets_does_not_mutate_either_input()
    {
        var stored = UrlAndGrpcSource();
        var incoming = SecretsMasker.Mask(UrlAndGrpcSource());
        SecretsMasker.MergeSecrets(incoming, stored);

        Assert.Equal(SourceKinds.SecretMask, incoming.Connector!.Url!.Headers["Authorization"]);
        Assert.Equal("Bearer real-secret", stored.Connector!.Url!.Headers["Authorization"]);
    }
}

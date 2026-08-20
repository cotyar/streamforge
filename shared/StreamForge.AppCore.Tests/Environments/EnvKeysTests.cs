using StreamForge.AppCore.Environments;
using Xunit;

namespace StreamForge.AppCore.Tests.Environments;

/// <summary>Plan 021 wave 0. The first test here is the plan's own D2 acceptance criterion, written as
/// code so a later wave cannot quietly trade it away: with no environment named, every key is the byte
/// string it was before this plan existed.</summary>
public class EnvKeysTests
{
    [Theory]
    [InlineData("catalog")]
    [InlineData("orders")]
    [InlineData("orders|dGVzdA")]           // a shard grain key
    [InlineData("0a1b2c3d4e5f60718293a4b5c6d7e8f9")] // a pipeline GUID("n")
    public void Default_environment_leaves_every_key_byte_identical(string key)
    {
        Assert.Equal(key, EnvKeys.Qualify(EnvKeys.Default, key));
        Assert.Equal(key, EnvKeys.Qualify(null, key));
        Assert.Equal((EnvKeys.Default, key), EnvKeys.Split(key));
    }

    [Fact]
    public void Qualify_and_split_round_trip()
    {
        var qualified = EnvKeys.Qualify("staging", "orders");
        Assert.Equal("staging.orders", qualified);
        Assert.Equal(("staging", "orders"), EnvKeys.Split(qualified));
    }

    [Fact]
    public void A_shard_key_survives_qualification_intact()
    {
        // TableShardKeys.ParseGrainKey splits on the LAST '|', so the qualified table name in front of
        // it must come back whole — this is the one composite key the separator could have collided with.
        var qualified = EnvKeys.Qualify("staging", "orders|dGVzdA");
        var (env, key) = EnvKeys.Split(qualified);
        Assert.Equal("staging", env);
        Assert.Equal("orders|dGVzdA", key);
    }

    [Theory]
    [InlineData("staging")]
    [InlineData("prod-eu")]
    [InlineData("a")]
    [InlineData("0")]
    public void Valid_names(string name) => Assert.True(EnvKeys.IsValidName(name));

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    [InlineData("default")]     // reserved: it is the empty string internally
    [InlineData("catalog")]     // reserved: already a singleton key in the same key space
    [InlineData("users")]
    [InlineData("Staging")]     // upper case would sanitize differently in a file name
    [InlineData("-lead")]       // must start alphanumeric
    [InlineData("has.dot")]
    [InlineData("has_underscore")]
    [InlineData("way-too-long-an-environment-name-past-32")]
    public void Invalid_names(string? name) => Assert.False(EnvKeys.IsValidName(name));

    [Fact]
    public void A_key_whose_prefix_is_not_a_legal_environment_belongs_to_default()
    {
        // The reason Split is safe to call on any key ever written, including every key that predates
        // this plan: a dot that is not an environment prefix is just part of the key.
        Assert.Equal((EnvKeys.Default, "Some.Legacy.Name"), EnvKeys.Split("Some.Legacy.Name"));
        Assert.Equal((EnvKeys.Default, ".leading"), EnvKeys.Split(".leading"));
    }

    [Fact]
    public void Display_and_normalize_are_inverses_at_the_api_boundary()
    {
        Assert.Equal("default", EnvKeys.Display(EnvKeys.Default));
        Assert.Equal("staging", EnvKeys.Display("staging"));
        Assert.Equal(EnvKeys.Default, EnvKeys.Normalize("default"));
        Assert.Equal(EnvKeys.Default, EnvKeys.Normalize(null));
        Assert.Equal(EnvKeys.Default, EnvKeys.Normalize("  "));
        Assert.Equal("staging", EnvKeys.Normalize(" staging "));
    }

    [Fact]
    public void An_entity_name_carrying_the_separator_is_refused_at_the_write_path()
    {
        Assert.True(EnvKeys.IsQualifiableEntityName("orders"));
        Assert.False(EnvKeys.IsQualifiableEntityName("staging.orders"));
        Assert.False(EnvKeys.IsQualifiableEntityName(""));
    }
}

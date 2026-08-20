using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;
using StreamForge.AppCore.Sinks;
using Xunit;

namespace StreamForge.AppCore.Tests.Environments;

/// <summary>Plan 021 wave 2. The bug being pinned here wrote ACROSS the environment boundary and
/// reported success: a loopback sink in `staging` published to the bare key `feed`, which is exactly the
/// key `default`'s own generator is attached at.</summary>
public class SinkEnvironmentScopingTests
{
    private static SinkSpec Loopback(string target) =>
        new() { Kind = SinkKinds.Loopback, Loopback = new LoopbackSinkConfig { TargetSourceName = target } };

    [Fact]
    public void The_default_environment_returns_the_very_same_instance()
    {
        var spec = Loopback("feed");
        Assert.Same(spec, SinkEnvironmentScoping.Scope(spec, EnvKeys.Default));
        Assert.Same(spec, SinkEnvironmentScoping.Scope(spec, null));
    }

    [Fact]
    public void A_loopback_target_is_qualified_and_the_original_is_not_mutated()
    {
        var spec = Loopback("feed");
        var scoped = SinkEnvironmentScoping.Scope(spec, "staging");

        Assert.Equal("staging.feed", scoped.Loopback!.TargetSourceName);
        // The catalog keeps what the author wrote — an export from staging must stay importable into prod.
        Assert.Equal("feed", spec.Loopback!.TargetSourceName);
    }

    [Fact]
    public void Qualification_happens_before_the_name_placeholder_is_expanded()
    {
        // LoopbackSinkClient expands "{name}" itself, so the prefix has to go on first or the result
        // would be "{name}" -> "orders_loop" -> never qualified at all.
        var scoped = SinkEnvironmentScoping.Scope(Loopback("{name}_loop"), "staging");
        Assert.Equal("staging.{name}_loop", scoped.Loopback!.TargetSourceName);
    }

    [Fact]
    public void A_duplex_sinks_source_name_is_qualified_too()
    {
        var spec = new SinkSpec { Kind = SinkKinds.Duplex, Duplex = new DuplexSinkConfig { SourceName = "orders_out" } };
        Assert.Equal("staging.orders_out", SinkEnvironmentScoping.Scope(spec, "staging").Duplex!.SourceName);
    }

    [Fact]
    public void A_sink_that_names_something_outside_this_process_is_left_alone()
    {
        // An environment has no opinion about a NATS subject, a URL, a database table or a file path —
        // qualifying one would rename an operator's topic out from under them.
        var nats = new SinkSpec { Kind = SinkKinds.Nats, Nats = new NatsPubConfig { Url = "nats://host:4222", Subject = "rows" } };
        Assert.Same(nats, SinkEnvironmentScoping.Scope(nats, "staging"));
    }
}

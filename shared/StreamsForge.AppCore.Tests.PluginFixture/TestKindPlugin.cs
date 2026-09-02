using StreamsForge.AppCore.Plugins;

namespace StreamsForge.AppCore.Tests.PluginFixture;

/// <summary>
/// A minimal out-of-tree plugin used ONLY as a build fixture for <c>StreamsForgePlugins</c> /
/// <c>UiPluginsEndpoints</c> tests in <c>shared/StreamsForge.AppCore.Tests</c>: it registers no transport
/// (nothing here needs one) and carries one embedded console UI module, <c>ui-plugins/test-kind.js</c>,
/// via the convention those tests exercise — <c>&lt;EmbeddedResource Include="ui-plugins/*.js"
/// LogicalName="ui-plugins/%(Filename)%(Extension)" /&gt;</c> in this project's csproj.
///
/// <para>Never referenced directly by test code (that would load this assembly into the test process the
/// normal way, and a second explicit load of the same identity from a copied file would collide with it).
/// Tests load the BUILT DLL from a temp directory through <see cref="StreamsForgePlugins.LoadFrom"/>
/// instead, exactly as an operator would drop a real plugin into <c>plugins/</c>.</para>
/// </summary>
public sealed class TestKindPlugin : IStreamsForgePlugin
{
    public string Name => "test-kind-plugin";

    public void Register()
    {
        // Nothing to register — this fixture exists only to prove the embedded ui-plugins/*.js resource
        // convention, not a transport kind.
    }
}

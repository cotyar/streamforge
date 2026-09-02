using StreamsForge.AppCore.Plugins;

namespace StreamsForge.Plugins.Fix;

/// <summary>
/// Installs the <c>fix</c>/<c>fix-duplex</c> transports (<see cref="StreamsForge.Connectors.Fix.FixConnectors"/>)
/// as a server plugin instead of a host ProjectReference. Merged to one self-contained DLL by
/// <c>plugins/ILRepack.Plugins.targets</c> — an operator installs this by copying ONE file into the
/// host's <c>plugins/</c> directory (or wherever <c>Plugins:Path</c> points).
/// </summary>
public sealed class FixPlugin : IStreamsForgePlugin
{
    public string Name => "fix";

    public void Register() => StreamsForge.Connectors.Fix.FixConnectors.RegisterAll();
}

using StreamsForge.AppCore.Plugins;

namespace StreamsForge.Plugins.Quant;

/// <summary>
/// Installs the QLNet-backed pricing scalars (<see cref="StreamsForge.Quant.QuantFunctions"/>) as a
/// server plugin instead of a host ProjectReference. Merged to one self-contained DLL by
/// <c>plugins/ILRepack.Plugins.targets</c> — an operator installs this by copying ONE file into the
/// host's <c>plugins/</c> directory (or wherever <c>Plugins:Path</c> points).
/// </summary>
public sealed class QuantPlugin : IStreamsForgePlugin
{
    public string Name => "quant";

    public void Register() => StreamsForge.Quant.QuantFunctions.RegisterAll();
}

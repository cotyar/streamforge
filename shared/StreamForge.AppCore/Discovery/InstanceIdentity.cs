using System.Text.Json;

namespace StreamForge.AppCore.Discovery;

/// <summary>
/// Plan 016 wave 5: this instance's stable identity, persisted at <c>{DataDir}/instance.json</c>.
///
/// <para><b>Why it is persisted at all.</b> A peer that changed identity on every restart makes "is this
/// the same instance I federated from yesterday" unanswerable, which is the one question a directory
/// exists to answer. A GUID in a one-key file is the cheapest thing that survives a restart, and it lives
/// next to the state the instance already owns — deleting the data dir (the documented reseed) resets the
/// identity too, which is the correct semantics: that IS a new instance.</para>
///
/// <para><b>An unwritable data dir does not stop the host.</b> Discovery metadata is not worth refusing to
/// start over: an id is generated in memory and the instance simply has a new identity each restart. That
/// degradation is silent by design at this layer — the caller logs it if it cares.</para>
/// </summary>
public static class InstanceIdentity
{
    public const string FileName = "instance.json";

    /// <summary>The id recorded in <paramref name="dataDir"/>, creating and persisting one if the file is
    /// absent or unreadable. Never throws.</summary>
    // ponytail: read-then-write with no lock. Two hosts sharing one data dir is already unsupported
    // (Orleans' JsonFileGrainStorage assumes sole ownership); make this atomic if that ever changes.
    public static string LoadOrCreate(string dataDir)
    {
        var path = Path.Combine(string.IsNullOrWhiteSpace(dataDir) ? "." : dataDir, FileName);
        try
        {
            if (File.Exists(path))
            {
                using var doc = JsonDocument.Parse(File.ReadAllText(path));
                if (doc.RootElement.TryGetProperty("instanceId", out var prop) &&
                    prop.GetString() is { Length: > 0 } existing)
                {
                    return existing;
                }
            }
        }
        catch
        {
            // Corrupt or unreadable — fall through and rewrite it rather than fail a boot over metadata.
        }

        var id = Guid.NewGuid().ToString("n");
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
            File.WriteAllText(path, JsonSerializer.Serialize(new { instanceId = id }) + "\n");
        }
        catch
        {
            // Unwritable: this instance gets a fresh identity every restart. See the class note.
        }

        return id;
    }
}

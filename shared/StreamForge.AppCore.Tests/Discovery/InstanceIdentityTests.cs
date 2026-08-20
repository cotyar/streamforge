using StreamForge.AppCore.Discovery;
using Xunit;

namespace StreamForge.AppCore.Tests.Discovery;

/// <summary>Plan 016 wave 5 — <see cref="InstanceIdentity"/>. Ownership: track A. Each test gets its own
/// temp directory (never a shared one — the class is a pure function of the directory it is given, so a
/// shared fixture would just be extra ceremony around isolation the temp dir already gives for free).</summary>
public sealed class InstanceIdentityTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "sf-instance-identity-tests-" + Guid.NewGuid().ToString("n"));

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }
    }

    [Fact]
    public void LoadOrCreate_writes_a_non_empty_id_and_the_file_exists_after()
    {
        var id = InstanceIdentity.LoadOrCreate(_dir);

        Assert.False(string.IsNullOrWhiteSpace(id));
        Assert.True(File.Exists(Path.Combine(_dir, InstanceIdentity.FileName)));
    }

    [Fact]
    public void LoadOrCreate_survives_a_restart_with_the_same_data_dir()
    {
        var first = InstanceIdentity.LoadOrCreate(_dir);
        var second = InstanceIdentity.LoadOrCreate(_dir); // simulates a second process start against the same dir

        Assert.Equal(first, second);
    }

    [Fact]
    public void LoadOrCreate_gives_different_data_dirs_different_ids()
    {
        var otherDir = _dir + "-other";
        try
        {
            var a = InstanceIdentity.LoadOrCreate(_dir);
            var b = InstanceIdentity.LoadOrCreate(otherDir);

            Assert.NotEqual(a, b);
        }
        finally
        {
            if (Directory.Exists(otherDir))
            {
                Directory.Delete(otherDir, recursive: true);
            }
        }
    }

    [Fact]
    public void LoadOrCreate_recovers_from_a_corrupt_file_instead_of_throwing()
    {
        Directory.CreateDirectory(_dir);
        File.WriteAllText(Path.Combine(_dir, InstanceIdentity.FileName), "{ not json");

        var id = InstanceIdentity.LoadOrCreate(_dir);

        Assert.False(string.IsNullOrWhiteSpace(id));
    }

    [Fact]
    public void LoadOrCreate_never_throws_on_an_empty_data_dir_value()
    {
        // Empty/whitespace DataDir is documented as "the working directory" on StreamForgeApiOptions —
        // this just asserts the degenerate input doesn't throw; it deliberately does not assert WHERE
        // the file lands, since that would make the test depend on the process's CWD. Cleans up the file
        // it necessarily drops in the test runner's CWD so repeated runs don't accumulate/mask a stale id.
        var cwdFile = Path.Combine(".", InstanceIdentity.FileName);
        try
        {
            var id = InstanceIdentity.LoadOrCreate("");

            Assert.False(string.IsNullOrWhiteSpace(id));
        }
        finally
        {
            if (File.Exists(cwdFile))
            {
                File.Delete(cwdFile);
            }
        }
    }
}

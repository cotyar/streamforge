using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace StreamsForge.Chain.Tests;

/// <summary>
/// A development certificate for a TLS chain test, minted by running the repo's OWN
/// <c>tools/tls/dev-cert.sh</c> rather than by calling <c>CertificateRequest</c> from C#.
///
/// <para>That is deliberate. The script is what an operator is told to run (the host's startup failure
/// message names it, and so does <c>SECURITY.md</c>), so a test that mints its certificate some other
/// way would leave the documented path completely unexercised — a broken SAN list or a wrong key
/// permission would be found by whoever followed the docs, not by CI. Running it here means a change
/// that breaks the script breaks these tests.</para>
///
/// <para>Locates the script from THIS file's compile-time path, the same trick
/// <see cref="HostProcess.HostBinDir"/> uses, so nothing depends on the runner's working
/// directory.</para>
/// </summary>
public sealed class DevCert : IDisposable
{
    private DevCert(string dir, string certPath, string keyPath)
    {
        Dir = dir;
        CertPath = certPath;
        KeyPath = keyPath;
    }

    public string Dir { get; }

    /// <summary>The PEM certificate. Doubles as a trust anchor: the script self-signs with
    /// <c>CA:TRUE</c>, so this same file is what another instance gets as
    /// <c>--Tls:TrustedCaPath</c>.</summary>
    public string CertPath { get; }

    public string KeyPath { get; }

    /// <summary>The host arguments that turn TLS on with this pair — ready to splat into
    /// <c>HostProcess</c>'s <c>extraArgs</c>.</summary>
    public string[] HostArgs =>
    [
        "--Tls:Enabled", "true",
        "--Kestrel:Certificates:Default:Path", CertPath,
        "--Kestrel:Certificates:Default:KeyPath", KeyPath,
    ];

    /// <summary>Path to <c>tools/tls/dev-cert.sh</c>, or null when it is missing.</summary>
    public static string? ScriptPath([CallerFilePath] string thisFile = "")
    {
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "..", "..", "tools", "tls", "dev-cert.sh"));
        return File.Exists(path) ? path : null;
    }

    /// <summary>Null when a pair can be minted here; otherwise the one-line reason a test should report
    /// as its skip. Same convention as <see cref="HostProcess.Preflight"/> — plain xunit v2 cannot skip
    /// dynamically, so the caller turns this into an explicit "skipped: ..." assertion.</summary>
    public static string? Preflight()
    {
        if (ScriptPath() is null)
        {
            return "tools/tls/dev-cert.sh not found — cannot mint a development certificate";
        }
        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS())
        {
            return "tools/tls/dev-cert.sh is a bash script — TLS chain tests run on Linux/macOS only";
        }
        return null;
    }

    /// <summary>Runs the script into a fresh temp directory. Extra SANs beyond the script's own
    /// <c>DNS:localhost,IP:127.0.0.1</c> are passed straight through.</summary>
    public static DevCert Create(params string[] extraSans)
    {
        var script = ScriptPath()
            ?? throw new InvalidOperationException("tools/tls/dev-cert.sh not found — call Preflight() first");
        var dir = Directory.CreateTempSubdirectory("sf-tls-cert-").FullName;

        var psi = new ProcessStartInfo("/bin/bash")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add(script);
        psi.ArgumentList.Add(dir);
        foreach (var san in extraSans)
        {
            psi.ArgumentList.Add(san);
        }

        using var proc = Process.Start(psi)
            ?? throw new InvalidOperationException("failed to start tools/tls/dev-cert.sh");
        var stdout = proc.StandardOutput.ReadToEnd();
        var stderr = proc.StandardError.ReadToEnd();
        proc.WaitForExit();
        if (proc.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"tools/tls/dev-cert.sh exited {proc.ExitCode}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}");
        }

        var cert = Path.Combine(dir, "cert.pem");
        var key = Path.Combine(dir, "key.pem");
        if (!File.Exists(cert) || !File.Exists(key))
        {
            throw new InvalidOperationException(
                $"tools/tls/dev-cert.sh succeeded but produced no cert.pem/key.pem in {dir}\n{stdout}");
        }
        return new DevCert(dir, cert, key);
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(Dir, recursive: true);
        }
        catch
        {
            // best-effort cleanup
        }
    }
}

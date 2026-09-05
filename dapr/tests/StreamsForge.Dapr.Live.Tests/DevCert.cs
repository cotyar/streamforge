using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// A development certificate for a Dapr-flavor TLS live test, minted by running the repo's OWN
/// <c>tools/tls/dev-cert.sh</c> rather than by calling <c>CertificateRequest</c> from C#. A deliberate
/// near-copy of <c>orleans/tests/StreamsForge.Chain.Tests/DevCert.cs</c>: same script, same reasoning,
/// a different assembly (these two test projects share no code — one references the Orleans host for
/// build order, the other the Dapr host, and neither references the other).
///
/// <para>Running the script rather than reimplementing it is the point. The script is what an operator
/// is told to run — the host's own startup failure message names it (see
/// <c>dapr/src/StreamsForge.Dapr.Host/Program.cs</c>'s <c>Tls:Enabled</c> fail-fast, which prints
/// "tools/tls/dev-cert.sh &lt;out-dir&gt;") — so a test that minted its certificate some other way would
/// leave the documented path unexercised on this flavor too.</para>
///
/// <para><b>The one Dapr-only difference is in <see cref="DaprRunArgs"/>, not here.</b> The HOST
/// arguments this type returns are byte-identical to the Orleans ones, because plan 025 D9 made the
/// Dapr host's TLS configuration identical on purpose. What is extra is that the Dapr SIDECAR also
/// calls the app port and speaks cleartext http unless told otherwise, so a TLS app port additionally
/// needs <c>dapr run --app-protocol https</c> — see <see cref="DaprHostProcess.DaprRunExtraArgs"/>.</para>
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
    /// <c>CA:TRUE</c>, so this same file is what a client gets as <c>grpcurl -cacert</c> or another
    /// instance as <c>--Tls:TrustedCaPath</c>.</summary>
    public string CertPath { get; }

    public string KeyPath { get; }

    /// <summary>The host arguments that turn TLS on with this pair — ready to splat into
    /// <c>DaprHostProcess</c>'s <c>extraArgs</c> (which land after the <c>--</c> separator, i.e. on the
    /// APP's command line, not <c>dapr run</c>'s).</summary>
    public string[] HostArgs =>
    [
        "--Tls:Enabled", "true",
        "--Kestrel:Certificates:Default:Path", CertPath,
        "--Kestrel:Certificates:Default:KeyPath", KeyPath,
    ];

    /// <summary>The <c>dapr run</c> arguments a TLS app port needs, for
    /// <see cref="DaprHostProcess.DaprRunExtraArgs"/>. Without this the sidecar waits forever for the
    /// app port (it probes it over cleartext http), and the whole instance never becomes healthy —
    /// which is the Dapr-only half of plan 025 D9.</summary>
    public static string[] DaprRunArgs => ["--app-protocol", "https"];

    /// <summary>Path to <c>tools/tls/dev-cert.sh</c>, or null when it is missing. Located from THIS
    /// file's compile-time path (this project lives at <c>dapr/tests/StreamsForge.Dapr.Live.Tests/</c>,
    /// three levels below the repo root), the same trick <see cref="DaprHostProcess.HostBinDir"/>
    /// uses.</summary>
    public static string? ScriptPath([CallerFilePath] string thisFile = "")
    {
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "..", "..", "tools", "tls", "dev-cert.sh"));
        return File.Exists(path) ? path : null;
    }

    /// <summary>Null when a pair can be minted here; otherwise the one-line reason a test should report
    /// as its skip. Same "record, don't throw" convention as <see cref="DaprHostProcess.Preflight"/>.</summary>
    public static string? Preflight()
    {
        if (ScriptPath() is null)
        {
            return "tools/tls/dev-cert.sh not found — cannot mint a development certificate";
        }
        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS())
        {
            return "tools/tls/dev-cert.sh is a bash script — TLS live tests run on Linux/macOS only";
        }
        return null;
    }

    /// <summary>Runs the script into a fresh temp directory. Extra SANs beyond the script's own
    /// <c>DNS:localhost,IP:127.0.0.1</c> are passed straight through.</summary>
    public static DevCert Create(params string[] extraSans)
    {
        var script = ScriptPath()
            ?? throw new InvalidOperationException("tools/tls/dev-cert.sh not found — call Preflight() first");
        var dir = Directory.CreateTempSubdirectory("sf-dapr-tls-cert-").FullName;

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

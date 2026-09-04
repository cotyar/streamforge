using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;

namespace StreamsForge.AppCore.Net;

/// <summary>
/// The one place this platform decides whether it trusts a REMOTE server's TLS certificate.
///
/// <para>Everything StreamsForge dials out to — a federated peer's gRPC stream, an HTTP sink, the peer
/// directory's probe, a <c>url</c> source's fetch, an OpenAPI document fetch — goes through a handler
/// built here, so a private certificate authority is configured ONCE
/// (<c>Tls:TrustedCaPath</c>) rather than five times, and the escape hatch
/// (<c>Tls:AcceptAnyCertificate</c>) is one grep away from an auditor instead of scattered across
/// call sites.</para>
///
/// <para><b>Default is the system trust store, untouched.</b> With neither setting configured,
/// <see cref="NewHandler"/> returns a plain <see cref="SocketsHttpHandler"/> with NO validation
/// callback at all — byte-for-byte the behaviour every one of those call sites had before this type
/// existed. A publicly-trusted <c>https://</c> endpoint needs no configuration here, and never did.</para>
///
/// <para><b>Why the callback is not simply "return true when a custom CA is set".</b> A custom root
/// replaces the trust anchor; it does not excuse the certificate from being the right certificate for
/// the host being dialled. So <see cref="Validate"/> keeps the two failure classes apart: a chain that
/// does not reach a known root is what an extra CA is allowed to fix, and a NAME MISMATCH is not —
/// <see cref="SslPolicyErrors.RemoteCertificateNameMismatch"/> fails even when the presented leaf
/// chains perfectly to the configured CA, because "signed by our CA" and "is the server we asked for"
/// are different questions and only the second one stops an in-cluster impersonation.</para>
///
/// <para><b>Ordering matters in <see cref="Validate"/>.</b> The
/// <see cref="SslPolicyErrors.None"/> short-circuit comes FIRST because
/// <see cref="X509ChainTrustMode.CustomRootTrust"/> <i>replaces</i> the system roots rather than
/// adding to them: a certificate the OS already trusts would be rebuilt against a store containing
/// only the extra CA and rejected. Nothing already-valid ever reaches the custom chain, which is what
/// makes <c>Tls:TrustedCaPath</c> purely additive.</para>
///
/// <para><b>Configuration is global, once, at startup.</b> <see cref="Configure"/> throws if a handler
/// has already been created — the five shared clients are <c>static readonly Lazy&lt;HttpClient&gt;</c>
/// singletons and a handler already handed to one of them cannot be re-pointed, so a late
/// <c>Configure</c> would silently apply to some call sites and not others. Host startup calls it
/// before anything can dial.</para>
/// </summary>
public static class OutboundTls
{
    /// <summary>Configuration key for the PEM file of extra trusted certificate authorities.</summary>
    public const string TrustedCaPathKey = "Tls:TrustedCaPath";

    /// <summary>Configuration key for the development-only "trust anything" switch.</summary>
    public const string AcceptAnyCertificateKey = "Tls:AcceptAnyCertificate";

    private static readonly Lock Gate = new();

    private static X509Certificate2Collection? _extraRoots;
    private static bool _acceptAny;
    private static bool _handlerCreated;

    /// <summary>True when either knob is in play, i.e. when <see cref="NewHandler"/> installs
    /// <see cref="Validate"/> instead of leaving the platform's own validation alone.</summary>
    public static bool IsConfigured => _acceptAny || _extraRoots is { Count: > 0 };

    /// <summary>The validation callback to hand to a component that builds its own TLS stack (the gRPC
    /// channel), or <c>null</c> when nothing is configured — <c>null</c> means "use the system trust
    /// store", which is not the same as a callback that returns <c>errors == None</c>, because a
    /// caller may treat a present callback as a reason to skip its own defaults.</summary>
    public static RemoteCertificateValidationCallback? Callback => IsConfigured ? Validate : null;

    /// <summary>
    /// Reads the outbound-trust configuration. Call exactly once, from host startup, BEFORE anything
    /// dials out.
    /// </summary>
    /// <param name="trustedCaPath"><c>Tls:TrustedCaPath</c> — a PEM file that may hold SEVERAL
    /// certificates (a full private-CA bundle); all of them become custom trust anchors. Null/blank =
    /// no extra anchors. A path that does not exist, or a file with no certificate in it, throws:
    /// silently falling back to the system store would turn a typo'd CA path into "every federated
    /// connection now fails at TLS" with nothing saying why.</param>
    /// <param name="acceptAnyCertificate"><c>Tls:AcceptAnyCertificate</c> — DEVELOPMENT ONLY. Accepts
    /// any certificate from any server, which removes the whole point of TLS on the outbound side.
    /// Logged as a warning at startup so it cannot be on in production unnoticed.</param>
    /// <param name="log">Optional logger for the <paramref name="acceptAnyCertificate"/> warning and
    /// the loaded-CA count.</param>
    /// <exception cref="InvalidOperationException">A handler has already been created — see the type
    /// doc for why re-pointing after the fact is worse than failing.</exception>
    public static void Configure(string? trustedCaPath, bool acceptAnyCertificate, ILogger? log = null)
        => Configure(trustedCaPath, acceptAnyCertificate, log, enforceOrdering: true);

    private static void Configure(
        string? trustedCaPath,
        bool acceptAnyCertificate,
        ILogger? log,
        bool enforceOrdering)
    {
        lock (Gate)
        {
            if (enforceOrdering && _handlerCreated)
            {
                throw new InvalidOperationException(
                    "OutboundTls.Configure was called after an outbound HTTP handler had already been "
                  + "created. Outbound TLS trust is process-global and is captured by the shared "
                  + "HttpClient singletons the first time one of them dials; configure it at host "
                  + "startup, before any connector, sink or probe runs.");
            }

            X509Certificate2Collection? roots = null;
            if (!string.IsNullOrWhiteSpace(trustedCaPath))
            {
                var path = trustedCaPath.Trim();
                if (!File.Exists(path))
                {
                    throw new InvalidOperationException(
                        $"{TrustedCaPathKey} points at '{path}', which does not exist. Give it a PEM file "
                      + "holding the certificate authority (or authorities) this instance should trust "
                      + "for outbound TLS, or remove the setting to use the system trust store.");
                }

                roots = [];
                roots.ImportFromPemFile(path);
                if (roots.Count == 0)
                {
                    throw new InvalidOperationException(
                        $"{TrustedCaPathKey} file '{path}' contains no certificate. It must be PEM "
                      + "(-----BEGIN CERTIFICATE-----), not DER and not a private key.");
                }

                log?.LogInformation(
                    "Outbound TLS: {Count} extra trusted certificate authorit(y/ies) loaded from {Path}.",
                    roots.Count,
                    path);
            }

            _extraRoots = roots;
            _acceptAny = acceptAnyCertificate;

            if (acceptAnyCertificate)
            {
                log?.LogWarning(
                    "{Key} is TRUE: this instance accepts ANY server certificate on every outbound "
                  + "HTTPS/gRPC connection, including an expired, self-signed or actively "
                  + "impersonating one. Development only — never leave this set in a deployment that "
                  + "talks to anything real.",
                    AcceptAnyCertificateKey);
            }
        }
    }

    /// <summary>
    /// A fresh outbound handler honouring whatever <see cref="Configure"/> was given. Plain (no
    /// callback) when nothing is configured, so the default path stays exactly the platform's own.
    /// </summary>
    public static HttpMessageHandler NewHandler()
    {
        lock (Gate)
        {
            _handlerCreated = true;
            var handler = new SocketsHttpHandler();
            if (_acceptAny || _extraRoots is { Count: > 0 })
            {
                handler.SslOptions.RemoteCertificateValidationCallback = Validate;
            }
            return handler;
        }
    }

    /// <summary>
    /// The decision itself, kept public and free of I/O so it can be unit-tested against
    /// synthesised certificates rather than a live server. See the type doc for the ordering rules —
    /// <see cref="SslPolicyErrors.None"/> first (custom roots REPLACE the system store),
    /// name mismatch never forgiven.
    /// </summary>
    public static bool Validate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors errors)
    {
        // Already valid against the system trust store — never rebuild it against the custom store,
        // which contains only the extra CA and would reject it.
        if (errors == SslPolicyErrors.None)
        {
            return true;
        }

        if (_acceptAny)
        {
            return true;
        }

        var roots = _extraRoots;
        if (roots is not { Count: > 0 })
        {
            return false;
        }

        // A custom CA answers "who signed this", never "is this the host I asked for". Impersonation
        // by a holder of any certificate our CA ever issued is exactly what this line stops.
        if (errors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch))
        {
            return false;
        }

        // No certificate at all is not a trust question.
        if (errors.HasFlag(SslPolicyErrors.RemoteCertificateNotAvailable) || certificate is null)
        {
            return false;
        }

        X509Certificate2 leaf;
        try
        {
            leaf = certificate as X509Certificate2
                ?? X509CertificateLoader.LoadCertificate(certificate.Export(X509ContentType.Cert));
        }
        catch
        {
            return false;
        }

        using var custom = new X509Chain();
        custom.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        custom.ChainPolicy.CustomTrustStore.AddRange(roots);
        // A private CA typically has no CRL/OCSP endpoint reachable from here, and a revocation check
        // that cannot complete would fail every connection for a reason unrelated to trust.
        custom.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        custom.ChainPolicy.VerificationFlags = X509VerificationFlags.NoFlag;

        // Intermediates the server sent along come from the platform's own chain, if it built one —
        // without them a leaf issued by an intermediate under the configured root cannot be chained.
        if (chain is not null)
        {
            foreach (var element in chain.ChainElements)
            {
                custom.ChainPolicy.ExtraStore.Add(element.Certificate);
            }
        }

        try
        {
            return custom.Build(leaf);
        }
        catch
        {
            return false;
        }
    }

    /// <summary>Test-only: forgets both the configuration and the "a handler already exists" latch, so
    /// each test can configure the process-global state from scratch. Never called by product code —
    /// a running host that re-pointed its trust mid-flight is the bug <see cref="Configure"/>'s throw
    /// exists to prevent.</summary>
    internal static void Reset()
    {
        lock (Gate)
        {
            _extraRoots = null;
            _acceptAny = false;
            _handlerCreated = false;
        }
    }

    /// <summary>Test-only: <see cref="Reset"/> plus <see cref="Configure"/> as one step, with the
    /// "a handler already exists" check SUPPRESSED.
    ///
    /// <para>The suppression is the point. Test assemblies share one process, and an unrelated test in
    /// another xunit collection — a peer-probe test, an HTTP-sink test — dials something and thereby
    /// latches <c>_handlerCreated</c>. Without this, whether an <c>OutboundTls</c> test could configure
    /// itself would depend on which other test happened to run first, which is a flake, not a
    /// contract. The <see cref="Configure"/> ordering rule is still asserted directly, by the test that
    /// creates a handler itself and then expects the throw.</para></summary>
    internal static void ResetAndConfigure(string? trustedCaPath, bool acceptAnyCertificate)
    {
        lock (Gate)
        {
            Reset();
            Configure(trustedCaPath, acceptAnyCertificate, log: null, enforceOrdering: false);
        }
    }
}

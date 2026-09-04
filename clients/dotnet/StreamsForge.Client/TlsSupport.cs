using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace StreamsForge.Client;

/// <summary>
/// TLS plumbing shared by all three HTTP paths this client opens: the REST <see cref="AuthHttpClient"/>,
/// <see cref="GrpcTransport"/>'s channel, and <see cref="SignalRTransport"/>'s <c>HubConnection</c>
/// (both its negotiate/SSE/long-poll <c>HttpClient</c> and its WebSocket upgrade). One validator
/// built once in <see cref="StreamsForgeClient.ConnectAsync"/> from <see cref="ConnectOptions"/> and
/// handed to all three, so a caller configures TLS trust in exactly one place.
/// </summary>
internal static class TlsSupport
{
    /// <summary>
    /// Accepts three shapes: bare <c>host:port</c> (plaintext, unchanged from before TLS support),
    /// <c>http://host:port</c>, or <c>https://host:port</c> (TLS). The scheme check is a literal
    /// prefix match rather than <c>Uri.TryCreate(target, UriKind.Absolute, ...)</c> on the raw
    /// string: <c>"localhost:9299"</c> is itself a syntactically valid absolute URI whose scheme is
    /// "localhost" (a URI scheme is any <c>[a-zA-Z][a-zA-Z0-9+.-]*</c> token followed by
    /// <c>:</c>) -- parsing it that way silently drops the port instead of failing loudly.
    /// </summary>
    internal static Uri ParseGrpcTarget(string target)
    {
        if (target.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ||
            target.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
        {
            return new Uri(target, UriKind.Absolute);
        }
        return new Uri($"http://{target}", UriKind.Absolute);
    }

    /// <summary>
    /// Builds the one <see cref="RemoteCertificateValidationCallback"/> shared by every TLS path,
    /// or <c>null</c> when neither TLS option is set -- callers pass <c>null</c> straight through to
    /// the BCL's own default system-trust validation, unmodified.
    /// <see cref="ConnectOptions.AcceptAnyCertificate"/> wins outright when set (dev only: accepts
    /// literally anything, name mismatch included). Otherwise, when
    /// <see cref="ConnectOptions.CaCertificatePath"/> names a PEM file (one or more certificates),
    /// the returned callback accepts a certificate trusted by the machine's OWN store exactly as
    /// before, OR one that chains to a certificate in that PEM file -- never both loosened at once,
    /// and a hostname/SAN mismatch is never forgiven by the extra CA either way.
    /// </summary>
    internal static RemoteCertificateValidationCallback? BuildValidator(ConnectOptions options)
    {
        if (options.AcceptAnyCertificate)
        {
            return (_, _, _, _) => true;
        }

        if (string.IsNullOrEmpty(options.CaCertificatePath))
        {
            return null;
        }

        // Imported once, up front: ImportFromPemFile throws immediately on a missing/malformed
        // file rather than deferring that failure to the first TLS handshake.
        var extraCa = new X509Certificate2Collection();
        extraCa.ImportFromPemFile(options.CaCertificatePath);

        return (_, certificate, chain, sslPolicyErrors) => Validate(certificate, chain, sslPolicyErrors, extraCa);
    }

    /// <summary>
    /// Mirrors <c>shared/StreamsForge.AppCore/Net/OutboundTls.Validate</c> (this SDK is standalone
    /// and does not reference that assembly, so the ~25 lines are copied rather than shared): the
    /// <see cref="SslPolicyErrors.None"/> short-circuit comes FIRST because
    /// <see cref="X509ChainTrustMode.CustomRootTrust"/> REPLACES the system roots rather than adding
    /// to them -- a certificate the OS already trusts would be rebuilt against a store containing
    /// only the extra CA and rejected. A name/SAN mismatch is never forgiven, even by a chain that
    /// would otherwise validate against the configured CA: "signed by our CA" and "is the server we
    /// asked for" are different questions, and only the second one stops an impersonator holding any
    /// certificate that CA ever issued.
    /// </summary>
    private static bool Validate(X509Certificate? certificate, X509Chain? chain, SslPolicyErrors sslPolicyErrors, X509Certificate2Collection extraCa)
    {
        // Already valid against the system trust store -- never rebuild it against the custom
        // store, which contains only the extra CA and would reject it.
        if (sslPolicyErrors == SslPolicyErrors.None) return true;

        if (sslPolicyErrors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch)) return false;

        // No certificate at all is not a trust question.
        if (sslPolicyErrors.HasFlag(SslPolicyErrors.RemoteCertificateNotAvailable) || certificate is null) return false;

        X509Certificate2 leaf;
        try
        {
            leaf = certificate as X509Certificate2 ?? X509CertificateLoader.LoadCertificate(certificate.Export(X509ContentType.Cert));
        }
        catch
        {
            return false;
        }

        using var customChain = new X509Chain();
        customChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        customChain.ChainPolicy.CustomTrustStore.AddRange(extraCa);
        // A dev CA (tools/tls/dev-cert.sh) ships no CRL/OCSP endpoint reachable from here, and a
        // revocation check that cannot complete would fail every connection for a reason unrelated
        // to whether this is the right certificate.
        customChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        // Deliberately NOT X509VerificationFlags.AllowUnknownCertificateAuthority: that flag tells
        // the chain engine to accept an otherwise-unrecognized root outright, which would make ANY
        // self-signed certificate validate under CustomRootTrust regardless of whether it is
        // actually one of the configured CAs -- the exact check this code exists to do.
        customChain.ChainPolicy.VerificationFlags = X509VerificationFlags.NoFlag;

        // Intermediates the server sent along come from the platform's own chain, if it built one --
        // without them a leaf issued by an intermediate under the configured root cannot be chained.
        if (chain is not null)
        {
            foreach (var element in chain.ChainElements)
            {
                customChain.ChainPolicy.ExtraStore.Add(element.Certificate);
            }
        }

        try
        {
            return customChain.Build(leaf);
        }
        catch
        {
            return false;
        }
    }
}

using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Xunit;

namespace StreamsForge.Client.Tests;

/// <summary>
/// Unit coverage for <see cref="TlsSupport"/> and the gRPC target/guess parsing it enables in
/// <see cref="StreamsForgeClient"/> -- no engine, no network, no real TLS handshake: the validator
/// callback is invoked directly with hand-built certificates and manufactured
/// <see cref="SslPolicyErrors"/>, exactly the parameters a real <c>SslStream</c> would pass it.
/// </summary>
public sealed class TlsSupportTests : IDisposable
{
    private readonly List<string> _tempFiles = [];

    public void Dispose()
    {
        foreach (var f in _tempFiles)
        {
            try { File.Delete(f); } catch { /* best effort */ }
        }
    }

    // ---- target parsing (3 shapes) ----

    [Fact]
    public void ParseGrpcTarget_BareHostPort_IsPlaintextHttp()
    {
        var uri = TlsSupport.ParseGrpcTarget("localhost:9299");
        Assert.Equal("http", uri.Scheme);
        Assert.Equal("localhost", uri.Host);
        Assert.Equal(9299, uri.Port);
    }

    [Fact]
    public void ParseGrpcTarget_ExplicitHttp_IsUnchanged()
    {
        var uri = TlsSupport.ParseGrpcTarget("http://localhost:9299");
        Assert.Equal("http", uri.Scheme);
        Assert.Equal("localhost", uri.Host);
        Assert.Equal(9299, uri.Port);
    }

    [Fact]
    public void ParseGrpcTarget_ExplicitHttps_IsTls()
    {
        var uri = TlsSupport.ParseGrpcTarget("https://127.0.0.1:7499");
        Assert.Equal("https", uri.Scheme);
        Assert.Equal("127.0.0.1", uri.Host);
        Assert.Equal(7499, uri.Port);
    }

    // ---- scheme-preserving +100 guess ----

    [Fact]
    public void DefaultGrpcTarget_PlainHttpUrl_GuessesPlainHostPort()
    {
        Assert.Equal("localhost:9299", StreamsForgeClient.DefaultGrpcTarget("http://localhost:9199"));
    }

    [Fact]
    public void DefaultGrpcTarget_HttpsUrl_PreservesHttpsScheme()
    {
        Assert.Equal("https://localhost:9299", StreamsForgeClient.DefaultGrpcTarget("https://localhost:9199"));
    }

    // ---- validator: accept-any ----

    [Fact]
    public void BuildValidator_AcceptAny_AcceptsEvenABadCertificate()
    {
        var validator = TlsSupport.BuildValidator(new ConnectOptions { AcceptAnyCertificate = true });
        Assert.NotNull(validator);

        using var unrelated = SelfSignedLeaf("CN=totally-unrelated");
        var accepted = validator!(this, unrelated, null, SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch);
        Assert.True(accepted);
    }

    // ---- validator: no TLS options configured ----

    [Fact]
    public void BuildValidator_NoOptionsConfigured_ReturnsNull()
    {
        Assert.Null(TlsSupport.BuildValidator(new ConnectOptions()));
    }

    // ---- validator: configured CA ----

    [Fact]
    public void BuildValidator_SystemTrustAlreadyOk_Accepts()
    {
        var caPath = WriteCaPem(out var ca, out _);
        using (ca)
        {
            var validator = TlsSupport.BuildValidator(new ConnectOptions { CaCertificatePath = caPath });
            using var leaf = SelfSignedLeaf("CN=irrelevant-here");
            // SslPolicyErrors.None means the BCL's own system-trust validation already succeeded --
            // the configured CA is never even consulted.
            Assert.True(validator!(this, leaf, null, SslPolicyErrors.None));
        }
    }

    [Fact]
    public void BuildValidator_LeafSignedByConfiguredCa_AcceptsDespiteChainError()
    {
        var caPath = WriteCaPem(out var ca, out var caKey);
        using (ca)
        using (caKey)
        {
            using var leaf = SignedLeaf(ca, caKey, "CN=localhost");
            var validator = TlsSupport.BuildValidator(new ConnectOptions { CaCertificatePath = caPath });

            // As the machine's own trust store would report it: the chain doesn't reach a
            // system-trusted root, only RemoteCertificateChainErrors is set.
            var accepted = validator!(this, leaf, null, SslPolicyErrors.RemoteCertificateChainErrors);
            Assert.True(accepted);
        }
    }

    [Fact]
    public void BuildValidator_UnrelatedCertificate_IsRejected()
    {
        var caPath = WriteCaPem(out var ca, out _);
        using (ca)
        {
            var validator = TlsSupport.BuildValidator(new ConnectOptions { CaCertificatePath = caPath });
            // Self-signed, chains to nothing in the configured CA file at all.
            using var unrelated = SelfSignedLeaf("CN=someone-elses-server");
            var accepted = validator!(this, unrelated, null, SslPolicyErrors.RemoteCertificateChainErrors);
            Assert.False(accepted);
        }
    }

    [Fact]
    public void BuildValidator_NameMismatch_IsRejectedEvenWithAMatchingCa()
    {
        var caPath = WriteCaPem(out var ca, out var caKey);
        using (ca)
        using (caKey)
        {
            using var leaf = SignedLeaf(ca, caKey, "CN=wrong-host");
            var validator = TlsSupport.BuildValidator(new ConnectOptions { CaCertificatePath = caPath });

            // A CHAIN that validates fine against the configured CA, but the server also failed
            // the hostname/SAN check -- must still be rejected.
            var accepted = validator!(
                this, leaf, null, SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch);
            Assert.False(accepted);
        }
    }

    // ---- test cert helpers ----

    private string WriteCaPem(out X509Certificate2 ca, out RSA? caKeyOut)
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=Test Dev CA", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        req.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        // Longer-lived than any leaf it signs below -- CertificateRequest.Create refuses a leaf
        // notAfter later than its issuer's.
        var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(60));

        var path = Path.Combine(Path.GetTempPath(), $"sf-dotnet-client-tls-test-ca-{Guid.NewGuid():N}.pem");
        File.WriteAllText(path, cert.ExportCertificatePem());
        _tempFiles.Add(path);

        ca = cert;
        caKeyOut = rsa;
        return path;
    }

    private static X509Certificate2 SignedLeaf(X509Certificate2 ca, RSA? caKey, string subject)
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(subject, rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        req.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        var serial = RandomNumberGenerator.GetBytes(16);
        // CreateSigned needs the issuer's *X509Certificate2 with the private key attached*, which
        // `ca` already carries (CreateSelfSigned attaches it) -- caKey is only kept alive by the
        // caller so the CA's RSA instance isn't disposed out from under it.
        _ = caKey;
        using var signed = req.Create(ca, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30), serial);
        // Re-import so the returned certificate owns its own state independent of `signed`/`rsa`.
        return X509CertificateLoader.LoadCertificate(signed.Export(X509ContentType.Cert));
    }

    private static X509Certificate2 SelfSignedLeaf(string subject)
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(subject, rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        return X509CertificateLoader.LoadCertificate(cert.Export(X509ContentType.Cert));
    }
}

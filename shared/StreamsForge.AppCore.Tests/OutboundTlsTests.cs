using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using StreamsForge.AppCore.Net;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>
/// <see cref="OutboundTls.Validate"/> against SYNTHESISED certificates — a real CA and a real leaf
/// minted in-process by <see cref="CertificateRequest"/>, not a live TLS handshake. The decision this
/// type guards ("do we trust the server we just dialled") is pure given
/// <c>(errors, certificate, chain)</c>, so it can be exercised exactly, including the two cases a
/// handshake test could not reliably produce on demand: a name mismatch on an otherwise perfect chain,
/// and an unrelated certificate signed by nobody we know.
///
/// <para><b>Serialized, and that is not optional.</b> <see cref="OutboundTls"/> holds process-global
/// state on purpose (see its type doc), so these tests share one mutable configuration. The collection
/// below disables parallelisation for this class and <see cref="Dispose"/> clears the state after every
/// test — without that, a test asserting "an unrelated cert is rejected" could observe another test's
/// <c>AcceptAnyCertificate</c>.</para>
///
/// <para>Seeding goes through <c>OutboundTls.ResetAndConfigure</c> rather than the public
/// <c>Configure</c> for a reason found the hard way: a test in ANOTHER xunit collection (peer probe,
/// HTTP sink) dials something, which latches "a handler already exists", and the public
/// <c>Configure</c> then throws here depending purely on scheduling. The ordering rule that latch
/// enforces is still asserted — by the one test that creates the handler itself.</para>
/// </summary>
[Collection(OutboundTlsCollection.Name)]
public sealed class OutboundTlsTests : IDisposable
{
    private readonly List<X509Certificate2> _owned = [];

    public void Dispose()
    {
        OutboundTls.Reset();
        foreach (var c in _owned)
        {
            c.Dispose();
        }
    }

    [Fact]
    public void SslPolicyErrorsNone_is_trusted_before_anything_else_is_consulted()
    {
        // Nothing configured at all: the callback would not even be installed, but the short-circuit is
        // what makes a custom trust store ADDITIVE rather than replacing the system roots, so it is
        // asserted directly. (CustomRootTrust replaces the system store — a publicly-trusted cert
        // rebuilt against a store holding only our private CA would be rejected.)
        var (_, leaf) = MintCaAndLeaf("valid.example");
        OutboundTls.ResetAndConfigure(WritePem(MintCaAndLeaf("other.example").Ca), acceptAnyCertificate: false);

        Assert.True(OutboundTls.Validate(this, leaf, chain: null, SslPolicyErrors.None));
    }

    [Fact]
    public void A_leaf_chaining_to_the_configured_CA_is_trusted_despite_chain_errors()
    {
        var (ca, leaf) = MintCaAndLeaf("peer.example");
        OutboundTls.ResetAndConfigure(WritePem(ca), acceptAnyCertificate: false);

        // RemoteCertificateChainErrors is exactly what a private-CA server produces against the system
        // store, and exactly what Tls:TrustedCaPath exists to answer.
        Assert.True(OutboundTls.Validate(
            this, leaf, chain: null, SslPolicyErrors.RemoteCertificateChainErrors));
    }

    [Fact]
    public void An_unrelated_certificate_is_rejected_even_with_a_CA_configured()
    {
        var (ca, _) = MintCaAndLeaf("peer.example");
        var (_, stranger) = MintCaAndLeaf("impostor.example");
        OutboundTls.ResetAndConfigure(WritePem(ca), acceptAnyCertificate: false);

        Assert.False(OutboundTls.Validate(
            this, stranger, chain: null, SslPolicyErrors.RemoteCertificateChainErrors));
    }

    [Fact]
    public void A_name_mismatch_is_rejected_even_when_the_leaf_chains_to_the_configured_CA()
    {
        // The point of the whole type: "our CA signed it" and "it is the host we asked for" are
        // different questions. A holder of ANY certificate this CA ever issued could otherwise
        // impersonate every other host under it.
        var (ca, leaf) = MintCaAndLeaf("peer.example");
        OutboundTls.ResetAndConfigure(WritePem(ca), acceptAnyCertificate: false);

        Assert.False(OutboundTls.Validate(
            this,
            leaf,
            chain: null,
            SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch));
        Assert.False(OutboundTls.Validate(
            this, leaf, chain: null, SslPolicyErrors.RemoteCertificateNameMismatch));
    }

    [Fact]
    public void With_no_CA_configured_a_chain_error_is_rejected()
    {
        OutboundTls.ResetAndConfigure(trustedCaPath: null, acceptAnyCertificate: false);
        var (_, leaf) = MintCaAndLeaf("peer.example");

        Assert.False(OutboundTls.Validate(
            this, leaf, chain: null, SslPolicyErrors.RemoteCertificateChainErrors));
        Assert.False(OutboundTls.IsConfigured);
        Assert.Null(OutboundTls.Callback);
    }

    [Fact]
    public void AcceptAnyCertificate_trusts_a_stranger_and_a_name_mismatch_alike()
    {
        OutboundTls.ResetAndConfigure(trustedCaPath: null, acceptAnyCertificate: true);
        var (_, stranger) = MintCaAndLeaf("impostor.example");

        Assert.True(OutboundTls.Validate(
            this,
            stranger,
            chain: null,
            SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch));
        Assert.True(OutboundTls.IsConfigured);
        Assert.NotNull(OutboundTls.Callback);
    }

    [Fact]
    public void A_missing_remote_certificate_is_never_trusted_by_a_custom_CA()
    {
        var (ca, _) = MintCaAndLeaf("peer.example");
        OutboundTls.ResetAndConfigure(WritePem(ca), acceptAnyCertificate: false);

        Assert.False(OutboundTls.Validate(
            this, certificate: null, chain: null, SslPolicyErrors.RemoteCertificateNotAvailable));
    }

    [Fact]
    public void A_PEM_holding_several_authorities_trusts_a_leaf_under_any_of_them()
    {
        var first = MintCaAndLeaf("one.example");
        var second = MintCaAndLeaf("two.example");
        var bundle = Path.Combine(NewTempDir(), "bundle.pem");
        File.WriteAllText(bundle, first.Ca.ExportCertificatePem() + "\n" + second.Ca.ExportCertificatePem() + "\n");

        OutboundTls.ResetAndConfigure(bundle, acceptAnyCertificate: false);

        Assert.True(OutboundTls.Validate(this, first.Leaf, null, SslPolicyErrors.RemoteCertificateChainErrors));
        Assert.True(OutboundTls.Validate(this, second.Leaf, null, SslPolicyErrors.RemoteCertificateChainErrors));
    }

    [Fact]
    public void Configure_after_a_handler_exists_throws_rather_than_silently_half_applying()
    {
        OutboundTls.ResetAndConfigure(trustedCaPath: null, acceptAnyCertificate: false);
        using var handler = OutboundTls.NewHandler();

        // The PUBLIC entry point, deliberately — this is the one test that asserts the ordering rule
        // itself, and it has just created the handler that must trip it.
        var ex = Assert.Throws<InvalidOperationException>(
            () => OutboundTls.Configure(trustedCaPath: null, acceptAnyCertificate: true));
        Assert.Contains("before any connector", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_TrustedCaPath_that_does_not_exist_throws_at_configuration_time()
    {
        var missing = Path.Combine(NewTempDir(), "nope.pem");
        var ex = Assert.Throws<InvalidOperationException>(
            () => OutboundTls.ResetAndConfigure(missing, acceptAnyCertificate: false));
        Assert.Contains(OutboundTls.TrustedCaPathKey, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_TrustedCaPath_holding_no_certificate_throws_rather_than_falling_back_silently()
    {
        var empty = Path.Combine(NewTempDir(), "empty.pem");
        File.WriteAllText(empty, "not a certificate\n");

        var ex = Assert.Throws<InvalidOperationException>(
            () => OutboundTls.ResetAndConfigure(empty, acceptAnyCertificate: false));
        Assert.Contains("no certificate", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void An_unconfigured_handler_carries_no_validation_callback_at_all()
    {
        OutboundTls.ResetAndConfigure(trustedCaPath: null, acceptAnyCertificate: false);
        using var handler = Assert.IsType<SocketsHttpHandler>(OutboundTls.NewHandler());

        // "No callback" is the default path staying byte-identical to what every call site had before
        // this type existed — not a callback that happens to return errors == None.
        Assert.Null(handler.SslOptions.RemoteCertificateValidationCallback);
    }

    [Fact]
    public void A_configured_handler_carries_the_validation_callback()
    {
        OutboundTls.ResetAndConfigure(trustedCaPath: null, acceptAnyCertificate: true);
        using var handler = Assert.IsType<SocketsHttpHandler>(OutboundTls.NewHandler());

        Assert.NotNull(handler.SslOptions.RemoteCertificateValidationCallback);
    }

    // ------------------------------------------------------------------
    // Certificate minting. A real CA (basicConstraints CA:TRUE) and a real leaf signed by it, so the
    // X509Chain built inside Validate does actual signature and constraint checking rather than a
    // shortcut.
    // ------------------------------------------------------------------

    private (X509Certificate2 Ca, X509Certificate2 Leaf) MintCaAndLeaf(string dnsName)
    {
        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest(
            $"CN={dnsName}-ca", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        caRequest.CertificateExtensions.Add(
            new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        caRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(caRequest.PublicKey, false));

        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = DateTimeOffset.UtcNow.AddDays(30);
        var ca = caRequest.CreateSelfSigned(notBefore, notAfter);

        using var leafKey = RSA.Create(2048);
        var leafRequest = new CertificateRequest(
            $"CN={dnsName}", leafKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        leafRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(dnsName);
        leafRequest.CertificateExtensions.Add(san.Build());
        leafRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(leafRequest.PublicKey, false));

        var serial = new byte[8];
        RandomNumberGenerator.Fill(serial);
        // Public-only: Validate never needs the private key, and a leaf carrying one behaves
        // differently on some platforms' chain building.
        using var signed = leafRequest.Create(ca, notBefore, notAfter.AddDays(-1), serial);
        var leaf = X509CertificateLoader.LoadCertificate(signed.RawData);

        _owned.Add(ca);
        _owned.Add(leaf);
        return (ca, leaf);
    }

    private string WritePem(X509Certificate2 cert)
    {
        var path = Path.Combine(NewTempDir(), "ca.pem");
        File.WriteAllText(path, cert.ExportCertificatePem());
        return path;
    }

    private static string NewTempDir() => Directory.CreateTempSubdirectory("sf-outbound-tls-").FullName;
}

/// <summary>One collection so <see cref="OutboundTlsTests"/> never runs in parallel with itself: the
/// type under test is process-global by design.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class OutboundTlsCollection
{
    public const string Name = "OutboundTls (process-global state)";
}

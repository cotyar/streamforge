package streamsforge

import java.io.File
import java.security.KeyStore
import java.security.SecureRandom
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager

/**
 * Everything [AuthClient] (REST, `java.net.http.HttpClient`) and [SignalRTransport] (OkHttp, via
 * the Java SignalR client) need to speak TLS the SAME way: one [SSLContext] plus the concrete
 * [X509TrustManager] backing it -- `java.net.http.HttpClient.Builder.sslContext` only needs the
 * former, but OkHttp's `sslSocketFactory(SSLSocketFactory, X509TrustManager)` needs both, so both
 * are carried together rather than re-deriving the trust manager twice. [insecure] rides along so
 * each caller also knows whether to additionally switch off hostname verification (see
 * [buildTlsConfig]'s doc).
 *
 * [GrpcTransport] does NOT consume this type -- grpc-netty-shaded's `SslContext`
 * (`io.grpc.netty.shaded.io.netty.handler.ssl.SslContext`) is a different, unrelated type from
 * `javax.net.ssl.SSLContext`, built via `GrpcSslContexts`/`InsecureTrustManagerFactory` instead.
 * [GrpcTransport] is therefore handed the raw `caFile`/`insecure` inputs directly and builds its
 * own Netty-flavored trust config from them -- same inputs, a parallel construction, because the
 * two SSL stacks don't share a common currency.
 */
// Not `internal`: AuthClient and SignalRTransport are public classes whose constructors take a
// TlsConfig? parameter, and Kotlin refuses a public API that exposes an internal type. Nothing
// about the type itself needs hiding -- it is three self-explanatory fields -- so it stays public
// even though nothing outside this library is expected to construct one directly (only
// [buildTlsConfig], which IS internal, does).
data class TlsConfig(val sslContext: SSLContext, val trustManager: X509TrustManager, val insecure: Boolean)

/** Trust-all, verify-nothing [X509TrustManager] for [insecure] connect() calls -- development
 * only, and paired with turning off hostname verification too (a self-signed dev cert routinely
 * fails that check regardless of the CA being trusted, e.g. when dialing an IP not in the cert's
 * SAN list). Never the default: a caller has to pass `insecure = true` on purpose. */
private object InsecureTrustManager : X509TrustManager {
    override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?) = Unit
    override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?) = Unit
    override fun getAcceptedIssuers(): Array<X509Certificate> = arrayOf()
}

/**
 * Builds the shared REST/SignalR TLS trust config for [StreamsForge.connect]'s `caFile`/`insecure`
 * parameters. Returns `null` when neither is set -- meaning "use the JVM's own default trust
 * store", the normal case for a host with a certificate from a CA the platform already trusts.
 *
 * [caFile] is a PEM file used as its OWN trust anchor (matching `tools/tls/dev-cert.sh`'s
 * self-signed certificate, which is documented there as working as both leaf and CA -- there is
 * no separate root to hand over). [insecure] trusts every certificate and, in each transport's own
 * wiring, additionally disables hostname verification -- development convenience only, never
 * silently defaulted on.
 */
internal fun buildTlsConfig(caFile: String?, insecure: Boolean): TlsConfig? {
    if (caFile == null && !insecure) return null
    val trustManager: X509TrustManager = if (insecure) {
        InsecureTrustManager
    } else {
        val certificateFactory = CertificateFactory.getInstance("X.509")
        val certificate = File(caFile!!).inputStream().use { certificateFactory.generateCertificate(it) } as X509Certificate
        val keyStore = KeyStore.getInstance(KeyStore.getDefaultType()).apply {
            load(null, null)
            setCertificateEntry("streamsforge-ca", certificate)
        }
        val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            .apply { init(keyStore) }
        trustManagerFactory.trustManagers.filterIsInstance<X509TrustManager>().firstOrNull()
            ?: throw StreamsForgeError("no X509TrustManager available for CA file $caFile")
    }
    val sslContext = SSLContext.getInstance("TLS").apply { init(null, arrayOf(trustManager), SecureRandom()) }
    return TlsConfig(sslContext, trustManager, insecure)
}

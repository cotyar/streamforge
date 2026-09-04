using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Net.Security;
using System.Text.Json;

namespace StreamsForge.Client;

/// <summary>
/// REST client with cached, self-refreshing StreamsForge auth. Ported from clients/python/src/streamsforge/_http.py:
/// the JWT is cached in memory for ~11h (the server issues 12h tokens) and re-minted exactly once
/// on any 401, then the request is retried once with the fresh token -- if THAT also 401s, this
/// raises rather than looping forever (a StreamsForge restart invalidates every token minted
/// before it, a normal event, but an auth system that is actually broken should fail loudly).
///
/// Shared by both live transports: <see cref="GetTokenAsync"/> is handed to <see cref="GrpcTransport"/>
/// and <see cref="SignalRTransport"/> as their token provider, so REST and the live transport
/// always carry the same JWT.
/// </summary>
internal sealed class AuthHttpClient : IAsyncDisposable
{
    private static readonly TimeSpan TokenLifetime = TimeSpan.FromHours(11); // server mints 12h; refresh a bit early

    private readonly HttpClient _http;
    private readonly string? _user;
    private readonly string? _password;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private string? _token;
    private DateTime? _tokenMintedAtUtc;

    public string BaseUrl { get; }

    public AuthHttpClient(string baseUrl, string? user, string? password, string? token = null, RemoteCertificateValidationCallback? certValidator = null)
    {
        BaseUrl = baseUrl.TrimEnd('/');
        var handler = new SocketsHttpHandler();
        if (certValidator is not null)
        {
            handler.SslOptions.RemoteCertificateValidationCallback = certValidator;
        }
        _http = new HttpClient(handler) { BaseAddress = new Uri(BaseUrl + "/") };
        _user = user;
        _password = password;
        _token = token;
        _tokenMintedAtUtc = token is not null ? DateTime.UtcNow : null;
    }

    public async ValueTask<string> GetTokenAsync(CancellationToken ct)
    {
        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_token is null || Expired()) await LoginLockedAsync(ct).ConfigureAwait(false);
            return _token!;
        }
        finally
        {
            _lock.Release();
        }
    }

    private bool Expired() => _tokenMintedAtUtc is null || DateTime.UtcNow - _tokenMintedAtUtc > TokenLifetime;

    private async Task LoginLockedAsync(CancellationToken ct)
    {
        if (string.IsNullOrEmpty(_user) || string.IsNullOrEmpty(_password))
        {
            throw new AuthException(
                "no StreamsForge credentials configured -- set ConnectOptions.User/Password or " +
                "the STREAMSFORGE_ADMIN_USER/STREAMSFORGE_ADMIN_PASS environment variables");
        }

        using var resp = await _http.PostAsJsonAsync("api/auth/login", new { username = _user, password = _password }, ct)
            .ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            throw new AuthException($"StreamsForge login failed: {(int)resp.StatusCode} {body}");
        }

        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false));
        _token = doc.RootElement.GetProperty("token").GetString();
        _tokenMintedAtUtc = DateTime.UtcNow;
    }

    public void InvalidateToken() => _token = null;

    /// <summary><paramref name="contentFactory"/> is called fresh for the initial attempt AND
    /// (only if needed) the one 401 retry, since an <see cref="HttpContent"/> instance cannot be
    /// resent once its stream has been consumed. <paramref name="auth"/> = false skips
    /// minting/attaching a Bearer token entirely -- used by the ingest path when only an ingest
    /// key is configured, so a caller that only feeds a source never forces an admin login.</summary>
    public async Task<HttpResponseMessage> SendAsync(
        HttpMethod method,
        string path,
        Func<HttpContent?>? contentFactory,
        bool auth,
        CancellationToken ct,
        Action<HttpRequestMessage>? configure = null)
    {
        if (!auth)
        {
            using var req = new HttpRequestMessage(method, path) { Content = contentFactory?.Invoke() };
            configure?.Invoke(req);
            return await _http.SendAsync(req, ct).ConfigureAwait(false);
        }

        var token = await GetTokenAsync(ct).ConfigureAwait(false);
        HttpResponseMessage resp;
        using (var req = new HttpRequestMessage(method, path) { Content = contentFactory?.Invoke() })
        {
            configure?.Invoke(req);
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            resp = await _http.SendAsync(req, ct).ConfigureAwait(false);
        }
        if (resp.StatusCode != HttpStatusCode.Unauthorized) return resp;
        resp.Dispose();

        InvalidateToken();
        var freshToken = await GetTokenAsync(ct).ConfigureAwait(false);
        using var retryReq = new HttpRequestMessage(method, path) { Content = contentFactory?.Invoke() };
        configure?.Invoke(retryReq);
        retryReq.Headers.Authorization = new AuthenticationHeaderValue("Bearer", freshToken);
        var retryResp = await _http.SendAsync(retryReq, ct).ConfigureAwait(false);
        if (retryResp.StatusCode == HttpStatusCode.Unauthorized)
            throw new AuthException($"StreamsForge rejected the re-minted token for {method} {path}");
        return retryResp;
    }

    public Task<HttpResponseMessage> GetAsync(string path, CancellationToken ct, bool auth = true) =>
        SendAsync(HttpMethod.Get, path, null, auth, ct);

    public Task<HttpResponseMessage> PostJsonAsync(string path, object body, CancellationToken ct, bool auth = true) =>
        SendAsync(HttpMethod.Post, path, () => JsonContent.Create(body), auth, ct);

    public Task<HttpResponseMessage> DeleteAsync(string path, CancellationToken ct, bool auth = true) =>
        SendAsync(HttpMethod.Delete, path, null, auth, ct);

    public ValueTask DisposeAsync()
    {
        _http.Dispose();
        _lock.Dispose();
        return ValueTask.CompletedTask;
    }
}

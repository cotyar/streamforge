using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;

namespace StreamForge.Client;

/// <summary>
/// SignalR live transport against <c>/hubs/stream</c>, in whichever single wire mode
/// (<see cref="HttpTransportType.WebSockets"/> / <see cref="HttpTransportType.ServerSentEvents"/> /
/// <see cref="HttpTransportType.LongPolling"/>) it is constructed with. Unlike the Python client,
/// this does NOT hand-roll the SignalR wire protocol (negotiate, the <c>\x1e</c>-delimited JSON
/// framing, the three byte pipes) -- <c>Microsoft.AspNetCore.SignalR.Client</c> already does all
/// of that, correctly, for all three transports against the exact same <c>/hubs/stream</c> the
/// engine serves (<c>MapHub</c> restricts no transport). This class only adapts that library's
/// <c>HubConnection</c> to <see cref="ITransport"/>.
///
/// Snapshot is REST here regardless of wire mode -- there is no "SSE version" of <c>GET /rows</c>.
/// </summary>
internal sealed class SignalRTransport : ITransport
{
    public string Name { get; }

    private readonly string _baseUrl;
    private readonly Func<CancellationToken, ValueTask<string>> _tokenProvider;
    private readonly HttpTransportType _transportType;
    private readonly AuthHttpClient _http;

    public SignalRTransport(string baseUrl, Func<CancellationToken, ValueTask<string>> tokenProvider, HttpTransportType transportType, AuthHttpClient http)
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _tokenProvider = tokenProvider;
        _transportType = transportType;
        _http = http;
        Name = transportType switch
        {
            HttpTransportType.WebSockets => "signalr:ws",
            HttpTransportType.ServerSentEvents => "signalr:sse",
            HttpTransportType.LongPolling => "signalr:lp",
            _ => "signalr",
        };
    }

    private HubConnection BuildConnection() =>
        new HubConnectionBuilder()
            .WithUrl($"{_baseUrl}/hubs/stream", options =>
            {
                options.Transports = _transportType;
                // The engine's JWT guard is keyed on the /hubs path prefix; SignalR's own client
                // attaches this as a query-string ?access_token= for ws/sse (those can't send
                // headers) and as an Authorization header for the negotiate/long-poll requests --
                // handled entirely inside HttpConnectionOptions, nothing to special-case here.
                options.AccessTokenProvider = async () => await _tokenProvider(CancellationToken.None).ConfigureAwait(false);
            })
            .Build();

    /// <summary>Opens and immediately tears down a connection -- used by <see cref="StreamForgeClient.ConnectAsync"/>'s
    /// Auto-mode probing to test whether this specific wire mode actually works before committing
    /// to it.</summary>
    public async Task ProbeAsync(CancellationToken ct)
    {
        var connection = BuildConnection();
        try
        {
            await connection.StartAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async Task<(IReadOnlyList<RowDelta> Rows, long Seq)> SnapshotAsync(string table, int limit, CancellationToken ct)
    {
        var id = await ResolveTableIdAsync(table, ct).ConfigureAwait(false);
        using var resp = await _http.GetAsync($"api/tables/{id}/rows?limit={limit}", ct).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false));
        var rows = doc.RootElement.GetProperty("rows").EnumerateArray()
            .Select(r => new RowDelta(RowCodec.FromJson(r.GetProperty("row")), r.GetProperty("weight").GetInt64()))
            .ToList();
        var seq = doc.RootElement.TryGetProperty("seq", out var seqEl) ? seqEl.GetInt64() : 0;
        return (rows, seq);
    }

    private async Task<string> ResolveTableIdAsync(string name, CancellationToken ct)
    {
        using var resp = await _http.GetAsync("api/tables", ct).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false));
        foreach (var t in doc.RootElement.EnumerateArray())
        {
            if (t.GetProperty("name").GetString() == name) return t.GetProperty("id").GetString()!;
        }
        throw new StreamForgeException($"no such table '{name}'");
    }

    /// <summary>Establishes the connection AND the server-acknowledged <c>SubscribeTable</c>
    /// invocation before returning -- see <see cref="ITransport.SubscribeAsync"/>'s doc for why
    /// this must not be deferred into a lazy iterator. This one is a HARD guarantee, not a
    /// best-effort one: <c>StreamHub.SubscribeTable</c> (shared/StreamForge.Api/Hubs/StreamHub.cs)
    /// is <c>Groups.AddToGroupAsync(...)</c>, synchronously part of handling the invocation, so by
    /// the time <c>InvokeAsync</c> completes the connection is provably in the group that receives
    /// <c>tableDelta</c> broadcasts -- no race window remains once this method returns.</summary>
    public async Task<IAsyncEnumerable<(IReadOnlyList<RowDelta> Deltas, long Seq)>> SubscribeAsync(string table, CancellationToken ct)
    {
        var connection = BuildConnection();
        // Unbounded: HubConnection's own "on" callback runs synchronously on its receive loop, so
        // this channel is purely a handoff to the async-enumerable consumer -- it is never allowed
        // to apply backpressure to the hub's receive loop itself (that loop has no way to slow the
        // server down; SignalR relays deltas, it does not flow-control them the way gRPC's bidi
        // ingest stream does).
        var channel = Channel.CreateUnbounded<(IReadOnlyList<RowDelta> Deltas, long Seq)>();

        // Registered BEFORE StartAsync/InvokeAsync below, deliberately: if this were registered
        // after the SubscribeTable invocation was sent, a tableDelta broadcast could arrive on the
        // wire in the gap and have nowhere to land.
        connection.On<string, List<JsonElement>, long>("tableDelta", (name, deltas, seq) =>
        {
            if (name != table) return;
            var converted = deltas
                .Select(d => new RowDelta(RowCodec.FromJson(d.GetProperty("row")), d.GetProperty("weight").GetInt64()))
                .ToList();
            channel.Writer.TryWrite((converted, seq));
        });

        connection.Closed += ex =>
        {
            channel.Writer.TryComplete(ex);
            return Task.CompletedTask;
        };

        try
        {
            await connection.StartAsync(ct).ConfigureAwait(false);
            await connection.InvokeAsync("SubscribeTable", table, ct).ConfigureAwait(false);
        }
        catch
        {
            // Establishment failed: nothing owns this connection yet (no consumer/iterator exists
            // to reach the disposal in Consume()'s finally below), so dispose it here rather than
            // leak a half-open connection.
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return Consume(connection, channel, ct);
    }

    /// <summary>Purely mechanical reads from an already-live subscription -- laziness here is
    /// harmless, unlike in the establishment phase above, because nothing this iterator's body
    /// does needs to happen before a caller proceeds past <see cref="SubscribeAsync"/>.</summary>
    private static async IAsyncEnumerable<(IReadOnlyList<RowDelta> Deltas, long Seq)> Consume(
        HubConnection connection,
        Channel<(IReadOnlyList<RowDelta> Deltas, long Seq)> channel,
        [EnumeratorCancellation] CancellationToken ct)
    {
        try
        {
            await foreach (var item in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                yield return item;
        }
        finally
        {
            channel.Writer.TryComplete();
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}

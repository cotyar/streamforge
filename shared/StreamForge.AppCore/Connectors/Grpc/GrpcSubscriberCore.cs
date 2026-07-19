using System.Text;
using System.Text.Json;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Reflection.V1Alpha;
using StreamForge.Abstractions;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.AppCore.Connectors.Grpc;

/// <summary>
/// The reconnecting subscribe loop behind a <c>grpc</c>-kind source (plan 006 D-G, federation): dials a
/// remote StreamForge instance's <c>DynamicStreamService/SubscribeEntity</c>, decodes each
/// <c>DynamicFrame</c> with <see cref="ProtoWireDecoder"/> against a schema obtained once per
/// (re)connect (snapshot-schema semantics - a schema edit on the remote made mid-subscription isn't
/// picked up until the next reconnect), and hands decoded rows to <paramref name="onRows"/>-shaped
/// callbacks supplied by the runtime driver (Orleans <c>ConnectorGrain</c> / Dapr <c>ConnectorActor</c>,
/// wave 3 - this class has no Orleans/Dapr/ASP.NET dependency itself).
///
/// <para><b>No generated client code</b>: <c>DynamicStreamService</c> has no client-side codegen in this
/// solution (the .proto lives server-side only, see <c>Protos/streamforge_dynamic.proto</c>), so the
/// request/response messages are hand-rolled tiny POCOs serialized directly against the fixed wire
/// layout <see cref="ProtoFileBuilder"/> documents for every download (<c>EntitySubscribeRequest{string
/// entity_key=1}</c>, <c>DynamicFrame{string entity_key=1; bytes payload=2; int64 seq=3}</c>) and wired
/// up via a hand-built <see cref="Method{TRequest,TResponse}"/> + <see cref="Marshaller{T}"/> over a raw
/// <see cref="CallInvoker"/> - the same pattern protoc-generated client stubs use internally, just
/// written by hand instead of generated.</para>
///
/// <para><b>Auth</b>: <see cref="GrpcSubConfig.Token"/>, when set, is used directly as a static bearer
/// token for every call (no re-login is possible with it - documented on the config type itself).
/// Otherwise, when <see cref="GrpcSubConfig.Username"/> is set, this logs in via
/// <c>POST {RestAddress}/api/auth/login</c> fresh on every (re)connect (mirroring the schema
/// snapshot-per-reconnect rule) and uses the returned token; <see cref="GrpcSubConfig.RestAddress"/>
/// MUST be set for this - there is deliberately no guessing of a REST port from the gRPC
/// <see cref="GrpcSubConfig.Address"/> (they need not be related at all), so a missing RestAddress
/// surfaces as a status "error" (retried forever at the usual backoff, since the config can't fix
/// itself, but the operator can update it and the connector will pick it up next attempt) rather than
/// silently guessing wrong. With neither Token nor Username set, calls carry no Authorization header at
/// all - works only against a remote that allows anonymous Viewer access.</para>
///
/// <para><b>Reconnection</b>: forever, honoring <paramref name="ct"/> at every await. Backoff delay is
/// computed LOCALLY with the exact D-E formula (<c>min(30s * 2^(k-1), 15 min)</c>) rather than via
/// <c>StreamForge.AppCore.Connectors.Scheduling.BackoffPolicy</c> (a concurrent wave-2 agent's file) to
/// keep this file's ownership self-contained during the parallel wave; the formula is pinned in the plan
/// so both copies are guaranteed to agree. A clean stream end (the remote closed the call without an
/// error - e.g. entity deleted) reconnects immediately with no backoff and resets the failure counter,
/// same as a successful poll cycle would for a scheduled connector. An UNAUTHENTICATED response gets
/// exactly one immediate re-login-and-retry (when login is in play) before falling through to the normal
/// backoff path on a second failure, per D-G's "JWT expiry on long gRPC subscriptions" risk mitigation.</para>
/// </summary>
public sealed class GrpcSubscriberCore
{
    private static readonly HttpClient SharedHttp = new();

    private readonly GrpcSubConfig _config;
    private readonly Func<IReadOnlyList<Dictionary<string, object?>>, long, Task> _onRows;
    private readonly Action<string, string?> _onStatus;

    public GrpcSubscriberCore(
        GrpcSubConfig config,
        Func<IReadOnlyList<Dictionary<string, object?>>, long, Task> onRows,
        Action<string, string?> onStatus)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _onRows = onRows ?? throw new ArgumentNullException(nameof(onRows));
        _onStatus = onStatus ?? throw new ArgumentNullException(nameof(onStatus));
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var needsLogin = string.IsNullOrEmpty(_config.Token) && !string.IsNullOrEmpty(_config.Username);
        var staticToken = string.IsNullOrEmpty(_config.Token) ? null : _config.Token;
        var failures = 0;

        while (!ct.IsCancellationRequested)
        {
            _onStatus("connecting", null);
            try
            {
                var token = staticToken;
                if (needsLogin)
                {
                    token = await LoginAsync(ct).ConfigureAwait(false); // fresh login every (re)connect
                }

                var (fields, numbers) = await FetchSchemaAsync(ct).ConfigureAwait(false);

                for (var attempt = 0; attempt < 2; attempt++)
                {
                    try
                    {
                        _onStatus("ok", null);
                        await SubscribeAndPumpAsync(fields, numbers, token, ct).ConfigureAwait(false);
                        break; // clean end of stream (or cancellation, checked below)
                    }
                    catch (RpcException rpc) when (rpc.StatusCode == StatusCode.Unauthenticated && needsLogin && attempt == 0)
                    {
                        token = await LoginAsync(ct).ConfigureAwait(false); // immediate re-login once, then retry
                    }
                }

                if (ct.IsCancellationRequested)
                {
                    return;
                }

                failures = 0; // reached here without throwing -> clean disconnect, reconnect with no backoff
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                failures++;
                _onStatus("error", $"{ex.GetType().Name}: {ex.Message}");

                try
                {
                    await Task.Delay(ComputeBackoffDelay(failures), ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Backoff (D-E, computed locally - see type doc)
    // ------------------------------------------------------------------

    private static readonly TimeSpan BaseDelay = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromMinutes(15);

    private static TimeSpan ComputeBackoffDelay(int consecutiveFailures)
    {
        if (consecutiveFailures <= 0)
        {
            return TimeSpan.Zero;
        }
        var exponent = Math.Min(consecutiveFailures - 1, 30); // clamp well before double overflow
        var scaledMs = BaseDelay.TotalMilliseconds * Math.Pow(2, exponent);
        return TimeSpan.FromMilliseconds(Math.Min(scaledMs, MaxDelay.TotalMilliseconds));
    }

    // ------------------------------------------------------------------
    // Login
    // ------------------------------------------------------------------

    private async Task<string> LoginAsync(CancellationToken ct)
    {
        if (string.IsNullOrEmpty(_config.RestAddress))
        {
            throw new InvalidOperationException(
                "GrpcSubConfig.Username is set but RestAddress is null - set RestAddress to the remote's " +
                "REST base (e.g. \"http://localhost:5199\") so GrpcSubscriberCore knows where to POST " +
                "/api/auth/login; it will not guess a REST port from the gRPC Address.");
        }

        var baseUrl = _config.RestAddress.TrimEnd('/');
        var payload = JsonSerializer.Serialize(new { username = _config.Username, password = _config.Password });
        using var content = new StringContent(payload, Encoding.UTF8, "application/json");
        using var response = await SharedHttp.PostAsync($"{baseUrl}/api/auth/login", content, ct).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        var body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        using var doc = JsonDocument.Parse(body);
        if (!doc.RootElement.TryGetProperty("token", out var tokenProp) || tokenProp.GetString() is not { Length: > 0 } token)
        {
            throw new InvalidOperationException($"Login response from '{baseUrl}/api/auth/login' did not contain a non-empty 'token' field.");
        }
        return token;
    }

    // ------------------------------------------------------------------
    // Schema acquisition
    // ------------------------------------------------------------------

    private async Task<(List<FieldDef> Fields, FieldNumberMap Numbers)> FetchSchemaAsync(CancellationToken ct)
    {
        if (string.Equals(_config.SchemaSource, "proto", StringComparison.OrdinalIgnoreCase))
        {
            var (fields, numbers, diagnostics) = ProtoTextSchemaParser.Parse(_config.ProtoText ?? "");
            if (fields is null || numbers is null)
            {
                throw new InvalidOperationException($"Failed to parse GrpcSubConfig.ProtoText: {string.Join("; ", diagnostics)}");
            }
            return (fields, numbers);
        }

        return await FetchViaReflectionAsync(ct).ConfigureAwait(false);
    }

    private async Task<(List<FieldDef> Fields, FieldNumberMap Numbers)> FetchViaReflectionAsync(CancellationToken ct)
    {
        using var channel = CreateChannel(_config.Address);
        var client = new ServerReflection.ServerReflectionClient(channel);
        using var call = client.ServerReflectionInfo(cancellationToken: ct);

        var (_, ident) = ParseEntityKey(_config.EntityKey);
        var messageSymbol = $"{DescriptorFactory.PackageName}.{DescriptorFactory.ToPascalCase(ident)}";

        await call.RequestStream.WriteAsync(new ServerReflectionRequest { Host = "", FileContainingSymbol = messageSymbol }).ConfigureAwait(false);
        if (!await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
        {
            throw new InvalidOperationException($"No reflection response received for symbol '{messageSymbol}'.");
        }
        var response = call.ResponseStream.Current;
        await call.RequestStream.CompleteAsync().ConfigureAwait(false);

        if (response.ErrorResponse is not null)
        {
            throw new InvalidOperationException(
                $"Reflection FileContainingSymbol('{messageSymbol}') failed: {response.ErrorResponse.ErrorMessage}");
        }

        var files = response.FileDescriptorResponse.FileDescriptorProto
            .Select(bytes => FileDescriptorProto.Parser.ParseFrom(bytes))
            .ToList();

        var (fields, numbers, diagnostics) = ReflectionSchemaWalker.FromDescriptors(files, _config.EntityKey);
        if (fields is null || numbers is null)
        {
            throw new InvalidOperationException($"Failed to resolve schema via reflection for '{_config.EntityKey}': {string.Join("; ", diagnostics)}");
        }
        return (fields, numbers);
    }

    // ------------------------------------------------------------------
    // One-shot schema fetch (plan 006 W4 — POST /api/sources/schema/from-remote). NOT the
    // reconnecting subscribe loop above: a single attempt, never throws (every failure — bad
    // entity key, dial failure, reflection error, malformed proto text — becomes a diagnostic so
    // the endpoint can answer 200-with-diagnostics rather than 5xx, matching the from-remote
    // contract). Additive: does not alter any existing member's behavior.
    // ------------------------------------------------------------------

    /// <summary>Fetches <c>(fields, numbers)</c> for <paramref name="config"/> once — the "proto"
    /// <see cref="GrpcSubConfig.SchemaSource"/> just parses <see cref="GrpcSubConfig.ProtoText"/>
    /// (already inline, no network); "reflection" dials the remote's v1alpha reflection service.
    /// When <see cref="GrpcSubConfig.Username"/> is set, this logs in first (mirroring
    /// <see cref="RunAsync"/>'s connect sequence, D-G parity) purely to surface a bad
    /// Username/Password/RestAddress as an early diagnostic — the resulting token is NOT attached
    /// to the reflection call itself, since this codebase's reflection surface is anonymous
    /// (<c>DynamicReflectionService</c> is <c>[AllowAnonymous]</c>), same as the reconnect loop
    /// above. Returns (null, null, diagnostics) on any failure — never throws.</summary>
    public static async Task<(List<FieldDef>? Fields, FieldNumberMap? Numbers, IReadOnlyList<string> Diagnostics)>
        FetchSchemaOnceAsync(GrpcSubConfig config, CancellationToken ct)
    {
        if (string.Equals(config.SchemaSource, "proto", StringComparison.OrdinalIgnoreCase))
        {
            var (protoFields, protoNumbers, protoDiagnostics) = ProtoTextSchemaParser.Parse(config.ProtoText ?? "");
            return (protoFields, protoNumbers, protoDiagnostics);
        }

        try
        {
            if (string.IsNullOrEmpty(config.Token) && !string.IsNullOrEmpty(config.Username))
            {
                var probe = new GrpcSubscriberCore(config, static (_, _) => Task.CompletedTask, static (_, _) => { });
                await probe.LoginAsync(ct).ConfigureAwait(false);
            }

            using var channel = CreateChannel(config.Address);
            var client = new ServerReflection.ServerReflectionClient(channel);
            using var call = client.ServerReflectionInfo(cancellationToken: ct);

            var (_, ident) = ParseEntityKey(config.EntityKey);
            var messageSymbol = $"{DescriptorFactory.PackageName}.{DescriptorFactory.ToPascalCase(ident)}";

            await call.RequestStream.WriteAsync(new ServerReflectionRequest { Host = "", FileContainingSymbol = messageSymbol }).ConfigureAwait(false);
            if (!await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
            {
                return (null, null, [$"No reflection response received for symbol '{messageSymbol}'."]);
            }
            var response = call.ResponseStream.Current;
            await call.RequestStream.CompleteAsync().ConfigureAwait(false);

            if (response.ErrorResponse is not null)
            {
                return (null, null, [$"Reflection FileContainingSymbol('{messageSymbol}') failed: {response.ErrorResponse.ErrorMessage}"]);
            }

            var files = response.FileDescriptorResponse.FileDescriptorProto
                .Select(bytes => FileDescriptorProto.Parser.ParseFrom(bytes))
                .ToList();

            return ReflectionSchemaWalker.FromDescriptors(files, config.EntityKey);
        }
        catch (Exception ex)
        {
            return (null, null, [$"{ex.GetType().Name}: {ex.Message}"]);
        }
    }

    private static (string Kind, string Ident) ParseEntityKey(string entityKey)
    {
        var idx = entityKey.IndexOf(':');
        if (idx <= 0 || idx == entityKey.Length - 1)
        {
            throw new InvalidOperationException($"Malformed GrpcSubConfig.EntityKey '{entityKey}' (expected 'kind:ident').");
        }
        return (entityKey[..idx], entityKey[(idx + 1)..]);
    }

    // ------------------------------------------------------------------
    // Subscribe + pump
    // ------------------------------------------------------------------

    private async Task SubscribeAndPumpAsync(List<FieldDef> fields, FieldNumberMap numbers, string? token, CancellationToken ct)
    {
        using var channel = CreateChannel(_config.Address);
        var invoker = channel.CreateCallInvoker();

        var method = new Method<SubscribeRequestMsg, FrameMsg>(
            MethodType.ServerStreaming,
            "streamforge.dynamic.v1.DynamicStreamService",
            "SubscribeEntity",
            RequestMarshaller,
            FrameMarshaller);

        var headers = new Metadata();
        if (!string.IsNullOrEmpty(token))
        {
            headers.Add("Authorization", $"Bearer {token}");
        }

        using var call = invoker.AsyncServerStreamingCall(
            method, null, new CallOptions(headers: headers, cancellationToken: ct), new SubscribeRequestMsg { EntityKey = _config.EntityKey });

        // A table source has table-delta (insert-only) semantics per D-G: negative/zero-weight deltas
        // (retractions) are dropped, never surfaced - a *source* can only ever grow.
        var isTable = _config.EntityKey.StartsWith("table:", StringComparison.Ordinal);

        while (await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
        {
            var frame = call.ResponseStream.Current;

            Dictionary<string, object?> row;
            long seq;
            if (isTable)
            {
                var (r, weight, s) = ProtoWireDecoder.DecodeDelta(fields, numbers, frame.Payload);
                if (weight <= 0)
                {
                    continue;
                }
                row = r;
                seq = s;
            }
            else
            {
                var (r, s, _) = ProtoWireDecoder.DecodeEvent(fields, numbers, frame.Payload);
                row = r;
                seq = s;
            }

            await _onRows([row], seq).ConfigureAwait(false);
        }
    }

    private static GrpcChannel CreateChannel(string address)
    {
        // StreamForge's gRPC surface is h2c (plaintext HTTP/2) - see plan/ARCHITECTURE port notes.
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        return GrpcChannel.ForAddress(address);
    }

    // ------------------------------------------------------------------
    // Hand-rolled request/response messages + marshallers (no client codegen exists for
    // DynamicStreamService - see type doc). Wire layout mirrors ProtoFileBuilder's fixed
    // "streaming contract" block exactly: EntitySubscribeRequest{string entity_key=1},
    // DynamicFrame{string entity_key=1; bytes payload=2; int64 seq=3}.
    // ------------------------------------------------------------------

    private sealed class SubscribeRequestMsg
    {
        public string EntityKey = "";
    }

    private sealed class FrameMsg
    {
        public string EntityKey = "";
        public byte[] Payload = [];
        public long Seq;
    }

    private static readonly Marshaller<SubscribeRequestMsg> RequestMarshaller =
        Marshallers.Create<SubscribeRequestMsg>(SerializeRequest, DeserializeRequestUnused);

    private static readonly Marshaller<FrameMsg> FrameMarshaller =
        Marshallers.Create<FrameMsg>(SerializeFrameUnused, DeserializeFrame);

    private static byte[] SerializeRequest(SubscribeRequestMsg msg)
    {
        using var ms = new MemoryStream();
        using (var output = new CodedOutputStream(ms, leaveOpen: true))
        {
            if (!string.IsNullOrEmpty(msg.EntityKey))
            {
                output.WriteTag(1, WireFormat.WireType.LengthDelimited);
                output.WriteString(msg.EntityKey);
            }
            output.Flush();
        }
        return ms.ToArray();
    }

    private static SubscribeRequestMsg DeserializeRequestUnused(byte[] _) =>
        throw new NotSupportedException("EntitySubscribeRequest is client-to-server only - this client never deserializes it.");

    private static byte[] SerializeFrameUnused(FrameMsg _) =>
        throw new NotSupportedException("DynamicFrame is server-to-client only - this client never serializes it.");

    private static FrameMsg DeserializeFrame(byte[] data)
    {
        var frame = new FrameMsg();
        var input = new CodedInputStream(data);
        uint tag;
        while ((tag = input.ReadTag()) != 0)
        {
            switch (WireFormat.GetTagFieldNumber(tag))
            {
                case 1:
                    frame.EntityKey = input.ReadString();
                    break;
                case 2:
                    frame.Payload = input.ReadBytes().ToByteArray();
                    break;
                case 3:
                    frame.Seq = input.ReadInt64();
                    break;
                default:
                    input.SkipLastField();
                    break;
            }
        }
        return frame;
    }
}

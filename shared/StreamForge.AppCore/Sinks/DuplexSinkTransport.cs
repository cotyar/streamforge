using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.AppCore.Sinks;

/// <summary>
/// Plan 019 D2 (wave 019-B): the proxy sink — the outbound half of a duplex session
/// (<see cref="SourceKinds.Fix"/> order entry is the first duplex kind, wave 019-E), reached by NAME rather
/// than by connection. <see cref="Create"/> hands back a <see cref="DuplexSinkClient"/> that holds nothing:
/// every publish resolves <see cref="DuplexSinkConfig.SourceName"/>'s live session through
/// <see cref="DuplexSessions.Find"/> and forwards to it. That is what makes the 30-second
/// <c>SinkSelection.Signature</c> teardown/rebuild cycle harmless for this kind specifically — tearing down
/// a client that owns no connection costs nothing, where tearing down a live FIX session on an unrelated
/// sink field edit would re-logon an order session mid-flight (plan 019's own motivating example).
///
/// <para><b>The validation question — closed by wave 019-B2, in <see cref="DuplexSinkCatalogValidation"/>
/// below, without widening anything.</b> Plan 019 D2 requires "a duplex sink whose named source does not
/// exist, or is not a duplex kind" to be a validation-time error. <see cref="Validate"/> below covers
/// everything that needs NO catalog: the config is present, <see cref="DuplexSinkConfig.SourceName"/> is
/// non-blank. It CANNOT cover "does a source named that actually exist" or "is it a duplex kind" —
/// <see cref="ISinkTransport.Validate"/>'s signature is <c>(SinkSpec, List&lt;string&gt;)</c>, with no
/// catalog reference to ask, and <see cref="SinkTransports.Validate"/> (the orchestrator that calls every
/// transport's <see cref="Validate"/>) has the identical shape for the identical reason — see that method's
/// own doc comment. <b>Neither signature was widened</b> — the check instead lives in
/// <see cref="DuplexSinkCatalogValidation.ValidateAsync"/>, called directly by
/// <c>PipelinesEndpoints.cs</c> and <c>TablesEndpoints.cs</c> (both already hold an
/// <c>ICatalogFacade registry</c> in scope at the same call sites that invoke
/// <see cref="SinkTransports.Validate"/>), right after that call, into the same error list.</para>
/// </summary>
public sealed class DuplexSinkTransport : ISinkTransport
{
    public string Kind => SinkKinds.Duplex;

    public bool IsConfigured(SinkSpec spec) =>
        spec.Duplex is { } d && !string.IsNullOrWhiteSpace(d.SourceName);

    public ISinkClient Create(SinkSpec spec, string entityKind, string entityName, Action<string, Exception>? onFailure) =>
        new DuplexSinkClient(spec.Duplex!, entityKind, entityName, onFailure);

    /// <summary>Everything checkable with no catalog reference — see this class's doc comment for the part
    /// that genuinely needs one, and where it belongs instead.</summary>
    public void Validate(SinkSpec spec, List<string> errors)
    {
        if (spec.Duplex is not { } cfg)
        {
            errors.Add($"kind '{SinkKinds.Duplex}' requires duplex config");
            return;
        }

        if (string.IsNullOrWhiteSpace(cfg.SourceName))
        {
            errors.Add("duplex.sourceName is required");
        }
    }

    // NOT Duplex = true, despite this kind existing entirely because of duplex sessions: that flag's own
    // doc comment (TransportDescriptor.cs) defines it as "this kind implements IDuplexTransport" — true
    // for an INBOUND kind like fix (wave 019-E), never true for this SINK, which implements ISinkTransport
    // and forwards to that kind's session rather than being one. DuplexTransportRegistryTests (wave A,
    // frozen) pins the flag as false for every OTHER registered descriptor across BOTH catalogs; leaving
    // it at its default here is what keeps that test green rather than contradicting it.
    public TransportDescriptor Describe() => new()
    {
        Kind = SinkKinds.Duplex,
        Version = "1.0.0", // plan 016 wave 4: explicit contract version — see TransportDescriptor.Version.
        Label = "Duplex session (proxy)",
        Help = "No connection of its own: forwards to the live session already opened by the named source (e.g. a fix source). Requires that source to be a duplex kind and currently started.",
        ConfigProperty = "duplex",
        Fields =
        [
            new TransportField
            {
                Key = "sourceName", Label = "Source", Required = true, Mono = true, Placeholder = "{name}",
                Help = "{name} is replaced with this pipeline's id / table's name, so one spec can serve a whole catalog. Must name a duplex-kind source (e.g. fix) that is currently started.",
            },
            new TransportField
            {
                Key = "requireSession", Label = "Require session to start", Type = TransportFieldTypes.Bool,
                Help = "When on, a pipeline/table using this sink refuses to start while the named source's session is down, instead of running with every send counted as a failure. (Plan 019 D3 — the start-time refusal itself is a later wave; this field only carries the setting.)",
            },
        ],
    };
}

/// <summary>
/// Plan 019 D2/D3: the outbound connection for ONE configured duplex sink — except there is no connection.
/// <see cref="PublishBatchAsync{T}"/> resolves <see cref="SourceName"/>'s live session by NAME, every call,
/// through <see cref="DuplexSessions.Find"/>, and forwards the whole batch as ONE
/// <see cref="IDuplexSession.SendAsync"/> call — a duplex session's entire value is that a batch is one
/// ordered handoff (see <see cref="IBatchSinkClient"/>'s own doc for why there is no time-based buffering to
/// undermine that), so this class implements <see cref="IBatchSinkClient"/> rather than leaning on
/// <see cref="SinkFanout"/>'s per-message fallback.
///
/// <para><b>Fire-and-forget contract, identical to every other <see cref="ISinkClient"/>, held to a real
/// wall-clock ceiling rather than trusting the session to honor cancellation.</b> <see cref="PublishTimeout"/>
/// (3s, the same value <see cref="NatsSinkClient.PublishTimeout"/> uses) is enforced with
/// <see cref="Task.WhenAny(Task,Task)"/> racing <see cref="IDuplexSession.SendAsync"/> against a plain
/// <see cref="Task.Delay(TimeSpan)"/> — NOT a linked, cancelled <see cref="CancellationTokenSource"/> alone,
/// because <see cref="IDuplexSession.SendAsync"/>'s only documented obligation on cancellation is "may throw
/// when the session itself is dead"; a session that never observes <c>ct</c> at all (buggy, or simply slow)
/// must still not be able to hang this call past budget. The abandoned send, if the race is lost, is left to
/// run to completion on its own — there is no way to un-send it, and awaiting it further here would be the
/// exact hang this timeout exists to prevent.</para>
///
/// <para><b>A missing session is a stated, counted failure — never a silent success.</b>
/// <see cref="DuplexSessions.Find"/> returns null when the named source is not running, is not a duplex
/// kind, or (see that class's own ponytail note) is held by another process. Every one of those counts here
/// exactly like a partial <see cref="DuplexSendOutcome"/> does: <see cref="Counters"/>'s <c>Failed</c>
/// increments by the WHOLE batch, <see cref="SinkPublishCounters.LastError"/> names the reason, and the
/// throttled <c>onFailure</c> callback fires — nothing about "the source isn't up right now" is allowed to
/// look like a delivered batch.</para>
///
/// <para><b><see cref="IDuplexSession.SendAsync"/> "MUST NOT throw for an ordinary delivery failure" is the
/// session's contract, not this class's</b> — but this class does not get to assume every session honors it
/// forever, so a thrown exception (the one case the session interface DOES allow — "the session itself is
/// dead") is caught here exactly like every other failure mode, never propagated. <see cref="ISinkClient.PublishAsync{T}"/>'s
/// own never-throw contract holds regardless of which of these three ways a send failed.</para>
/// </summary>
public sealed class DuplexSinkClient : ISinkClient, IBatchSinkClient
{
    /// <summary>Upper bound on ONE <see cref="PublishBatchAsync{T}"/> call, including however long the
    /// resolved session's own <see cref="IDuplexSession.SendAsync"/> takes — see this class's doc comment
    /// for why this is enforced as a real race rather than trusting <c>ct</c> alone. Same value as
    /// <see cref="NatsSinkClient.PublishTimeout"/>.</summary>
    public static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(3);

    /// <summary>Minimum gap between two <c>onFailure</c> invocations for the SAME client — identical value
    /// and reason to <see cref="NatsSinkClient.LogThrottleWindow"/>.</summary>
    public static readonly TimeSpan LogThrottleWindow = TimeSpan.FromSeconds(30);

    private readonly string _sourceName;
    private readonly Action<string, Exception>? _onFailure;

    private long _published;
    private long _failed;
    private string? _lastError;
    private long _lastFailureAtMs;
    private long _lastLoggedAtMs;

    /// <param name="config">Non-null <c>SinkSpec.Duplex</c> — the caller filters for that before
    /// constructing a client, same convention as every other <see cref="ISinkClient"/> in this file.</param>
    /// <param name="entityKind">"pipeline" | "table" — carried for log/failure-callback context only.</param>
    /// <param name="entityName">Pipeline id or table name; also what <c>{name}</c> in
    /// <see cref="DuplexSinkConfig.SourceName"/> expands to.</param>
    /// <param name="onFailure">Invoked (throttled) with (sourceName, exception) on a missing session, a
    /// partial/whole send rejection, or the publish budget expiring. Never invoked on a fully-accepted send.
    /// May be null.</param>
    public DuplexSinkClient(
        DuplexSinkConfig config, string entityKind, string entityName, Action<string, Exception>? onFailure = null)
    {
        ArgumentNullException.ThrowIfNull(config);
        _sourceName = config.SourceName.Replace("{name}", entityName, StringComparison.Ordinal);
        _onFailure = onFailure;
        EntityName = entityName;
        EntityKind = entityKind;
    }

    /// <summary>The expanded target source name — <c>{name}</c> already substituted. What
    /// <see cref="DuplexSessions.Find"/> is called with on every publish.</summary>
    public string SourceName => _sourceName;

    public string EntityName { get; }

    /// <summary>"pipeline" | "table".</summary>
    public string EntityKind { get; }

    public SinkPublishCounters Counters => new(
        Interlocked.Read(ref _published),
        Interlocked.Read(ref _failed),
        Volatile.Read(ref _lastError),
        Interlocked.Read(ref _lastFailureAtMs));

    /// <summary>A batch of one — see this class's doc comment for why every real caller
    /// (<see cref="SinkFanout"/>) reaches <see cref="PublishBatchAsync{T}"/> directly instead.</summary>
    public Task PublishAsync<T>(T payload, CancellationToken ct) => PublishBatchAsync([payload], ct);

    /// <summary>Forwards <paramref name="payloads"/> to <see cref="SourceName"/>'s live session as ONE
    /// <see cref="IDuplexSession.SendAsync"/> call. NEVER throws — see this class's doc comment for the
    /// three failure modes (missing session, session-reported partial/whole rejection, budget expiry) and
    /// how each is counted.</summary>
    public async Task PublishBatchAsync<T>(IReadOnlyList<T> payloads, CancellationToken ct)
    {
        if (payloads.Count == 0)
        {
            return;
        }

        if (ct.IsCancellationRequested)
        {
            // Caller (host shutdown, sink config changed mid-publish) is tearing this client down — not a
            // session problem, so not counted as a failure. Mirrors NatsSinkClient's identical guard.
            return;
        }

        var session = DuplexSessions.Find(_sourceName);
        if (session is null)
        {
            FailAll(payloads.Count, new InvalidOperationException(
                $"duplex sink target source '{_sourceName}' has no live session (not started, not a duplex kind, or held by another process)"));
            return;
        }

        var rows = payloads.Select(SinkStepGuard.RowOf).ToList();

        Task<DuplexSendOutcome> sendTask;
        try
        {
            sendTask = session.SendAsync(rows, ct);
        }
        catch (Exception ex)
        {
            // IDuplexSession.SendAsync's own doc: may throw ONLY when the session itself is dead. That is
            // still an ordinary delivery failure from THIS class's point of view — ISinkClient.PublishAsync
            // never throws regardless of which of the session's two outcomes (thrown vs. reported) produced
            // the failure.
            FailAll(payloads.Count, ex);
            return;
        }

        var completed = await Task.WhenAny(sendTask, Task.Delay(PublishTimeout)).ConfigureAwait(false);

        if (ct.IsCancellationRequested)
        {
            // Shutdown raced the send — same non-failure treatment as the guard above, checked again here
            // because the wait itself is where that race is actually observed.
            return;
        }

        if (!ReferenceEquals(completed, sendTask))
        {
            // The session did not return within budget. sendTask is abandoned, not awaited further — there
            // is no way to un-send it, and waiting on it here would be exactly the hang this timeout exists
            // to prevent (see this class's doc comment).
            FailAll(payloads.Count, new TimeoutException(
                $"duplex session '{_sourceName}' did not respond within {PublishTimeout}"));
            return;
        }

        DuplexSendOutcome outcome;
        try
        {
            outcome = await sendTask.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            FailAll(payloads.Count, ex);
            return;
        }

        if (outcome.Sent > 0)
        {
            Interlocked.Add(ref _published, outcome.Sent);
        }

        if (outcome.Failed > 0)
        {
            var reason = outcome.Failures.Count > 0
                ? string.Join("; ", outcome.Failures.Take(3)
                    .Select(f => f.CorrelationId is null ? f.Reason : $"{f.CorrelationId}: {f.Reason}"))
                : "delivery rejected";
            RecordFailure(outcome.Failed, new InvalidOperationException(
                $"duplex session '{_sourceName}' rejected {outcome.Failed} of {rows.Count} row(s): {reason}"));
        }
    }

    private void FailAll(int count, Exception ex) => RecordFailure(count, ex);

    private void RecordFailure(int count, Exception ex)
    {
        Interlocked.Add(ref _failed, count);
        Volatile.Write(ref _lastError, $"{ex.GetType().Name}: {ex.Message}");
        Interlocked.Exchange(ref _lastFailureAtMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        if (_onFailure is null)
        {
            return;
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var last = Interlocked.Read(ref _lastLoggedAtMs);
        if (now - last < LogThrottleWindow.TotalMilliseconds)
        {
            return;
        }

        // Only the thread that wins this race actually logs — same reasoning as
        // NatsSinkClient.MaybeReportFailure/HttpSinkClient.Fail: a burst of near-simultaneous failures must
        // produce one log line, not one per row.
        if (Interlocked.CompareExchange(ref _lastLoggedAtMs, now, last) != last)
        {
            return;
        }

        _onFailure(_sourceName, ex);
    }

    /// <summary>No-op: this client owns no connection — see this class's and <see cref="DuplexSinkTransport"/>'s
    /// doc comments for why that is the entire point. The live session it forwards to is owned and disposed
    /// by the connector driver (Orleans <c>ConnectorGrain</c> / Dapr <c>ConnectorActor</c>, wave 019-C/D),
    /// not by this proxy.</summary>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Plan 019 D2 (wave 019-B2): the catalog-aware half of duplex sink validation that
/// <see cref="DuplexSinkTransport.Validate"/> cannot do itself — "a duplex sink whose named source does
/// not exist, or is not a duplex kind" is a save-time error. Deliberately NOT a widened
/// <see cref="ISinkTransport.Validate"/> or <see cref="SinkTransports.Validate"/> signature (both frozen —
/// see this class's containing file's own doc comment): this is a free-standing helper the two endpoint
/// call sites (<c>PipelinesEndpoints.cs</c>, <c>TablesEndpoints.cs</c>) invoke directly, right after
/// <see cref="SinkTransports.Validate"/>, appending into the SAME error list.
/// </summary>
public static class DuplexSinkCatalogValidation
{
    /// <summary>Checks every <see cref="SinkKinds.Duplex"/> sink in <paramref name="sinks"/> against the
    /// catalog, appending to <paramref name="errors"/> — two distinguishable messages, because "no such
    /// source" and "source X is not a duplex kind" have different fixes.
    ///
    /// <para><b><paramref name="entityName"/> and the one case it cannot cover.</b> A duplex sink's
    /// <see cref="DuplexSinkConfig.SourceName"/> may contain the SAME <c>{name}</c> template every other
    /// sink kind supports (<see cref="DuplexSinkClient"/>'s own doc: "so one spec can serve a whole
    /// catalog"), expanded here with <paramref name="entityName"/> exactly like the runtime client expands
    /// it. For a TABLE, that name is <c>TableDefinition.Name</c> — always known already, it is user-supplied
    /// and validated for uniqueness before this ever runs, so <paramref name="entityName"/> is never null
    /// from <c>TablesEndpoints.cs</c>. For a PIPELINE it is <c>PipelineDefinition.Id</c> — minted by
    /// <c>RegistryGrain.CreatePipelineAsync</c>/<c>RegistryActor</c> as a fresh <c>Guid</c> ONLY once the
    /// entity is actually created, so on <c>POST /api/pipelines</c> (before creation) there is no id yet to
    /// substitute. <paramref name="entityName"/> is null for exactly that one call site; a sink whose
    /// <c>SourceName</c> still contains the literal <c>{name}</c> token after a null-entityName no-op
    /// expansion is SKIPPED here rather than reported as "no such source '{name}'" — a false rejection of
    /// what is genuinely a template, not a typo. A literal (non-templated) source name is checked exactly
    /// the same at every call site regardless. The gap this leaves — a pipeline created with a templated
    /// duplex source that will never resolve — surfaces at runtime instead, the same loud-failure path plan
    /// 019 D3 built for every other unresolvable duplex send (<see cref="DuplexSinkClient"/>'s "missing
    /// session" branch); it is also caught on the very next <c>PUT</c>, where the id is always
    /// known.</para></summary>
    public static async Task ValidateAsync(
        IReadOnlyList<SinkSpec> sinks, string? entityName, ICatalogFacade registry, List<string> errors)
    {
        foreach (var sink in sinks)
        {
            if (sink.Kind != SinkKinds.Duplex || sink.Duplex is not { } cfg || string.IsNullOrWhiteSpace(cfg.SourceName))
            {
                // Missing/blank config is DuplexSinkTransport.Validate's job (it runs first at every call
                // site) — nothing for a catalog lookup to add.
                continue;
            }

            var resolved = entityName is null ? cfg.SourceName : cfg.SourceName.Replace("{name}", entityName, StringComparison.Ordinal);
            if (resolved.Contains("{name}", StringComparison.Ordinal))
            {
                // entityName unavailable (pipeline create, pre-id) AND the source name is genuinely
                // templated — see this method's own doc for why this is skipped rather than misreported.
                continue;
            }

            var source = await registry.GetSourceAsync(resolved);
            if (source is null)
            {
                errors.Add($"duplex sink source '{resolved}' does not exist");
            }
            else if (DuplexTransports.Find(source.Kind) is null)
            {
                errors.Add($"duplex sink source '{resolved}' (kind '{source.Kind}') is not a duplex kind");
            }
        }
    }
}

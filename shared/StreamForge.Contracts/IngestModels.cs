namespace StreamForge.Abstractions;

// ============================================================================
// Plan 008 W4 — client-push ingress.
//
// THE HONEST FRAMING, because it decides every shape below: there is no end-to-end backpressure
// available in this architecture and no design here can create one. Orleans' OnNextAsync returns
// whether or not PushStreamBus dropped the item downstream; Dapr's PublishEventAsync tells you about
// the sidecar hop, not about whether the routers keep up. What CAN be honest is admission control
// against a bounded buffer we own and can measure. So success is 202 Accepted ("buffered"), never
// 200, and IngestStatus surfaces DownstreamDropped so the second loss point is visible instead of
// being discovered later as missing rows.
// ============================================================================

/// <summary>What a source does when its ingress buffer has no room for an incoming batch. Source-level
/// setting, not per-request — a client cannot pick its own policy.</summary>
public enum IngressOverflowPolicy
{
    /// <summary>Default. Refuse the batch whole (429 + Retry-After) so the identical body can be
    /// retried. All-or-nothing: a partially-admitted batch would have no safe retry.</summary>
    Reject = 0,

    /// <summary>Wait for space, up to <see cref="IngestConfig.MaxWaitMs"/> (server-capped). On the bidi
    /// gRPC path this is the only real transport-level backpressure in the feature: the server stops
    /// reading, HTTP/2 flow control closes the window, and the client's WriteAsync genuinely blocks.
    /// A batch larger than capacity must reject immediately rather than burn the timeout.</summary>
    Block = 1,

    /// <summary>Admit what fits, discard the overflow from the incoming batch. Reported as
    /// <c>dropped</c> on the 202 — never silent.</summary>
    DropNewest = 2,

    /// <summary>Admit the batch, evicting the oldest buffered rows to make room. This retroactively
    /// invalidates earlier 202s, which is why it is documented and never the default.</summary>
    DropOldest = 3,

    /// <summary>No buffer: the request awaits the publish itself. Note this is NOT "lossless" — under
    /// Orleans push transport the publish can still drop downstream.</summary>
    Inline = 4,
}

/// <summary>Ingress settings for a <see cref="SourceKinds.Ingest"/> source.</summary>
[GenerateSerializer]
public sealed class IngestConfig
{
    [Id(0)] public IngressOverflowPolicy Policy { get; set; } = IngressOverflowPolicy.Reject;
    /// <summary>Buffer capacity in rows. Default matches Streams:PushCapacity.</summary>
    [Id(1)] public int CapacityRows { get; set; } = 10_000;
    /// <summary>Only meaningful for <see cref="IngressOverflowPolicy.Block"/>; server-capped at 30s.</summary>
    [Id(2)] public int MaxWaitMs { get; set; } = 5_000;
    /// <summary>Largest batch a single push may carry; a bigger one is 413, never a partial admit.</summary>
    [Id(3)] public int MaxBatchRows { get; set; } = 1_000;
    /// <summary>When true an undeclared field fails its row instead of being dropped-and-counted. The
    /// encoder can only write declared fields either way — this only chooses silence vs. an error.</summary>
    [Id(4)] public bool RejectUnknownFields { get; set; }
}

/// <summary>Outcome of one push. Maps 1:1 onto the REST status codes so the endpoint layer holds no
/// policy of its own.</summary>
public enum IngestOutcome
{
    /// <summary>202 — buffered (or, for Inline, published). Never 200: see this file's header.</summary>
    Accepted = 0,
    /// <summary>400 — at least one row failed coercion and <c>partial</c> was not requested.</summary>
    Invalid = 1,
    /// <summary>404 — no such source.</summary>
    NotFound = 2,
    /// <summary>409 — the source exists but is not ingest-kind. Pushing into a generator would make
    /// its timer-driven rate and every counter unreconcilable; strictness here is reversible, laxity
    /// is not.</summary>
    WrongKind = 3,
    /// <summary>413 — batch exceeds <see cref="IngestConfig.MaxBatchRows"/> or buffer capacity.</summary>
    TooLarge = 4,
    /// <summary>429 — no room under <see cref="IngressOverflowPolicy.Reject"/>, or Block timed out.</summary>
    Overloaded = 5,
}

/// <summary>Result of one push. <see cref="Accepted"/> + <see cref="Dropped"/> + <see cref="Invalid"/>
/// accounts for every row in the request.</summary>
[GenerateSerializer]
public sealed class IngestResult
{
    [Id(0)] public IngestOutcome Outcome { get; set; }
    [Id(1)] public int Accepted { get; set; }
    /// <summary>Rows discarded by DropNewest/DropOldest — always named in the response, never silent.</summary>
    [Id(2)] public int Dropped { get; set; }
    [Id(3)] public int Invalid { get; set; }
    /// <summary>Populated for <see cref="IngestOutcome.Overloaded"/>; the REST layer clamps the
    /// Retry-After header to [1,30] whole seconds from this.</summary>
    [Id(4)] public int RetryAfterMs { get; set; }
    /// <summary>Human-readable reason for any non-Accepted outcome; null when accepted.</summary>
    [Id(5)] public string? Error { get; set; }
    /// <summary>Per-row messages for the rows that failed coercion (bounded — do not echo the batch).</summary>
    [Id(6)] public List<string> RowErrors { get; set; } = [];
}

/// <summary>Backs GET /api/sources/{name}/ingest. Deliberately NOT overloaded onto /status, which is
/// the connector-runtime surface.</summary>
[GenerateSerializer]
public sealed class IngestStatus
{
    [Id(0)] public IngressOverflowPolicy Policy { get; set; }
    [Id(1)] public int CapacityRows { get; set; }
    [Id(2)] public int DepthRows { get; set; }
    [Id(3)] public int MaxBatchRows { get; set; }
    [Id(4)] public long TotalAccepted { get; set; }
    [Id(5)] public long TotalRejected { get; set; }
    [Id(6)] public long TotalDropped { get; set; }
    [Id(7)] public long TotalInvalid { get; set; }
    /// <summary>Rows the drain pump has handed to the stream/pub-sub layer.</summary>
    [Id(8)] public long TotalPublished { get; set; }
    /// <summary>The SECOND loss point, surfaced on purpose: rows the transport dropped after we
    /// published them (Orleans PushStreamBus.TotalDropped). Counted today and exposed nowhere else.</summary>
    [Id(9)] public long DownstreamDropped { get; set; }
    [Id(10)] public long LastPushMs { get; set; }
}

/// <summary>Client-push ingress (plan 008 W4). A NEW interface rather than an ICatalogFacade
/// extension — existing facade members are frozen (test fakes implement them); same reasoning as
/// <see cref="IConnectorStatusFacade"/>. The buffer lives in a host-process singleton in both
/// flavors, so implementations call it directly rather than through a grain/actor: an unbounded,
/// unobservable grain inbox with no admission point would make the policy choice decorative.</summary>
public interface IIngressFacade
{
    /// <summary>Coerce, admit, and (for Inline) publish one batch. Coercion happens BEFORE admission so
    /// a 400 never leaves partial state. <paramref name="partial"/> admits the valid rows of a batch
    /// that has invalid ones instead of failing the whole batch.</summary>
    Task<IngestResult> PushAsync(string sourceName, IReadOnlyList<Dictionary<string, object?>> events, bool partial);

    /// <summary>Null when the source does not exist or is not ingest-kind.</summary>
    Task<IngestStatus?> GetStatusAsync(string sourceName);
}

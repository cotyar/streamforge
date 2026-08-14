using System.Text;
using System.Text.Json;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Formats;

namespace StreamForge.AppCore.Sinks;

/// <summary>
/// Plan 012: the outbound connection for ONE configured file sink — the egress twin of the
/// <c>file</c> SOURCE kind, and the reason this wave exists: a platform that reads CSV should be able to
/// write it. Owns one append-mode <see cref="StreamWriter"/> for its lifetime and holds the same
/// fire-and-forget contract <see cref="NatsSinkClient"/> documents at length: never throws, never blocks
/// the caller past <see cref="PublishTimeout"/>, counts and throttles its own failures. Read that class
/// first — this one is deliberately its smaller sibling.
///
/// <para><b>Append, never truncate.</b> The file is a log. Nothing here rewrites or shortens it, so
/// pointing a sink at the wrong path costs an operator some junk at the end of a file rather than its
/// contents. A restart re-opens the same file and continues after what is already there.</para>
///
/// <para><b>The CSV header is fixed for the life of the file</b>, which is the one real constraint of
/// writing CSV incrementally: columns are taken from <see cref="FileSinkConfig.Columns"/> when set, else
/// from the existing file's header when appending to a non-empty file, else from the first row written.
/// A column that shows up only in a LATER row is dropped, and counted as a failure (with the column name
/// in <see cref="SinkPublishCounters.LastError"/>) rather than being appended out of band — a row with an
/// extra cell would shift every following column for every reader downstream. Rows out of one table or
/// pipeline are uniform in practice; set <c>Columns</c> explicitly when they are not.</para>
///
/// <para><b>Honest limits.</b> Writes go to the HOST's filesystem as the host process user (same trust
/// the file/folder source kinds already extend to an Editor, in the write direction). There is no
/// rotation, no size cap and no fsync — the OS decides when the flushed bytes reach the disk. On Unix a
/// file deleted or rotated underneath a running sink keeps receiving writes on the old inode until the
/// sink is reconfigured; there is no reopen-on-rename watch.</para>
/// </summary>
public sealed class FileSinkClient : ISinkClient
{
    /// <summary>Upper bound on one publish, covering both the queue behind the write lock and the write
    /// itself — the mechanism behind "never blocks the caller" when the target is a stalled network mount.</summary>
    public static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(3);

    /// <summary>Minimum gap between two <c>onFailure</c> invocations for the SAME client (see
    /// <see cref="NatsSinkClient.LogThrottleWindow"/> — same reason, same value).</summary>
    public static readonly TimeSpan LogThrottleWindow = TimeSpan.FromSeconds(30);

    /// <summary>How much of an existing file is read looking for its header line. A CSV header longer
    /// than this is not a header.</summary>
    private const int HeaderProbeBytes = 64 * 1024;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly bool _csv;
    private readonly Action<string, Exception>? _onFailure;

    private List<string>? _columns;
    private bool _headerWritten;
    private StreamWriter? _writer;

    private long _published;
    private long _failed;
    private string? _lastError;
    private long _lastFailureAtMs;
    private long _lastLoggedAtMs;

    /// <param name="config">Non-null <c>SinkSpec.File</c> — the caller filters for that before constructing.</param>
    /// <param name="entityKind">"pipeline" | "table"; used only in failure callbacks.</param>
    /// <param name="entityName">Pipeline id or table name — also what <c>{name}</c> in the path expands to.</param>
    /// <param name="onFailure">Invoked (throttled) with (path, exception). Never invoked on success.</param>
    public FileSinkClient(
        FileSinkConfig config, string entityKind, string entityName, Action<string, Exception>? onFailure = null)
    {
        ArgumentNullException.ThrowIfNull(config);
        EntityName = entityName;
        EntityKind = entityKind;
        Path = config.Path.Replace("{name}", entityName, StringComparison.Ordinal);
        // Anything that isn't csv is written as NDJSON — the descriptor offers exactly those two, and a
        // typo in an imported config should still produce a readable file rather than nothing at all.
        _csv = string.Equals(config.Format, FileFormats.Csv, StringComparison.OrdinalIgnoreCase);
        _onFailure = onFailure;

        var columns = config.Columns
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToList();
        if (columns.Count > 0)
        {
            _columns = columns;
        }
    }

    /// <summary>The expanded destination path — <c>{name}</c> already substituted.</summary>
    public string Path { get; }

    public string EntityName { get; }

    /// <summary>"pipeline" | "table" — what kind of message this client renders (a table delta carries a
    /// weight; a pipeline row does not).</summary>
    public string EntityKind { get; }

    public SinkPublishCounters Counters => new(
        Interlocked.Read(ref _published),
        Interlocked.Read(ref _failed),
        Volatile.Read(ref _lastError),
        Interlocked.Read(ref _lastFailureAtMs));

    /// <summary>Appends one message. NEVER throws — <paramref name="ct"/> already cancelled is the one
    /// case treated as "the caller is shutting down" rather than a failure, exactly as
    /// <see cref="NatsSinkClient.PublishAsync{T}"/> does.</summary>
    public async Task PublishAsync<T>(T payload, CancellationToken ct)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(PublishTimeout);
            await _gate.WaitAsync(cts.Token).ConfigureAwait(false);
            try
            {
                var writer = Open();
                await writer.WriteAsync(Render(payload).AsMemory(), cts.Token).ConfigureAwait(false);
                await writer.FlushAsync(cts.Token).ConfigureAwait(false);
                _headerWritten = true;
            }
            finally
            {
                _gate.Release();
            }

            Interlocked.Increment(ref _published);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Host shutdown / sink reconfigured mid-publish — not a sink failure.
        }
        catch (Exception ex)
        {
            Fail(ex);
        }
    }

    /// <summary>The text one message contributes to the file: a CSV line (preceded by the header, once)
    /// or one NDJSON line carrying the whole message — the same JSON shape the NATS sink publishes, so a
    /// file sink and a NATS sink on the same entity produce the same records in two containers.</summary>
    private string Render<T>(T payload)
    {
        if (!_csv)
        {
            return JsonSerializer.Serialize(payload, JsonOptions) + "\n";
        }

        var row = RowOf(payload);
        var text = new StringBuilder();
        if (_columns is null)
        {
            _columns = [.. row.Keys];
        }

        if (!_headerWritten)
        {
            // Marked written only once the bytes are actually flushed (see PublishAsync) — a write that
            // fails must not leave the file destined to start with a data row.
            text.Append(CsvFormatter.Row(_columns));
        }

        var dropped = row.Keys.Where(k => !_columns.Contains(k)).ToList();
        if (dropped.Count > 0)
        {
            // Counted, not thrown: the row itself is still written, minus the columns the header has no
            // room for. See this class's doc for why the header cannot grow.
            Fail(new InvalidOperationException(
                $"row has column(s) not in this file's header and they were dropped: {string.Join(", ", dropped)}"));
        }

        text.Append(CsvFormatter.Row(_columns.Select(c => row.TryGetValue(c, out var v) ? v : null)));
        return text.ToString();
    }

    /// <summary>Flattens a sink message to the cells one CSV line holds. A table delta's
    /// <c>_weight</c> is part of the row here because a delta stream without its weights is not the
    /// table — a retraction would be indistinguishable from an insert.</summary>
    private static Dictionary<string, object?> RowOf<T>(T payload) => payload switch
    {
        NatsTableDeltaMessage d => new Dictionary<string, object?>(d.Row, StringComparer.Ordinal) { ["_weight"] = d.Weight },
        NatsPipelineRowMessage p => new Dictionary<string, object?>(p.Row, StringComparer.Ordinal),
        // No other payload type reaches a sink today; rendering its JSON into a single cell keeps a
        // future one visible in the file instead of silently empty.
        _ => new Dictionary<string, object?>(StringComparer.Ordinal) { ["value"] = JsonSerializer.Serialize(payload, JsonOptions) },
    };

    /// <summary>Opens (once) the append writer, creating missing parent directories. When the file
    /// already has content its FIRST LINE is the header — reused verbatim, so restarting the host
    /// continues an existing CSV instead of writing a second header or, worse, writing rows in a
    /// different column order under the old one.</summary>
    private StreamWriter Open()
    {
        if (_writer is not null)
        {
            return _writer;
        }

        var dir = System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(Path));
        if (!string.IsNullOrEmpty(dir))
        {
            Directory.CreateDirectory(dir);
        }

        if (_csv && new FileInfo(Path) is { Exists: true, Length: > 0 })
        {
            var existing = FormatParsers.CsvHeader(ReadHeaderProbe());
            if (existing.Count > 0)
            {
                _columns ??= existing;
                _headerWritten = true;
            }
        }

        _writer = new StreamWriter(new FileStream(Path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite));
        return _writer;
    }

    private string ReadHeaderProbe()
    {
        using var stream = new FileStream(Path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var buffer = new byte[(int)Math.Min(HeaderProbeBytes, stream.Length)];
        var read = stream.Read(buffer, 0, buffer.Length);
        return Encoding.UTF8.GetString(buffer, 0, read);
    }

    private void Fail(Exception ex)
    {
        Interlocked.Increment(ref _failed);
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

        if (Interlocked.CompareExchange(ref _lastLoggedAtMs, now, last) != last)
        {
            return;
        }

        _onFailure(Path, ex);
    }

    public async ValueTask DisposeAsync()
    {
        if (_writer is not null)
        {
            await _writer.DisposeAsync().ConfigureAwait(false);
            _writer = null;
        }

        _gate.Dispose();
    }
}

using System.Globalization;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;
using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.Connectors.Database;

/// <summary>
/// Plan 017 wave D: PostgreSQL logical replication (pgoutput) as an <see cref="IPolledTransport"/> —
/// <see cref="SourceKinds.PostgresCdc"/>. Reuses <see cref="DbSourceConfig"/> (<see cref="DbSourceConfig.SlotName"/>,
/// <see cref="DbSourceConfig.PublicationName"/>, <see cref="DbSourceConfig.Tables"/>,
/// <see cref="DbSourceConfig.MaxPollMs"/>, <see cref="DbSourceConfig.CreateSlotIfMissing"/>) and
/// <see cref="ISqlDialect"/> only for its connection-string plumbing (<see cref="DbSource"/>'s sibling in
/// spirit, not in code — a replication connection speaks a different protocol than the ADO.NET path
/// <see cref="DbSource"/> uses, so nothing else is shared).
///
/// <para><b>The one design decision that shapes this whole class: one replication connection PER POLL
/// CYCLE — not a long-lived streaming session.</b> <see cref="IPolledTransport"/> is a singleton per kind
/// and gets no "this source was deleted" notification, so a cached, long-lived replication session would
/// keep a slot open for a source that no longer exists — and an undrained-but-held slot pins WAL until the
/// SOURCE database's disk fills. Opening a fresh connection every cycle makes that class of bug impossible
/// by construction: there is nothing left running between cycles to leak. Durability is correct by
/// construction the other way too — the <c>cursor</c> handed to <see cref="PollAsync"/> is one
/// <c>PolledSourceCore</c> has ALREADY PERSISTED, so confirming exactly that position to Postgres, and
/// nothing newer, can never acknowledge data StreamForge has not durably recorded. WHERE in the cycle that
/// confirmation happens is a mechanical detail Npgsql's own protocol constraints decide — covered next —
/// and the durability property does not depend on it.</para>
///
/// <para><b>Confirmation timing: what Npgsql 10.0.3 actually accepts, verified against a live Postgres 17
/// container, not guessed from an exception message.</b> The obvious-looking order — confirm the previous
/// cursor, THEN call <c>StartReplication</c> — throws <c>InvalidOperationException: Status update can only
/// be sent during replication</c> every single time: <c>SetReplicationStatus</c>/<c>SendStatusUpdate</c>
/// only work once the connection has ACTUALLY entered replication mode, and <c>StartReplication</c> itself
/// is a lazy async-iterator that never sends <c>START_REPLICATION</c> to the server until the first
/// <c>MoveNextAsync</c> — so there is no moment before draining begins where a status update is legal.
/// Testing also ruled out the next-most-obvious fix, confirming in a <c>finally</c> AFTER the drain: that
/// only works when the loop exits on a plain <c>break</c>. The instant the SAME
/// <see cref="CancellationTokenSource"/> that bounds <see cref="DbSourceConfig.MaxPollMs"/> cancels an
/// IN-FLIGHT <c>MoveNextAsync</c> — the normal, expected way most cycles end, including ones that already
/// received and buffered rows — Npgsql has already dropped out of replication mode by the time the
/// <c>finally</c> runs, and the confirm throws the identical exception. What DOES work, verified
/// repeatedly: confirming on receipt of the FIRST message in the drain loop, before doing anything else
/// with it — see <c>DrainAsync</c>. That message is proof the connection is replicating; nothing before it
/// is, and nothing about ending the loop later undoes it. A cycle that never receives a single message (a
/// genuinely idle stream) confirms nothing at all THAT cycle, which is correct, not a gap: there is nothing
/// new to release WAL for beyond whatever an earlier cycle already confirmed, and re-affirming an unchanged
/// position on every idle tick would have no server-side effect an operator could observe.</para>
///
/// <para><b>The cost, stated rather than hidden:</b> a full replication handshake every cycle (slot lookup
/// or creation, <c>START_REPLICATION</c>) and a latency floor at the source's schedule interval — this is
/// not a push subscription that reacts within milliseconds of a commit. <see cref="PolledBatch.HasMore"/>
/// re-arms the driver immediately when a cycle stops because it filled <see cref="DbSourceConfig.BatchSize"/>
/// rather than because <see cref="DbSourceConfig.MaxPollMs"/> ran out, so a backlog drains at full speed
/// across successive cycles — the schedule interval caps latency when the source is idle or near-caught-up,
/// not throughput when it is behind.</para>
///
/// <para><b>A batch ends only on a transaction boundary.</b> Rows are buffered per transaction (from
/// <c>BeginMessage</c> to <c>CommitMessage</c>) and only moved into the emitted batch, with the cursor
/// advanced to <c>CommitMessage.TransactionEndLsn</c>, once that transaction's COMMIT is actually seen.
/// Rows from a transaction whose COMMIT this cycle never reaches are discarded, not emitted — emitting them
/// while confirming a cursor short of their commit would replay them after a restart; emitting them while
/// confirming past it would be worse, an unrecoverable gap. If a single transaction is larger than
/// <see cref="DbSourceConfig.BatchSize"/>, this reader keeps consuming its events (buffered, uncommitted)
/// past the cap rather than stalling — the cap is only checked immediately after a COMMIT flushes rows into
/// the batch, so it can be overshot by up to one transaction's worth of rows, never split one mid-flight.</para>
///
/// <para><b>Full, in-order tuple enumeration is not optional.</b> A pgoutput <c>ReplicationTuple</c> streams
/// its column values over the wire one at a time; this reader must consume every value, in the order
/// pgoutput sent them, before advancing to the next protocol message — see <c>AppendAsync</c>. Stashing a
/// tuple to read later, or skipping a column believed unneeded, desynchronizes the connection's read
/// position for every message after it, which is corruption, not merely a wrong row.</para>
///
/// <para><b>Relation caching is scoped to ONE poll cycle, on purpose.</b> pgoutput sends a
/// <c>RelationMessage</c> once per relation per replication SESSION — and because this reader opens a new
/// session (a new <c>StartReplication</c> call) every cycle by design, a fresh <see cref="PgRelationCache"/>
/// per cycle is exactly correct, not a shortcut: nothing needs to survive between cycles because pgoutput
/// itself resends the relation description at the start of the next one.</para>
///
/// <para><b>A delete under a non-<c>FULL</c> replica identity carries key columns only.</b>
/// (<c>KeyDeleteMessage.Key</c> rather than <c>FullDeleteMessage.OldRow</c>.) This reader emits that
/// partial row as-is — it does not fabricate the missing columns and does not drop the event, matching
/// <see cref="StreamForge.AppCore.Connectors.Mapping.CdcEnvelope"/>'s documented behavior for the same
/// situation on the Debezium path.</para>
///
/// <para><b>What is counted rather than fatal, and the honest gap in that today:</b> a
/// <c>TruncateMessage</c>/<c>TypeMessage</c>/<c>OriginMessage</c>/<c>LogicalDecodingMessage</c>, or any
/// future pgoutput message this reader does not recognize, never fails the cycle — one unrepresentable
/// message must not discard every good row sitting beside it, the same discipline
/// <c>ConnectorPollCycle</c> already applies to unrepresentable Debezium events via
/// <see cref="ConnectorRuntimeStatus.EnvelopeSkippedTotal"/>. <b>That counter has no live wire to this
/// class today</b>: <see cref="IPolledTransport.PollAsync"/> returns only <see cref="PolledBatch"/>
/// (rows/cursor/hasMore — a frozen contract), and <c>ConnectorPollCycle.ExecuteRows</c> — the entry point a
/// row source's rows go through — never produces a non-zero <c>EnvelopeSkipped</c>, unlike the
/// Debezium-envelope path that has its own counting built in. So these events ARE skipped safely, but the
/// count itself is not currently surfaced anywhere an operator can see it; wiring that through is future
/// work, not silently dropped correctness.</para>
///
/// <para><b>Delivery is at-least-once</b>, same ceiling as the polled kinds: a cycle can never confirm a
/// position ahead of what is durably persisted, because the ONLY value ever confirmed is <c>startLsn</c> —
/// the cursor an earlier cycle already persisted — never a position this cycle merely read; a cycle that
/// fails after emitting rows but before the NEXT cycle's cursor is persisted re-confirms that SAME
/// already-persisted cursor and re-streams from there, which can repeat rows already emitted but never
/// skips one.</para>
/// </summary>
public sealed class PgCdcSource(ISqlDialect dialect) : IPolledTransport, ISchemaProbe
{
    private readonly ISqlDialect _dialect = dialect;

    public string Kind => SourceKinds.PostgresCdc;

    public void Validate(SourceDefinition def, List<string> errors)
    {
        ArgumentNullException.ThrowIfNull(def);
        ArgumentNullException.ThrowIfNull(errors);

        var config = def.Connector?.Db;
        if (config is null)
        {
            errors.Add($"kind '{Kind}' requires connector.db");
            return;
        }

        if (!DbEndpoint.From(config).Addressable)
        {
            errors.Add("connector.db needs host + database (or a connectionString)");
        }

        if (string.IsNullOrWhiteSpace(config.SlotName))
        {
            errors.Add("connector.db needs a slotName — the Postgres logical replication slot this source streams from");
        }

        if (string.IsNullOrWhiteSpace(config.PublicationName))
        {
            errors.Add("connector.db needs a publicationName — the Postgres publication the slot streams from");
        }

        // These belong to the polled 'postgres' kind (a cursor COLUMN and a query to page it with); a CDC
        // source's cursor is the replication LSN, and its row set is whatever the publication carries, so
        // a value here is either a copy-paste from a polled source or a misunderstanding either way.
        if (!string.IsNullOrWhiteSpace(config.CursorColumn))
        {
            errors.Add($"connector.db.cursorColumn belongs to the polled '{SourceKinds.Postgres}' kind, not '{Kind}' — a CDC cursor is the replication LSN, not a column");
        }

        if (!string.IsNullOrWhiteSpace(config.CursorKind) && config.CursorKind != CursorKinds.Long)
        {
            errors.Add($"connector.db.cursorKind belongs to the polled '{SourceKinds.Postgres}' kind, not '{Kind}'");
        }

        if (!string.IsNullOrWhiteSpace(config.Query))
        {
            errors.Add($"connector.db.query belongs to the polled '{SourceKinds.Postgres}' kind, not '{Kind}' — CDC has no query, the publication defines what streams");
        }

        if (!string.IsNullOrWhiteSpace(config.Where))
        {
            errors.Add($"connector.db.where belongs to the polled '{SourceKinds.Postgres}' kind, not '{Kind}'");
        }

        if (!string.IsNullOrWhiteSpace(config.CaptureInstance))
        {
            errors.Add($"connector.db.captureInstance belongs to '{SourceKinds.MsSqlCdc}', not '{Kind}'");
        }

        if (config.Snapshot)
        {
            errors.Add(
                $"connector.db.snapshot is not supported by '{Kind}': a replication slot carries no history from " +
                "before its own creation, and the EXPORT_SNAPSHOT alternative needs one connection held open " +
                "across the whole backfill, which contradicts this transport's connection-per-cycle design. " +
                $"Backfill with the polled '{SourceKinds.Postgres}' kind first, then switch this source to " +
                $"'{Kind}' to tail changes from where the backfill left off.");
        }
    }

    public async Task<PolledBatch> PollAsync(SourceDefinition def, string? cursor, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(def);
        var config = def.Connector?.Db
            ?? throw new InvalidOperationException($"source '{def.Name}' is kind '{Kind}' but has no connector.db");

        await using var conn = new LogicalReplicationConnection(ConnectionStringFor(config));
        await conn.Open(ct).ConfigureAwait(false);

        if (cursor is null)
        {
            var seedCursor = await SeedCursorAsync(config, conn, ct).ConfigureAwait(false);
            return new PolledBatch([], seedCursor, HasMore: false);
        }

        var startLsn = CdcLsn.DecodePg(cursor);

        var slot = new PgOutputReplicationSlot(config.SlotName);
        // binary: true — verified against a live server (see class doc): pgoutput's default TEXT format
        // makes Npgsql's non-generic ReplicationValue.Get(ct) hand back every column as a bare string
        // ("10" for a bigint, not 10L), because there is no target type for it to parse text INTO without
        // one. Binary format carries its own type tag on the wire, so the same Get(ct) call resolves the
        // column's real CLR type (long/decimal/DateTime/...) exactly as DbSource's ADO.NET path already
        // does — which is the whole point: a native-CDC row must be typed identically to a polled one.
        var options = new PgOutputReplicationOptions(config.PublicationName, PgOutputProtocolVersion.V1, binary: true);
        var messages = conn.StartReplication(slot, options, ct, walLocation: startLsn);

        // Confirming happens inside DrainAsync, on receipt of the first message — Npgsql 10.0.3 rejects a
        // status update before the connection is actually replicating (verified against a live server; see
        // the class doc's "confirmation timing" section for what was tried and why this is what's left).
        return await DrainAsync(conn, startLsn, config, messages, ct).ConfigureAwait(false);
    }

    public Task<SchemaProbeResult> ProbeAsync(SourceDefinition def, CancellationToken ct)
        => CdcPreflight.ProbePostgresAsync(def, ct);

    public TransportDescriptor Describe() => new()
    {
        Kind = Kind,
        Label = "PostgreSQL (CDC)",
        Help =
            "Streams row changes from a logical replication slot instead of polling a cursor column. " +
            "Requires wal_level = logical on the server and a role with the REPLICATION privilege. A slot " +
            "nobody drains PINS WAL until the SOURCE database's disk fills — max_slot_wal_keep_size is the " +
            "server-side safety valve, not a substitute for keeping this source running. REPLICA IDENTITY " +
            "FULL on a table is what makes a DELETE carry more than its key columns; without it a delete " +
            "row is partial, not fabricated or dropped. An unchanged TOASTed column arrives as the sentinel " +
            "__debezium_unavailable_value, not its real content. DELIVERY IS AT-LEAST-ONCE. Snapshot is NOT " +
            $"supported here — backfill with the polled '{SourceKinds.Postgres}' kind first, then switch this " +
            "source to this kind to tail from where the backfill left off.",
        ConfigProperty = "db",
        Polled = true,
        Mapping = false,
        CanProbe = true,
        Groups =
        [
            new TransportGroup
            {
                Key = "replication",
                Label = "Replication",
                Help = "The slot and publication this source streams from, and the LSN it starts at.",
            },
            new TransportGroup
            {
                Key = "advanced",
                Label = "Advanced",
                Help = "Escape hatches. connectionString overrides every connection field above it.",
            },
        ],
        Fields =
        [
            new TransportField { Key = "host", Label = "Host", Required = true, Mono = true, Placeholder = "db.internal" },
            new TransportField { Key = "port", Label = "Port", Type = TransportFieldTypes.Number, Placeholder = _dialect.DefaultPort.ToString(CultureInfo.InvariantCulture), Help = "0 uses the default." },
            new TransportField { Key = "database", Label = "Database", Required = true, Mono = true },
            new TransportField { Key = "username", Label = "Username", Help = "Needs the REPLICATION privilege." },
            new TransportField { Key = "password", Label = "Password", Type = TransportFieldTypes.Secret },
            new TransportField { Key = "tls", Label = "Require TLS", Type = TransportFieldTypes.Bool },
            new TransportField
            {
                Key = "slotName", Label = "Slot name", Required = true, Mono = true, Group = "replication",
                Help = "The Postgres logical replication slot to stream from (pgoutput plugin). Not created automatically unless createSlotIfMissing is on.",
            },
            new TransportField
            {
                Key = "publicationName", Label = "Publication", Required = true, Mono = true, Group = "replication",
                Help = "CREATE PUBLICATION on the source database beforehand — this is the real filter on what tables stream.",
            },
            new TransportField
            {
                Key = "tables", Label = "Tables (CSV)", Mono = true, Group = "replication",
                Placeholder = "public.orders, public.customers",
                Help = "Optional 'schema.table' allowlist. The publication is the real filter; this only narrows what the driver surfaces from it.",
            },
            new TransportField
            {
                Key = "initialCursor", Label = "Start at LSN", Mono = true, Group = "replication",
                Placeholder = "0/16B3748",
                Help = "Postgres LSN to start from when no cursor is persisted yet. WINS over both slot creation and reading the slot's confirmed_flush_lsn.",
            },
            new TransportField
            {
                Key = "createSlotIfMissing", Label = "Create slot if missing", Type = TransportFieldTypes.Bool, Group = "advanced",
                Help = "Off by default ON PURPOSE: creating a slot begins pinning WAL on the source database, a consequential act on a system this connector does not own.",
            },
            new TransportField { Key = "maxPollMs", Label = "Max poll time (ms)", Type = TransportFieldTypes.Number, Default = "1000", Group = "advanced" },
            new TransportField { Key = "commandTimeoutSeconds", Label = "Command timeout (s)", Type = TransportFieldTypes.Number, Default = "30", Group = "advanced" },
            new TransportField
            {
                Key = "connectionString", Label = "Connection string", Type = TransportFieldTypes.Secret, Group = "advanced",
                Help = "Overrides host/port/database/username/password/TLS entirely. Masked wholesale, so the host stops being visible in the console — that is the cost of the escape hatch.",
            },
        ],
    };

    /// <summary>Builds the connection string via the dialect (TLS and every other endpoint rule already
    /// live there — see <see cref="ISqlDialect"/>'s class doc) without ever opening the throwaway
    /// <see cref="System.Data.Common.DbConnection"/> that produces it.</summary>
    private string ConnectionStringFor(DbSourceConfig config)
    {
        using var probe = _dialect.CreateConnection(DbEndpoint.From(config));
        return probe.ConnectionString;
    }

    /// <summary>The first-ever cycle for this source: mint a starting LSN and emit nothing. See the class
    /// doc's algorithm summary — <see cref="DbSourceConfig.InitialCursor"/> wins over both the
    /// create-slot and read-existing-slot branches when set.</summary>
    private async Task<string> SeedCursorAsync(DbSourceConfig config, LogicalReplicationConnection replicationConnection, CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(config.InitialCursor))
        {
            // Validate the format up front — a malformed InitialCursor should fail THIS cycle with a clear
            // message, not silently become an unparsable persisted cursor that fails every cycle after.
            CdcLsn.DecodePg(config.InitialCursor);
            return config.InitialCursor;
        }

        if (config.CreateSlotIfMissing)
        {
            var slot = await replicationConnection.CreatePgOutputReplicationSlot(
                config.SlotName,
                slotSnapshotInitMode: LogicalSlotSnapshotInitMode.NoExport,
                cancellationToken: ct).ConfigureAwait(false);
            return CdcLsn.EncodePg(slot.ConsistentPoint);
        }

        return await ReadConfirmedFlushLsnAsync(config, ct).ConfigureAwait(false);
    }

    /// <summary>Reads <c>confirmed_flush_lsn</c> for <see cref="DbSourceConfig.SlotName"/> over an ORDINARY
    /// connection — not the replication-protocol one <see cref="PollAsync"/> is mid-handshake on. Throws,
    /// naming the slot, the publication, and the exact statements an operator needs to run, when the slot
    /// does not exist: <see cref="DbSourceConfig.CreateSlotIfMissing"/> defaults to false precisely because
    /// creating one is a consequential act on a database this connector does not own, so this path never
    /// does it silently.</summary>
    private async Task<string> ReadConfirmedFlushLsnAsync(DbSourceConfig config, CancellationToken ct)
    {
        await using var connection = _dialect.CreateConnection(DbEndpoint.From(config));
        await connection.OpenAsync(ct).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = @p0";
        command.CommandTimeout = config.CommandTimeoutSeconds > 0 ? config.CommandTimeoutSeconds : 30;
        var parameter = command.CreateParameter();
        parameter.ParameterName = "p0";
        parameter.Value = config.SlotName;
        command.Parameters.Add(parameter);

        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        if (result is null or DBNull)
        {
            throw new InvalidOperationException(
                $"replication slot '{config.SlotName}' does not exist (or has no confirmed_flush_lsn yet) — " +
                "this source will not create one because that begins pinning WAL on the source database until " +
                $"something drains it. Create it and its publication first: SELECT pg_create_logical_replication_slot" +
                $"('{config.SlotName}', 'pgoutput'); and CREATE PUBLICATION {(string.IsNullOrWhiteSpace(config.PublicationName) ? "<publicationName>" : config.PublicationName)} " +
                "FOR ALL TABLES; (or FOR TABLE ... for a narrower set) — or turn on createSlotIfMissing to have " +
                "this source create the slot itself on its next cycle.");
        }

        return (string)result;
    }

    /// <summary>Consumes replication messages for up to <see cref="DbSourceConfig.MaxPollMs"/>, or until
    /// <see cref="DbSourceConfig.BatchSize"/> rows have accumulated at a transaction boundary — see the
    /// class doc's "batch ends only on a transaction boundary" section. Also where <paramref name="startLsn"/>
    /// gets confirmed to Postgres — see the class doc's "confirmation timing" section for why it happens
    /// HERE, on the first message, rather than before streaming starts.</summary>
    private static async Task<PolledBatch> DrainAsync(
        LogicalReplicationConnection conn,
        NpgsqlLogSequenceNumber startLsn,
        DbSourceConfig config,
        IAsyncEnumerable<PgOutputReplicationMessage> messages,
        CancellationToken ct)
    {
        var batchCap = Math.Max(1, config.BatchSize);
        var tableFilter = ParseTableFilter(config.Tables);
        var relations = new PgRelationCache();

        List<Dictionary<string, object?>> batch = [];
        List<Dictionary<string, object?>>? pending = null;
        string? cursor = null;
        var hasMore = false;
        var confirmed = false;

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromMilliseconds(Math.Max(1, config.MaxPollMs)));

        try
        {
            await foreach (var message in messages.WithCancellation(timeoutCts.Token).ConfigureAwait(false))
            {
                if (!confirmed)
                {
                    // The FIRST message to arrive is proof the connection has actually entered replication
                    // mode — the only moment Npgsql 10.0.3 will accept a status update (verified live; see
                    // class doc). Confirming ONLY startLsn — the cursor we were handed, never anything this
                    // cycle has read since — is what keeps this exactly as durable as the pinned design
                    // intended; only the WHEN changed, not the WHAT. A cycle that never receives a message
                    // (a genuinely idle poll) confirms nothing, which is correct: there is nothing new to
                    // release WAL for beyond what an earlier cycle already confirmed.
                    confirmed = true;
                    conn.SetReplicationStatus(startLsn);
                    await conn.SendStatusUpdate(ct).ConfigureAwait(false);
                }

                var stop = false;
                switch (message)
                {
                    case RelationMessage relation:
                        relations.Set(
                            relation.RelationId,
                            relation.Namespace,
                            relation.RelationName,
                            relation.Columns.Select(c => c.ColumnName).ToArray(),
                            relation.ReplicaIdentity.ToString());
                        break;

                    case BeginMessage:
                        // A fresh transaction starts here. Anything buffered under an unseen COMMIT — should
                        // never happen in a well-formed pgoutput stream, but nothing here assumes it can't —
                        // is discarded along with it, per the class doc's transaction-boundary rule.
                        pending = [];
                        break;

                    case InsertMessage insert:
                        pending ??= [];
                        await AppendAsync(pending, relations, insert.Relation, insert.NewRow, CdcStamp.OpCreate, tableFilter, keyOnly: false, ct).ConfigureAwait(false);
                        break;

                    case DefaultUpdateMessage update:
                        pending ??= [];
                        await AppendAsync(pending, relations, update.Relation, update.NewRow, CdcStamp.OpUpdate, tableFilter, keyOnly: false, ct).ConfigureAwait(false);
                        break;

                    case IndexUpdateMessage update:
                        pending ??= [];
                        await AppendAsync(pending, relations, update.Relation, update.NewRow, CdcStamp.OpUpdate, tableFilter, keyOnly: false, ct).ConfigureAwait(false);
                        break;

                    case FullUpdateMessage update:
                        pending ??= [];
                        await AppendAsync(pending, relations, update.Relation, update.NewRow, CdcStamp.OpUpdate, tableFilter, keyOnly: false, ct).ConfigureAwait(false);
                        break;

                    case KeyDeleteMessage delete:
                        // Non-FULL replica identity: only the key columns survive. Emitted as-is — see the
                        // class doc's honest-limit paragraph. Never fabricated, never dropped — keyOnly:
                        // true is what DROPS the non-key placeholders instead of fabricating them as null.
                        pending ??= [];
                        await AppendAsync(pending, relations, delete.Relation, delete.Key, CdcStamp.OpDelete, tableFilter, keyOnly: true, ct).ConfigureAwait(false);
                        break;

                    case FullDeleteMessage delete:
                        pending ??= [];
                        await AppendAsync(pending, relations, delete.Relation, delete.OldRow, CdcStamp.OpDelete, tableFilter, keyOnly: false, ct).ConfigureAwait(false);
                        break;

                    case CommitMessage commit:
                        if (pending is not null)
                        {
                            var tsMs = ToUnixMs(commit.TransactionCommitTimestamp);
                            foreach (var row in pending)
                            {
                                row[CdcStamp.TsColumn] = tsMs;
                            }

                            batch.AddRange(pending);
                            cursor = CdcLsn.EncodePg(commit.TransactionEndLsn);
                            pending = null;
                        }

                        if (batch.Count >= batchCap)
                        {
                            hasMore = true;
                            stop = true;
                        }

                        break;

                    default:
                        // TruncateMessage, TypeMessage, OriginMessage, LogicalDecodingMessage, and any future
                        // pgoutput message this reader does not recognize: counted-in-spirit, never fatal —
                        // one unrepresentable message must not discard every good row beside it. See the
                        // class doc's paragraph on where that count currently has no channel to travel through.
                        break;
                }

                if (stop)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // MaxPollMs elapsed while the caller's own token is still live — a normal end to the cycle, not
            // a failure. A real shutdown (ct itself firing) is NOT caught here and propagates, per
            // IPolledTransport.PollAsync's contract with PolledSourceCore.
        }

        return new PolledBatch(batch, cursor, hasMore);
    }

    /// <summary>Enumerates <paramref name="tuple"/> FULLY and IN ORDER — the discipline that loses data if
    /// skipped, see the class doc — decodes it via <see cref="PgTupleDecoder"/>, stamps it, and appends it
    /// to <paramref name="pending"/> unless <paramref name="tableFilter"/> excludes it. The tuple is always
    /// consumed to completion even when the row ends up filtered out, because pgoutput streams tuple values
    /// over the same wire every other message travels on — leaving one half-read would desynchronize
    /// everything that follows.
    ///
    /// <para><paramref name="keyOnly"/> — verified against a live server — is what
    /// <see cref="KeyDeleteMessage"/> needs and no other message does: pgoutput's key-only tuple is NOT
    /// shorter than the relation's column count, it is the SAME width with every non-key column's slot
    /// carrying <see cref="TupleDataKind.Null"/> as a placeholder, indistinguishable at the wire level from
    /// an actual SQL NULL. <see cref="PgTupleDecoder.Decode"/> cannot tell those apart on its own — it just
    /// sees "Null kind" either way — so the distinction is resolved HERE, the one place that knows which
    /// message produced the tuple: a <c>Value</c>-kind field never decodes to a C# <c>null</c> (a real SQL
    /// NULL always arrives as <c>Null</c>-kind, never as a null payload inside a decoded value), so after
    /// <see cref="PgTupleDecoder.Decode"/> returns, every <c>null</c> entry in its row unambiguously came
    /// from a <c>Null</c>-kind field — and for a key-only delete that means "not part of the key, not
    /// sent", which this method drops rather than emits. Emitting it as <c>null</c> would be
    /// indistinguishable from the row's real key column genuinely having a null value there, which cannot
    /// happen (Postgres requires a replica-identity key to be <c>NOT NULL</c>) — so dropping it is the
    /// only reading that is not a fabrication.</para></summary>
    private static async Task AppendAsync(
        List<Dictionary<string, object?>> pending,
        PgRelationCache relations,
        RelationMessage relationMessage,
        ReplicationTuple tuple,
        string op,
        HashSet<string>? tableFilter,
        bool keyOnly,
        CancellationToken ct)
    {
        var relation = relations.Get(relationMessage.RelationId);

        List<PgTupleField> fields = [];
        await foreach (var value in tuple.WithCancellation(ct).ConfigureAwait(false))
        {
            var kind = value.Kind switch
            {
                TupleDataKind.Null => PgTupleValueKind.Null,
                TupleDataKind.UnchangedToastedValue => PgTupleValueKind.UnchangedToast,
                _ => PgTupleValueKind.Value,
            };
            var clr = kind == PgTupleValueKind.Value ? Cell(await value.Get(ct).ConfigureAwait(false)) : null;
            fields.Add(new PgTupleField(value.GetFieldName(), kind, clr));
        }

        if (tableFilter is not null && !tableFilter.Contains(relation.QualifiedName))
        {
            return;
        }

        var decoded = PgTupleDecoder.Decode(relation, fields);
        var row = decoded.Row;
        if (keyOnly)
        {
            foreach (var name in row.Where(kv => kv.Value is null).Select(kv => kv.Key).ToList())
            {
                row.Remove(name);
            }
        }

        // _ts is applied later, once the enclosing transaction's COMMIT is seen — see DrainAsync's
        // CommitMessage case. Nothing here knows the commit timestamp yet.
        CdcStamp.Apply(row, op, relation.QualifiedName, tsMs: null);
        pending.Add(row);
    }

    private static HashSet<string>? ParseTableFilter(string tables)
        => string.IsNullOrWhiteSpace(tables)
            ? null
            : tables.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet(StringComparer.Ordinal);

    private static long ToUnixMs(DateTime commitTimestamp)
        => new DateTimeOffset(DateTime.SpecifyKind(commitTimestamp, DateTimeKind.Utc)).ToUnixTimeMilliseconds();

    /// <summary>One replication value as the platform's field types can hold it — the same conversion
    /// table <see cref="DbSource"/>'s own <c>Cell</c> applies to an ADO.NET value, kept in step with it by
    /// hand since that method is private to a file this wave does not touch. Anything with no CLR
    /// representation the platform's six field types can hold becomes JSON text, exactly what
    /// <see cref="FieldType.Json"/> means.</summary>
    private static object? Cell(object? raw) => raw switch
    {
        null or DBNull => null,
        string or bool or byte or sbyte or short or ushort or int or uint or long or ulong => raw,
        float or double or decimal => raw,
        DateTime or DateTimeOffset => raw,
        byte[] bytes => Convert.ToBase64String(bytes),
        Guid guid => guid.ToString(),
        DateOnly date => date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        TimeOnly time => time.ToString("O", CultureInfo.InvariantCulture),
        TimeSpan span => span.ToString("c", CultureInfo.InvariantCulture),
        char c => c.ToString(),
        _ => System.Text.Json.JsonSerializer.Serialize(raw, raw.GetType()),
    };
}

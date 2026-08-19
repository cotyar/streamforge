using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;
using StreamForge.AppCore.Access;
using StreamForge.AppCore.Config;

namespace StreamForge.Api;

/// <summary>
/// Plan 015 wave 8-B — <b>the execute half of "request → N-of-M approve → execute/expire"</b>.
///
/// <para>Waves 4 and 5 built everything up to the decision and stopped there:
/// <see cref="IApprovalFacade.RecordOutcomeAsync"/> was implemented on both flavours and called from no
/// route, so <see cref="ApprovalState.Executed"/> and <see cref="ApprovalState.Failed"/> were
/// unreachable outside tests and an approval granted its requester <i>nothing</i> — they had to retry
/// the original action, which was refused again unless their grants had changed in the meantime. This
/// class is what runs when the last vote lands, and <see cref="ApprovalsEndpoints.VoteAsync"/> is its
/// only caller.</para>
///
/// <h3>1. What was approved is the (Action, Scope) pair the approver saw — and that alone decides what runs</h3>
///
/// <para><b><see cref="ApprovalRequest.PayloadJson"/> is caller-supplied and untrusted.</b> The
/// requester chose it at filing time; the approver read two strings in an inbox. So the operation and
/// its target are derived <b>only</b> from <see cref="ApprovalRequest.Action"/> and
/// <see cref="ApprovalRequest.Scope"/>:</para>
/// <list type="bullet">
///   <item><see cref="ApprovalRequest.Action"/> selects the operation, from a closed switch. An action
///   with no case does not fall through to anything — it records <see cref="ApprovalState.Failed"/>
///   with a sentence naming itself.</item>
///   <item><see cref="ApprovalRequest.Scope"/> names the entity, and must name <b>exactly one</b>:
///   <c>*</c>, a prefix (<c>prod-*</c>) and a <c>tag:</c> scope are all refused
///   (<see cref="NamesOneEntity"/>). An approval whose scope covers a set must never be cashed in
///   against a member of that set the approver never looked at.</item>
///   <item>The payload may then supply only a <b>body</b> that cannot widen either: today that is one
///   <c>status</c> word for the lifecycle actions and one <see cref="SourceDefinition"/> for
///   <see cref="Actions.SourceWrite"/> — whose <c>Name</c> must equal the scope, because on a source the
///   name IS the identity. Any <c>name</c>/<c>id</c> the payload carries is checked against the entity
///   resolved <i>from the scope</i> (<see cref="IdentityDisagreement"/>): a payload pointing somewhere
///   else is a refusal, never a reconciliation — the scope wins, and disagreement means the approver and
///   the executor were looking at two different things.</item>
/// </list>
///
/// <h3>2. It runs on the approval's authority, and deliberately re-checks no entitlement</h3>
///
/// <para>The requester was refused — that refusal is <i>why</i> the approval exists — so re-running
/// their check here would refuse every approved request and the feature would execute nothing, which is
/// exactly the state this wave found. Nor is it run as the approver: an approver approves, they do not
/// impersonate. It runs unattributed to any live principal, bounded by (Action, Scope), and the
/// <b><see cref="AuditEntry.ApprovalId"/> on every row it writes</b> is what makes the action traceable
/// back to the decision that authorized it. An executed action nobody can trace back to its approval
/// defeats the point of having approved it.</para>
///
/// <para><b>Not <see cref="Actions.ApprovalBypass"/>.</b> That constant was declared in wave 1, is
/// referenced nowhere, and means the opposite thing: a <i>grant a human holds</i> that lets them skip
/// the second pair of eyes (break-glass), "conspicuous in an audit row" by its own doc comment. The
/// executor holds no grant and skips nothing — it cashes in an approval that was actually given. Using
/// the name here would mean that anyone later granted break-glass would silently inherit the executor's
/// authority, which is the wrong direction for the one entitlement nobody holds by default.</para>
///
/// <h3>3. At most once, by a claim that is a compare-and-swap in the store</h3>
///
/// <para>Two approvers voting concurrently can both observe <see cref="ApprovalState.Approved"/> on
/// their re-read (each vote is applied serially in the store, but both routes then read the post-state),
/// and one approver double-clicking gets there too. So "did I see it become Approved?" cannot be the
/// permission to execute. <see cref="ApprovalStateMachine.RecordOutcome"/> is the authority instead: it
/// transitions <b>out of</b> <see cref="ApprovalState.Approved"/> and refuses every state that is not
/// it, and the store applies it serially. <see cref="TryTransitionAsync"/> therefore
/// <b>claims before it executes</b> and proves the claim by writing a text carrying a unique attempt
/// token and reading it back: whoever finds their own token in
/// <see cref="ApprovalRequest.Outcome"/> won, everybody else returns without running anything. The
/// re-read is what makes it flavour-independent — the Orleans grain returns the request whether or not
/// the transition happened, the Dapr actor returns null.</para>
///
/// <para><b>A claim that cannot be recorded means nothing runs.</b> If the approval store throws, the
/// vote still succeeds (the approval is real) and the action does not happen — the opposite ordering
/// would be an execution nothing can account for. That is the one case where this class deliberately
/// does NOT swallow-and-continue the way audit does.</para>
///
/// <h3>4. What an exception during execution means, and why it is not "not approved"</h3>
///
/// <para>Three different facts, and a requester has to be able to tell them apart:</para>
/// <list type="bullet">
///   <item><b>Not approved</b> — the request is Pending / Rejected / Expired. Nothing ran and the state
///   says so.</item>
///   <item><b>Approved but unrunnable</b> — no executor for the action, a scope naming a set, a payload
///   disagreeing with the scope, a missing entity. Refused <i>before</i> the claim, recorded
///   <see cref="ApprovalState.Failed"/> with the sentence. Never <see cref="ApprovalState.Executed"/>,
///   never silence.</item>
///   <item><b>Approved, attempted, and the action itself failed</b> — a table with a running dependent,
///   a pipeline whose SQL no longer compiles. The approval was legitimately granted and the world
///   refused; the audit row carries <c>failed</c> and the exception's own sentence, with the approval
///   id on it.</item>
/// </list>
///
/// <para>ponytail: because the claim has to be taken BEFORE the outcome is known and
/// <see cref="ApprovalStateMachine.RecordOutcome"/> accepts exactly one transition out of
/// <see cref="ApprovalState.Approved"/>, a run that throws leaves the request
/// <see cref="ApprovalState.Executed"/> with a claim text that says "attempted; the audit row carries
/// the result" — true, but it makes the reader follow one hop. Ceiling: <c>state == Executed</c> alone
/// does not mean the action succeeded. Upgrade path (a state-machine change, deliberately NOT made
/// here): let <see cref="ApprovalStateMachine.RecordOutcome"/> also accept
/// <see cref="ApprovalState.Executed"/> → <see cref="ApprovalState.Failed"/> as a restatement, or add an
/// explicit claim state; the second <c>RecordOutcomeAsync</c> call this class already makes on the
/// failure path (<see cref="ReportFailureAsync"/>) then lands with no edit here.</para>
///
/// <h3>5. Which actions have an executor</h3>
///
/// <para>Deliberately not a general HTTP replayer — nothing about the original request is retained, and
/// rebuilding one from a payload is how a replay smuggles in what it did not have. The set is the
/// actions whose operation is one call on <see cref="ICatalogFacade"/>: <see cref="Actions.SourceWrite"/>,
/// <see cref="Actions.SourceDelete"/>, <see cref="Actions.PipelineDelete"/>,
/// <see cref="Actions.TableDelete"/>, <see cref="Actions.PipelineControl"/> and
/// <see cref="Actions.TableControl"/>.</para>
///
/// <para>ponytail: <b><see cref="Actions.PipelineWrite"/> and <see cref="Actions.TableWrite"/> have no
/// executor</b>, and neither do <see cref="Actions.ConfigReplace"/>, <see cref="Actions.UserWrite"/>,
/// <see cref="Actions.SourceRun"/> or <see cref="Actions.AccessWrite"/>. For the two catalog writes the
/// reason is specific: their REST handlers are ~70 lines each of DTO→definition translation (tag and
/// metadata merge, sink secret round-tripping, create-vs-update by id), and a second implementation of
/// that here is precisely the divergence this plan has already produced three times — three agents, one
/// rule, three answers. <see cref="Actions.SourceWrite"/> IS supported because a source's REST body IS
/// the stored definition: validate, <see cref="SecretsMasker.MergeSecrets"/>, upsert, with no
/// translation step to get wrong. Ceiling: an approved <c>pipeline.write</c> records Failed and the
/// requester must be granted the entitlement instead. Upgrade path: lift each PUT handler's
/// DTO→definition body into a shared function both it and a case here call — one refactor per entity,
/// worth doing when a deployment actually routes writes through approval.</para>
/// </summary>
public static class ApprovalExecutor
{
    /// <summary><see cref="AuditEntry.Origin"/> for everything this class writes. Not <c>rest</c>: a
    /// REST call is what filed and what voted, but nothing a caller sent is being executed here — the
    /// row's cause is the approval, and an operator filtering the log by origin should be able to ask
    /// "what ran because somebody approved it" as one query.</summary>
    public const string ApprovalOrigin = "approval";

    /// <summary>Outcome vocabulary, from <see cref="AuditEntry.Outcome"/>'s own doc comment.</summary>
    private const string ExecutedOutcome = "executed";
    private const string FailedOutcome = "failed";

    /// <summary>Outcome strings land in a persisted document that is rewritten whole; an exception
    /// message from a connector can be arbitrarily long, and a request list is read on every poll.</summary>
    private const int MaxOutcomeChars = 400;

    private static readonly JsonSerializerOptions PayloadOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
    };

    /// <summary>Run the approved action, once.
    ///
    /// <para>Returns the request as it now stands — <see cref="ApprovalState.Executed"/>,
    /// <see cref="ApprovalState.Failed"/>, or unchanged when another caller had already claimed it (or
    /// when the claim could not be recorded at all). The caller returns that to the approver as the
    /// vote's response body, so the answer to "what happened to the thing I just approved" is in the
    /// reply to the click that approved it.</para></summary>
    /// <param name="approver">Whose vote completed the approval. Used in the claim text and the audit
    /// detail so the log names both halves — who asked and who released it — and NOT as the actor of the
    /// change: the change is the requester's, which is what they asked for.</param>
    public static async Task<ApprovalRequest> ExecuteAsync(
        ApprovalRequest request,
        string approver,
        ICatalogFacade catalog,
        IApprovalFacade approvals,
        IAuditSink? sink,
        ILogger? logger)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (request.State != ApprovalState.Approved)
        {
            // Not reachable from the vote route (it checks), and cheap insurance against a second one.
            return request;
        }

        // ---------------------------------------------------------------------------- claim, then plan
        //
        // THE ORDER IS THE CORRECTNESS ARGUMENT, and it was the other way round until it was tested
        // against two concurrent callers. Planning first looks better — the knowable failures (no
        // executor, a set-shaped scope, a payload pointing elsewhere) would reach Failed without ever
        // passing through the claim's optimistic Executed. But a plan is computed against live catalog
        // state, so the caller that LOST the race plans against a world the winner has already changed,
        // concludes "the entity is gone", and records that failure over the winner's success. The loser
        // must not write anything at all, and the only thing that can tell it it lost is the claim.
        //
        // So: claim first. Nothing below this line runs for a caller that did not win, and every outcome
        // recorded below is the claim holder correcting its own optimistic claim — which is exactly the
        // one transition ApprovalStateMachine.RecordOutcome accepts beyond "out of Approved".
        var claim = Trim(
            $"{request.Action} on '{request.Scope}' claimed for execution when '{approver}' approved it; "
            + "the audit row for this approval carries the result");

        var (won, claimed) = await TryTransitionAsync(approvals, request, executed: true, claim, logger).ConfigureAwait(false);
        if (!won)
        {
            // Somebody else's claim, or a store that could not record ours. Either way: do not run, and
            // do not record — the state we are looking at is not ours to describe.
            logger?.LogInformation(
                "Approval {ApprovalId} was not claimed by this vote (state {State}); nothing executed.",
                request.Id, claimed.State);
            return claimed;
        }

        var (run, refusal) = await PlanAsync(request, catalog).ConfigureAwait(false);
        if (run is null)
        {
            // Approved, claimed by us, and unrunnable. Correcting our own claim to Failed is honest and
            // is now accepted; if the correction cannot be recorded the audit row is still written,
            // because "nothing happened and here is why" is the fact worth keeping.
            var (_, after) = await TryTransitionAsync(
                approvals, request, executed: false, $"not executed — {refusal}", logger).ConfigureAwait(false);

            Write(sink, Row(request, approver, FailedOutcome, $"not executed — {refusal}"));
            logger?.LogWarning(
                "Approval {ApprovalId} ({Action} on {Scope}) was approved but not executed: {Reason}",
                request.Id, request.Action, request.Scope, refusal);

            return after;
        }

        // ------------------------------------------------------------------------------------- run it
        var row = Row(
            request,
            approver,
            ExecutedOutcome,
            $"executed under approval {request.Id}, filed by '{request.RequestedBy}' and released by '{approver}'");

        try
        {
            await run(row, sink).ConfigureAwait(false);
            logger?.LogInformation(
                "Approval {ApprovalId} executed: {Action} on {Scope} (requested by {RequestedBy}, approved by {Approver}).",
                request.Id, request.Action, request.Scope, request.RequestedBy, approver);
        }
        catch (Exception ex)
        {
            await ReportFailureAsync(request, approver, approvals, sink, logger, ex).ConfigureAwait(false);
            return await approvals.GetAsync(request.Id).ConfigureAwait(false) ?? claimed;
        }

        return claimed;
    }

    // ==============================================================================================
    // Planning — everything decided from (Action, Scope), before anything is claimed or run
    // ==============================================================================================

    /// <summary>What this row would do, or why it cannot. Never throws: a planning problem is an
    /// answer, not an exception, because every one of them ends in a recorded
    /// <see cref="ApprovalState.Failed"/> with the sentence in it.</summary>
    private delegate Task Execution(AuditEntry row, IAuditSink? sink);

    private static async Task<(Execution? Run, string? Refusal)> PlanAsync(ApprovalRequest request, ICatalogFacade catalog)
    {
        var scope = request.Scope ?? "";

        if (!NamesOneEntity(scope))
        {
            return (null, $"scope '{scope}' names a set, not one entity — an approval given for a "
                + "wildcard, prefix or tag scope is never cashed in against a member of it");
        }

        switch (request.Action)
        {
            case Actions.SourceWrite:
            {
                if (!TryPayload<SourceDefinition>(request, out var incoming, out var error))
                {
                    return (null, error);
                }

                if (!string.Equals(incoming!.Name, scope, StringComparison.Ordinal))
                {
                    return (null, $"the payload describes source '{incoming.Name}' and the approval was "
                        + $"given for '{scope}'");
                }

                var errors = SourceValidation.Validate(incoming);
                if (errors.Count > 0)
                {
                    return (null, $"the payload is not a valid source: {string.Join("; ", errors)}");
                }

                var stored = await catalog.GetSourceAsync(scope).ConfigureAwait(false);
                return (async (row, sink) =>
                {
                    // The same MergeSecrets the PUT handler runs, and for the same D-H reason: an
                    // incoming "***" is the mask a read path produced, and persisting it literally would
                    // destroy the credential. On a create `stored` is null and this is a no-op.
                    var effective = SecretsMasker.MergeSecrets(incoming, stored);
                    await catalog.UpsertSourceAsync(effective).ConfigureAwait(false);
                    CatalogChangeAudit.RecordSource(sink, row, stored, effective);
                }, null);
            }

            case Actions.SourceDelete:
            {
                var stored = await catalog.GetSourceAsync(scope).ConfigureAwait(false);
                if (stored is null)
                {
                    return (null, $"no source named '{scope}' exists any more");
                }

                if (IdentityDisagreement(request, stored.Name, stored.Name) is { } clash)
                {
                    return (null, clash);
                }

                return (async (row, sink) =>
                {
                    if (!await catalog.DeleteSourceAsync(scope).ConfigureAwait(false))
                    {
                        throw new InvalidOperationException($"source '{scope}' was gone before the delete ran");
                    }

                    CatalogChangeAudit.RecordSource(sink, row, stored, null);
                }, null);
            }

            case Actions.PipelineDelete:
            {
                var (pipeline, missing) = await FindPipelineAsync(catalog, request, scope).ConfigureAwait(false);
                if (pipeline is null)
                {
                    return (null, missing);
                }

                return (async (row, sink) =>
                {
                    if (!await catalog.DeletePipelineAsync(pipeline.Id).ConfigureAwait(false))
                    {
                        throw new InvalidOperationException($"pipeline '{scope}' was gone before the delete ran");
                    }

                    CatalogChangeAudit.RecordPipeline(sink, row, pipeline, null);
                }, null);
            }

            case Actions.PipelineControl:
            {
                var (pipeline, missing) = await FindPipelineAsync(catalog, request, scope).ConfigureAwait(false);
                if (pipeline is null)
                {
                    return (null, missing);
                }

                if (!TryStatus(request, out var status, out var error))
                {
                    return (null, error);
                }

                return (async (row, sink) =>
                {
                    var updated = await catalog.SetPipelineStatusAsync(pipeline.Id, status).ConfigureAwait(false)
                        ?? throw new InvalidOperationException($"pipeline '{scope}' was gone before the {Word(status)} ran");

                    // Start-time compile failures are reported IN the definition (Failed + Error), not
                    // as an exception — so a run that "succeeded" and left the pipeline Failed has to be
                    // read as a failure here, or the audit row would claim the approved action worked.
                    if (updated.Status == PipelineStatus.Failed && !string.IsNullOrWhiteSpace(updated.Error))
                    {
                        CatalogChangeAudit.RecordPipeline(sink, row, pipeline, updated);
                        throw new InvalidOperationException(updated.Error);
                    }

                    CatalogChangeAudit.RecordPipeline(sink, row, pipeline, updated);
                }, null);
            }

            case Actions.TableDelete:
            {
                var (table, missing) = await FindTableAsync(catalog, request, scope).ConfigureAwait(false);
                if (table is null)
                {
                    return (null, missing);
                }

                return (async (row, sink) =>
                {
                    // Throws InvalidOperationException when a Running table depends on this one — a
                    // legitimately approved action the world refuses, which is the third of the three
                    // facts in the type remarks.
                    if (!await catalog.DeleteTableAsync(table.Id).ConfigureAwait(false))
                    {
                        throw new InvalidOperationException($"table '{scope}' was gone before the delete ran");
                    }

                    CatalogChangeAudit.RecordTable(sink, row, table, null);
                }, null);
            }

            case Actions.TableControl:
            {
                var (table, missing) = await FindTableAsync(catalog, request, scope).ConfigureAwait(false);
                if (table is null)
                {
                    return (null, missing);
                }

                if (!TryStatus(request, out var status, out var error))
                {
                    return (null, error);
                }

                return (async (row, sink) =>
                {
                    var updated = await catalog.SetTableStatusAsync(table.Id, status).ConfigureAwait(false)
                        ?? throw new InvalidOperationException($"table '{scope}' was gone before the {Word(status)} ran");

                    if (updated.Status == PipelineStatus.Failed && !string.IsNullOrWhiteSpace(updated.Error))
                    {
                        CatalogChangeAudit.RecordTable(sink, row, table, updated);
                        throw new InvalidOperationException(updated.Error);
                    }

                    CatalogChangeAudit.RecordTable(sink, row, table, updated);
                }, null);
            }

            default:
                return (null, $"no executor is wired for action '{request.Action}' — this deployment can "
                    + "record the decision but cannot carry it out, so nothing was changed and the "
                    + "requester needs the entitlement itself");
        }
    }

    private static async Task<(PipelineDefinition? Pipeline, string? Refusal)> FindPipelineAsync(
        ICatalogFacade catalog, ApprovalRequest request, string scope)
    {
        // By NAME, because wave 3 settled that the scope IS the name on all three surfaces: an id is a
        // Guid the registry minted, and a `prod-*` grant matches none of them.
        var pipeline = (await catalog.GetPipelinesAsync().ConfigureAwait(false))
            .FirstOrDefault(p => string.Equals(p.Name, scope, StringComparison.Ordinal));

        if (pipeline is null)
        {
            return (null, $"no pipeline named '{scope}' exists any more");
        }

        return IdentityDisagreement(request, pipeline.Name, pipeline.Id) is { } clash
            ? (null, clash)
            : (pipeline, null);
    }

    private static async Task<(TableDefinition? Table, string? Refusal)> FindTableAsync(
        ICatalogFacade catalog, ApprovalRequest request, string scope)
    {
        var table = (await catalog.GetTablesAsync().ConfigureAwait(false))
            .FirstOrDefault(t => string.Equals(t.Name, scope, StringComparison.Ordinal));

        if (table is null)
        {
            return (null, $"no table named '{scope}' exists any more");
        }

        return IdentityDisagreement(request, table.Name, table.Id) is { } clash
            ? (null, clash)
            : (table, null);
    }

    // ==============================================================================================
    // The payload, and the three things it is not allowed to do
    // ==============================================================================================

    /// <summary><c>*</c>, <c>prod-*</c> and <c>tag:finance</c> are the scope grammar's set-shaped forms
    /// (<see cref="PermissionEvaluator"/> owns the grammar; this only has to recognise "not one
    /// thing"). An empty scope is the same answer for the same reason.</summary>
    private static bool NamesOneEntity(string scope) =>
        !string.IsNullOrWhiteSpace(scope)
        && !scope.Contains('*', StringComparison.Ordinal)
        && !scope.StartsWith("tag:", StringComparison.Ordinal);

    /// <summary>Does the payload point at something other than the entity the SCOPE resolved to?
    ///
    /// <para>The comparison runs one way round on purpose: the entity is found from the scope first, and
    /// the payload's <c>name</c>/<c>id</c> is then required to denote <i>that</i> entity — which lets a
    /// chat-filed payload carrying the pipeline's id agree with a scope carrying its name, while a
    /// payload naming a different entity is a refusal. The scope never moves to meet the payload.</para></summary>
    private static string? IdentityDisagreement(ApprovalRequest request, string name, string id)
    {
        if (Payload(request) is not { } payload)
        {
            return null;
        }

        foreach (var key in new[] { "name", "id" })
        {
            if (payload[key] is not JsonValue value || value.GetValueKind() != JsonValueKind.String)
            {
                continue;
            }

            var claimed = value.GetValue<string>();
            if (!string.IsNullOrWhiteSpace(claimed)
                && !string.Equals(claimed, name, StringComparison.Ordinal)
                && !string.Equals(claimed, id, StringComparison.Ordinal))
            {
                return $"the payload's {key} is '{claimed}' and the approval was given for '{request.Scope}'";
            }
        }

        return null;
    }

    /// <summary>The one thing a lifecycle payload decides. <see cref="Actions.PipelineControl"/> covers
    /// start AND stop by construction of the action vocabulary, so choosing between them widens nothing
    /// the approver was not shown — the pair they approved was "control this pipeline".
    /// <para>ponytail: a deployment that wants "you may approve a stop but not a start" needs two
    /// actions, not a smarter executor; that is a change to <see cref="Actions"/> and to every guard
    /// that asks <see cref="Actions.PipelineControl"/>. Ceiling stated, not built.</para></summary>
    private static bool TryStatus(ApprovalRequest request, out PipelineStatus status, out string? error)
    {
        status = PipelineStatus.Stopped;
        var word = Payload(request)?["status"] is JsonValue node && node.GetValueKind() == JsonValueKind.String
            ? node.GetValue<string>()
            : null;

        if (string.IsNullOrWhiteSpace(word))
        {
            error = $"'{request.Action}' needs a payload saying which way: {{\"status\":\"Running\"}} or "
                + "{\"status\":\"Stopped\"}";
            return false;
        }

        if (!Enum.TryParse(word, ignoreCase: true, out status) || status == PipelineStatus.Failed)
        {
            error = $"the payload's status '{word}' is not Running or Stopped";
            return false;
        }

        error = null;
        return true;
    }

    private static bool TryPayload<T>(ApprovalRequest request, out T? value, out string? error)
        where T : class
    {
        value = null;

        if (string.IsNullOrWhiteSpace(request.PayloadJson))
        {
            error = $"'{request.Action}' needs the payload it would have executed, and this request carries none";
            return false;
        }

        try
        {
            value = JsonSerializer.Deserialize<T>(request.PayloadJson, PayloadOptions);
        }
        catch (JsonException ex)
        {
            error = $"the payload is not readable as {typeof(T).Name}: {ex.Message}";
            return false;
        }

        if (value is null)
        {
            error = $"the payload is not readable as {typeof(T).Name}";
            return false;
        }

        error = null;
        return true;
    }

    /// <summary>The payload as an object, or null when it is absent or is not one. Never throws — a
    /// malformed payload is data, and the only right answer to it is a refusal with a sentence.</summary>
    private static JsonObject? Payload(ApprovalRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.PayloadJson))
        {
            return null;
        }

        try
        {
            return JsonNode.Parse(request.PayloadJson) as JsonObject;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    // ==============================================================================================
    // The claim, the outcome, and the rows
    // ==============================================================================================

    /// <summary>One transition out of <see cref="ApprovalState.Approved"/>, and whether WE made it.
    ///
    /// <para>The proof is the text: <paramref name="outcome"/> gets a unique attempt token, and the
    /// stored <see cref="ApprovalRequest.Outcome"/> is read back and compared. Only the caller whose own
    /// token comes back may act. That is deliberately not "did the call return non-null" — the Orleans
    /// grain returns the request whether the state machine accepted or refused, the Dapr actor returns
    /// null on refusal, and a rule that reads differently on the two flavours is how you get an action
    /// executed twice on one of them.</para></summary>
    private static async Task<(bool Won, ApprovalRequest Now)> TryTransitionAsync(
        IApprovalFacade approvals, ApprovalRequest request, bool executed, string outcome, ILogger? logger)
    {
        var stamped = Trim($"{outcome} [attempt {Guid.NewGuid().ToString("n")[..8]}]");

        try
        {
            await approvals.RecordOutcomeAsync(request.Id, executed, stamped).ConfigureAwait(false);
            var now = await approvals.GetAsync(request.Id).ConfigureAwait(false) ?? request;
            return (string.Equals(now.Outcome, stamped, StringComparison.Ordinal), now);
        }
        catch (Exception ex)
        {
            // The claim is the at-most-once guarantee, so a store that cannot record it means nothing
            // runs. This is the one place here that does NOT swallow-and-continue: audit may be lost
            // without changing what happened, a claim may not.
            logger?.LogWarning(
                ex, "Approval {ApprovalId} could not have its outcome recorded; nothing was executed.", request.Id);
            return (false, request);
        }
    }

    /// <summary>An execution that threw: the audit row is the record, and the second
    /// <c>RecordOutcomeAsync</c> corrects the claim's optimistic Executed to Failed. That correction is
    /// the one transition <c>ApprovalStateMachine.RecordOutcome</c> accepts beyond "out of Approved",
    /// and it is safe only because this class claims before it plans — so the caller making it is
    /// always the claim holder, describing its own attempt.</summary>
    private static async Task ReportFailureAsync(
        ApprovalRequest request, string approver, IApprovalFacade approvals, IAuditSink? sink, ILogger? logger, Exception ex)
    {
        var reason = $"executed under approval {request.Id} and FAILED: {ex.Message}";

        logger?.LogError(
            ex, "Approval {ApprovalId} ({Action} on {Scope}) was approved and its execution failed.",
            request.Id, request.Action, request.Scope);

        Write(sink, Row(request, approver, FailedOutcome, reason));

        try
        {
            await approvals.RecordOutcomeAsync(request.Id, false, Trim(reason)).ConfigureAwait(false);
        }
        catch (Exception recordFailure)
        {
            logger?.LogWarning(recordFailure, "Approval {ApprovalId}: the failure could not be recorded either.", request.Id);
        }
    }

    /// <summary>The row every write here carries.
    ///
    /// <para><see cref="AuditEntry.Actor"/> is the REQUESTER: this is the change they asked for, and
    /// attributing it to the approver would read as the approver having made an edit they never made.
    /// The approver is named in <see cref="AuditEntry.Detail"/> instead, and their own decision already
    /// has its own row from <see cref="AccessGuard"/> on the vote. <see cref="AuditEntry.OnBehalfOf"/>
    /// stays null — it means "an agent acted for this human" and nothing here is an agent.</para></summary>
    private static AuditEntry Row(ApprovalRequest request, string approver, string outcome, string detail) => new()
    {
        Id = Guid.NewGuid().ToString("n"),
        AtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Actor = string.IsNullOrWhiteSpace(request.RequestedBy) ? "(unknown)" : request.RequestedBy,
        Action = request.Action,
        Scope = request.Scope,
        Outcome = outcome,
        Detail = detail,
        // The whole point of AuditEntry.ApprovalId, and the reason it was reserved in wave 0.
        ApprovalId = request.Id,
        Origin = ApprovalOrigin,
    };

    private static void Write(IAuditSink? sink, AuditEntry row)
    {
        try
        {
            sink?.Record(row);
        }
        catch (Exception)
        {
            // Same swallow as CatalogChangeAudit's, and for the same reason: the action already
            // happened (or already did not), and recording it must never change what the caller sees.
        }
    }

    private static string Trim(string text) =>
        text.Length <= MaxOutcomeChars ? text : text[..MaxOutcomeChars] + "…";

    private static string Word(PipelineStatus status) => status == PipelineStatus.Running ? "start" : "stop";
}

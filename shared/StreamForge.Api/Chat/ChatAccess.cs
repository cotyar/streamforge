using System.Collections.Frozen;
using System.Security.Claims;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;
using StreamForge.AppCore.Access;

namespace StreamForge.Api;

// =================================================================================================
// Plan 015 wave 3-C — the AI chat stops being the way around every entitlement.
//
// Before this file, POST /api/chat was gated ONCE (RequireAuthorization("Editor")) and its sixteen
// tools re-checked nothing. That made the chat a hole straight through waves 0-3: a Viewer cannot PUT
// a pipeline, but could ask the model to. The fix is not "gate the chat harder" — it is that every
// tool asks the SAME question its REST equivalent asks, with the same Actions constant at the same
// scope, so that a grant written for the console means exactly the same thing when the model invokes
// it.
//
// Three things live here, in the order they matter:
//   1. ChatToolPermissions — the tool -> action table, and the reason a tool that is not in it cannot
//      run at all.
//   2. ChatAttribution — Actor (the model) and OnBehalfOf (the human whose token it carried), which
//      AuditEntry deliberately keeps in two fields and which must never collapse into one.
//   3. ChatToolGate — the per-request object the tools call, holding the guard, the attribution, the
//      Chat:MayExecutePrivileged switch, and the two seams wave 4 fills in (approval store, audit
//      sink).
// =================================================================================================

/// <summary>
/// Which entitlement each chat tool needs. <b>This table, not the individual handlers, is the source
/// of truth</b> — a handler passes only the scope and the resource's tags, and
/// <see cref="ChatToolGate.AuthorizeAsync"/> looks the action up here. That is what keeps the chat
/// surface and the REST surface from drifting apart one careless edit at a time.
///
/// <para>Every action/scope pair below is copied from the row the same operation already has in
/// <c>shared/StreamForge.AppCore.Tests/Access/LegacyEquivalenceMatrixTests.cs</c> — the wave 2-B
/// enumeration of what each REST route means in entitlement terms. Where that matrix scopes a route by
/// an entity, the corresponding tool scopes at that entity's <b>NAME</b> — including pipelines and
/// tables, whose route segment is an id. An id is a <c>Guid("n")</c> the registry minted, so a scope an
/// operator would actually write (<c>prod-*</c>, an exact name) can only ever match the name; scoping on
/// the id would leave the feature present and useless on exactly the two entity types that have one.
/// REST and gRPC settled on the same rule, because a grant has to mean one thing on every transport. A
/// tool that lists rather than addresses asks at <c>*</c>, exactly as the list routes do.</para>
///
/// <para><b>A tool that is not in this table cannot run.</b> <see cref="ChatToolGate"/> denies an
/// unknown tool name rather than defaulting it to anything, so the failure mode of "wave 8 adds a
/// seventeenth tool and forgets the permission" is a dead tool and a loud reason string, not an
/// ungated one. <c>ChatAccessTests.Every_declared_chat_tool_has_a_permission</c> turns that into a
/// build-time-ish assertion by comparing this table against
/// <c>ChatToolCatalog.Descriptions</c>.</para>
/// </summary>
public static class ChatToolPermissions
{
    /// <summary>Tool name -> the <see cref="Actions"/> constant it needs.</summary>
    public static readonly FrozenDictionary<string, string> ByTool = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        // Sources — scoped by NAME, because that is what GET/PUT/DELETE /api/sources/{name} is scoped by.
        ["list_sources"] = Actions.SourceRead,
        ["get_source"] = Actions.SourceRead,
        ["create_source"] = Actions.SourceWrite,
        ["update_source"] = Actions.SourceWrite,
        // pause/resume have no REST route of their own: the SPA flips Enabled through
        // PUT /api/sources/{name}, which the matrix maps to source.write. So do these.
        ["pause_source"] = Actions.SourceWrite,
        ["resume_source"] = Actions.SourceWrite,
        ["delete_source"] = Actions.SourceDelete,

        // Pipelines — scoped by NAME (see the type comment: the {id} in the route is a Guid). create_pipeline
        // is the exception the matrix also has: POST /api/pipelines has no id in the route, and the
        // entity does not have one yet, so the proposed NAME is the only scope that exists at decision
        // time. A "may create pipelines called prod-*" entitlement is therefore expressible; a
        // "may create pipelines" one is written at *.
        ["list_pipelines"] = Actions.PipelineRead,
        ["get_pipeline"] = Actions.PipelineRead,
        ["create_pipeline"] = Actions.PipelineWrite,
        // POST /api/pipelines/validate is Editor-gated and the matrix maps it to pipeline.write at * —
        // compiling SQL against every source's schema is a read of the whole catalog's shape, and the
        // platform already decided that costs a write entitlement. Mirrored rather than softened.
        ["validate_sql"] = Actions.PipelineWrite,

        // Tables — scoped by NAME, same reason as pipelines.
        ["list_tables"] = Actions.TableRead,
        ["get_table"] = Actions.TableRead,
        ["table_rows"] = Actions.TableRead,
        ["search_table"] = Actions.TableRead,
        ["table_history"] = Actions.TableRead,
    }.ToFrozenDictionary(StringComparer.Ordinal);

    /// <summary>The tools actually offered to the model, from <c>ChatToolCatalog</c>. Exposed publicly
    /// purely so a test in another assembly can assert that this table and that catalog name exactly
    /// the same sixteen tools — without it, "add a tool, forget the permission" is caught only at
    /// runtime, by the tool being dead.</summary>
    public static IReadOnlyCollection<string> DeclaredTools => (IReadOnlyCollection<string>)ChatToolCatalog.Descriptions.Keys;
}

/// <summary>
/// Who acted, and on whose behalf. <see cref="AuditEntry"/> splits <see cref="AuditEntry.Actor"/> from
/// <see cref="AuditEntry.OnBehalfOf"/> precisely so an LLM's action is never recorded as a human's, and
/// this record is what carries both halves to every call site that will one day feed that sink.
///
/// <para><see cref="Actor"/> is the model (<c>model:gemini-3.6-flash</c>) and <see cref="OnBehalfOf"/>
/// is the authenticated username whose token the request carried. <see cref="Origin"/> is the constant
/// <c>"chat"</c>, which is also what <see cref="ApprovalRequest.Origin"/> wants, so an approver's inbox
/// can show an LLM-proposed change as an LLM-proposed change without reading the audit log.</para>
///
/// <para><b>The authorization decision is made for the HUMAN, not for the model.</b> There is no
/// separate model identity in the access document and inventing one would mean an entitlement nobody
/// granted. The model's identity is an attribution fact, not a permission fact — which is exactly why
/// it belongs in <see cref="AuditEntry.Actor"/> and not in the evaluator's input.</para>
/// </summary>
/// <param name="Actor">The model that produced the tool call, e.g. <c>model:gemini-3.6-flash</c>.</param>
/// <param name="OnBehalfOf">The authenticated human whose token the chat request carried.</param>
public sealed record ChatAttribution(string Actor, string OnBehalfOf)
{
    public const string ChatOrigin = "chat";

    public string Origin => ChatOrigin;

    /// <summary>The model's identity as an actor string. Prefixed so no audit reader ever has to guess
    /// whether <c>gemini-3.6-flash</c> is a person.</summary>
    public static string ActorFor(string model) => $"model:{model}";

    public static ChatAttribution For(string model, ClaimsPrincipal principal) =>
        new(ActorFor(model), principal.Identity?.Name ?? "");

    /// <summary>One audit row, with both identities and the origin already in place. Wave 4 changes
    /// where this goes, not how it is built.</summary>
    public AuditEntry Row(string action, string scope, string outcome, string? detail = null, string? approvalId = null) => new()
    {
        Id = Guid.NewGuid().ToString("n"),
        AtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Actor = Actor,
        OnBehalfOf = OnBehalfOf,
        Origin = Origin,
        Action = action,
        Scope = scope,
        Outcome = outcome,
        Detail = detail,
        ApprovalId = approvalId,
    };
}

/// <summary>
/// <b>Wave 4 seam.</b> Files an approval request and answers with the id the store assigned.
///
/// <para>Wave 4 owns the approval store and its state machine, so nothing here writes one. What this
/// wave owes wave 4 is a call site that already knows everything a request needs — action, scope,
/// reason, the human it is filed for, the payload that would have executed, and
/// <see cref="ApprovalRequest.Origin"/> = <c>chat</c> — so that connecting the store is a DI
/// registration and not a re-plumbing of sixteen tools.</para>
///
/// <para>The draft's <see cref="ApprovalRequest.RequestedBy"/> is the <i>human</i>, not the model: an
/// approver needs to know whose authority is being asked for and who to go and talk to. That the
/// proposal came from an LLM is carried by <see cref="ApprovalRequest.Origin"/> and repeated in the
/// reason text, which is the split <see cref="AuditEntry"/> makes for the same reason.</para>
/// </summary>
public interface IChatApprovalFiler
{
    /// <summary>The stored request's id, or <c>null</c> when no store is wired up yet.</summary>
    Task<string?> FileAsync(ApprovalRequest draft, CancellationToken ct);
}

/// <summary>The default until wave 4 registers a real one: files nothing and says so.
///
/// <para>This is the fail-closed half of "the model proposes, a human approves" — the tool does not
/// execute, the model is told an approval is required, and it is handed the correlation id that this
/// refusal was logged under. That id is honestly labelled as a correlation id and NOT as an approval
/// id, because there is no approval to have an id yet and telling a model otherwise would have it tell
/// a user to go and look for a request that does not exist.</para></summary>
public sealed class UnwiredChatApprovalFiler(ILogger logger) : IChatApprovalFiler
{
    public Task<string?> FileAsync(ApprovalRequest draft, CancellationToken ct)
    {
        logger.LogWarning(
            "Chat tool needs approval for {Action} on {Scope} (requested by {RequestedBy}, origin {Origin}) but no approval store is wired — refusing. Correlation {Correlation}.",
            draft.Action,
            draft.Scope,
            draft.RequestedBy,
            draft.Origin,
            draft.Id);
        return Task.FromResult<string?>(null);
    }
}

/// <summary><b>Wave 4 seam.</b> Where an audit row goes. Wave 4 replaces the default with the bounded
/// drop-on-overflow <c>Channel</c> the plan describes; until then every row is logged, with both
/// identities and the origin, which is the brief's "where you cannot yet write an audit row, log both
/// identities".</summary>
public interface IChatAuditSink
{
    void Record(AuditEntry entry);
}

/// <summary>The default sink: one structured log line per decision, carrying Actor, OnBehalfOf and
/// Origin as separate fields so a log query can already answer "what did the model do as alice".</summary>
public sealed class LoggingChatAuditSink(ILogger logger) : IChatAuditSink
{
    public void Record(AuditEntry entry) =>
        logger.LogInformation(
            "audit {Outcome} {Action} on {Scope} — actor {Actor} on behalf of {OnBehalfOf} (origin {Origin}){Detail}",
            entry.Outcome,
            entry.Action,
            entry.Scope,
            entry.Actor,
            entry.OnBehalfOf,
            entry.Origin,
            entry.Detail is null ? "" : $": {entry.Detail}");
}

/// <summary>What the model is told when a tool is refused outright. <see cref="Reason"/> is
/// <see cref="AccessResult.Reason"/> verbatim — it names the grant that denied or says that none
/// matched, which is what lets the model explain the refusal to the user instead of retrying blindly
/// or inventing a cause.</summary>
public sealed record ChatToolDenied(string Error, string Action, string Scope, string Reason);

/// <summary>What the model is told when the decision is <see cref="AccessDecision.RequiresApproval"/>
/// and <c>Chat:MayExecutePrivileged</c> is false: the tool did NOT run, a request was filed (or could
/// not be), and here is the identifier to quote.</summary>
/// <param name="ApprovalId">The stored request's id, or null when no approval store is wired up yet
/// (wave 4).</param>
/// <param name="CorrelationId">Always present, always honest: the id this refusal was logged under, so
/// a user can be pointed at something an operator can actually find in the log.</param>
public sealed record ChatToolApprovalRequired(
    bool ApprovalRequired,
    string Message,
    string? ApprovalId,
    string CorrelationId,
    string Action,
    string Scope,
    string Reason,
    string RequestedBy);

/// <summary>
/// The per-request object every chat tool asks before it does anything.
///
/// <para><b>Why a gate object and not just an <see cref="AccessGuard"/>.</b> Three things have to
/// travel together to every tool: the decision (the guard), who to attribute it to (the model AND the
/// human), and what to do with a <see cref="AccessDecision.RequiresApproval"/> answer. Passing the
/// guard alone would have each of sixteen call sites re-derive the other two, which is how the two
/// identities eventually collapse into one.</para>
///
/// <para><b><c>Chat:MayExecutePrivileged</c>, default false.</b> A grant that requires approval means a
/// human says yes before the thing happens; the model proposing it does not make it a human saying
/// yes. So the default files the request and refuses. Setting the flag to <c>true</c> makes the model
/// execute approval-gated actions on its own authority — which is a real configuration somebody may
/// want and a configuration a reviewer must be able to find, so it is named in the class, named in the
/// refusal-that-did-not-happen log line at <c>Warning</c>, and named in the audit row's Detail.</para>
/// </summary>
public sealed class ChatToolGate
{
    /// <summary>The config key, in one place so the report, the tests and the code cannot disagree.</summary>
    public const string MayExecutePrivilegedKey = "Chat:MayExecutePrivileged";

    private static readonly ClaimsPrincipal Anonymous = new(new ClaimsIdentity());

    private readonly AccessGuard? _guard;
    private readonly ClaimsPrincipal _principal;
    private readonly IChatApprovalFiler _filer;
    private readonly IChatAuditSink _audit;
    private readonly ILogger _logger;

    /// <param name="principal">The <b>human's</b> principal, which is what the guard evaluates.
    /// Separate from <paramref name="attribution"/> on purpose: the attribution says the model acted,
    /// the principal says whose entitlements it acted within, and those are two different questions.</param>
    public ChatToolGate(
        AccessGuard guard,
        ClaimsPrincipal principal,
        ChatAttribution attribution,
        bool mayExecutePrivileged,
        IChatApprovalFiler filer,
        IChatAuditSink audit,
        ILogger logger)
    {
        _guard = guard;
        _principal = principal;
        Attribution = attribution;
        MayExecutePrivileged = mayExecutePrivileged;
        _filer = filer;
        _audit = audit;
        _logger = logger;
    }

    private ChatToolGate(ChatAttribution attribution)
    {
        _guard = null;
        _principal = Anonymous;
        Attribution = attribution;
        MayExecutePrivileged = false;
        _filer = null!;
        _audit = null!;
        _logger = null!;
    }

    /// <summary>A gate that checks nothing, for constructing <see cref="GeminiChatService"/> outside the
    /// HTTP pipeline — which in this repo means the unit tests that drive the tool loop against a stub
    /// Gemini server and a fake catalog.
    ///
    /// <para>It exists because <c>GeminiChatService.HandleAsync</c>'s gate parameter is optional, and
    /// the parameter is optional because those tests predate this wave and a pre-existing test is a
    /// finding, not something to edit. <b>The production path never reaches it:</b>
    /// <c>ChatEndpoints</c> is the only caller of <c>HandleAsync</c> that a request can reach, and it
    /// always passes a real gate built from the DI <see cref="AccessGuard"/>. <see cref="IsUnguarded"/>
    /// is public so a test can assert exactly that.</para></summary>
    public static ChatToolGate Unguarded { get; } = new(new ChatAttribution("model:unguarded", ""));

    /// <summary>True only for <see cref="Unguarded"/>.</summary>
    public bool IsUnguarded => _guard is null;

    public ChatAttribution Attribution { get; }

    /// <summary>Whether a <see cref="AccessDecision.RequiresApproval"/> decision executes anyway.</summary>
    public bool MayExecutePrivileged { get; }

    /// <summary>
    /// Null when the tool may proceed; otherwise the object to hand straight back to the model.
    /// </summary>
    /// <param name="tool">The tool's name, which is how the action is looked up in
    /// <see cref="ChatToolPermissions.ByTool"/>. An unknown name is denied.</param>
    /// <param name="scope">The resource's name (sources) or id (pipelines, tables), or <c>"*"</c> for a
    /// tool that addresses no single resource — matching the scope its REST equivalent is checked at.</param>
    /// <param name="resourceTags">The resource's Tags, so <c>tag:finance</c> entitlements can match.
    /// Null narrows the answer; it never widens it.</param>
    /// <param name="args">The tool call's arguments, stored verbatim as the approval request's payload:
    /// re-executing from the payload is the only replay mechanism, so nothing about the original
    /// request can be smuggled into the approved one.</param>
    public async Task<object?> AuthorizeAsync(
        string tool,
        string scope,
        IReadOnlyCollection<string>? resourceTags,
        JsonElement args,
        CancellationToken ct = default)
    {
        if (IsUnguarded)
        {
            return null;
        }

        if (!ChatToolPermissions.ByTool.TryGetValue(tool, out var action))
        {
            // Fail closed on an unknown tool rather than defaulting to anything. A seventeenth tool
            // added without a row in the table is a dead tool with a legible reason, not an ungated one.
            var reason = $"tool '{tool}' declares no permission in ChatToolPermissions — refusing to run it";
            _audit.Record(Attribution.Row("chat.tool." + tool, scope, "denied", reason));
            return new ChatToolDenied($"tool '{tool}' is not permitted", "(undeclared)", scope, reason);
        }

        var result = await _guard!.CheckAsync(_principal, action, scope, resourceTags).ConfigureAwait(false);

        switch (result.Decision)
        {
            case AccessDecision.Allowed:
                _audit.Record(Attribution.Row(action, scope, "allowed", result.Reason));
                return null;

            case AccessDecision.RequiresApproval when MayExecutePrivileged:
                // The conspicuous configuration. Warning level, the key spelled out, and the same
                // sentence in the audit row's Detail — a reviewer asking "did anything run without an
                // approval?" gets one grep, not an archaeology exercise.
                _logger.LogWarning(
                    "{Key}=true — chat tool {Tool} ({Action} on {Scope}) EXECUTED WITHOUT APPROVAL. Actor {Actor} on behalf of {OnBehalfOf}. {Reason}",
                    MayExecutePrivilegedKey,
                    tool,
                    action,
                    scope,
                    Attribution.Actor,
                    Attribution.OnBehalfOf,
                    result.Reason);
                _audit.Record(Attribution.Row(
                    action,
                    scope,
                    "allowed",
                    $"{MayExecutePrivilegedKey}=true — executed WITHOUT approval. {result.Reason}"));
                return null;

            case AccessDecision.RequiresApproval:
                return await FileAsync(tool, action, scope, result, args, ct).ConfigureAwait(false);

            default:
                _audit.Record(Attribution.Row(action, scope, "denied", result.Reason));
                return new ChatToolDenied(
                    $"not permitted: {tool} requires '{action}' on '{scope}'",
                    action,
                    scope,
                    result.Reason);
        }
    }

    private async Task<object> FileAsync(string tool, string action, string scope, AccessResult result, JsonElement args, CancellationToken ct)
    {
        var correlationId = Guid.NewGuid().ToString("n");
        var draft = new ApprovalRequest
        {
            // The store assigns the real id; this one is the correlation id the refusal is logged under
            // so that an unwired build still gives the user something an operator can find.
            Id = correlationId,
            RequestedBy = Attribution.OnBehalfOf,
            RequestedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Action = action,
            Scope = scope,
            Reason = $"proposed by {Attribution.Actor} on behalf of {Attribution.OnBehalfOf} via the AI chat tool '{tool}'. {result.Reason}",
            Origin = ChatAttribution.ChatOrigin,
            PayloadJson = args.ValueKind == JsonValueKind.Undefined ? null : args.GetRawText(),
        };

        var approvalId = await _filer.FileAsync(draft, ct).ConfigureAwait(false);

        _audit.Record(Attribution.Row(
            action,
            scope,
            "requires-approval",
            approvalId is null
                ? $"no approval store wired (correlation {correlationId}). {result.Reason}"
                : result.Reason,
            approvalId));

        var message = approvalId is null
            ? $"This needs a human approval before it can run, and no approval store is configured on this deployment yet, so nothing was filed and nothing was changed. Tell the user to ask an administrator, quoting correlation id {correlationId}."
            : $"This needs a human approval before it can run. Approval request {approvalId} has been filed for {Attribution.OnBehalfOf}; nothing was changed. Tell the user to ask an approver to review it.";

        return new ChatToolApprovalRequired(
            ApprovalRequired: true,
            Message: message,
            ApprovalId: approvalId,
            CorrelationId: correlationId,
            Action: action,
            Scope: scope,
            Reason: result.Reason,
            RequestedBy: Attribution.OnBehalfOf);
    }
}

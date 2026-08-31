using System.Security.Claims;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.Api.Auth;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 015 wave 3-C — the AI chat stops being the way around every entitlement.
///
/// <para>Before this wave <c>POST /api/chat</c> was gated once and its sixteen tools re-checked
/// nothing, so a caller who could not PUT a pipeline could ask the model to. These tests are about the
/// four things that have to be true for that to be fixed, and they are deliberately written at two
/// levels: directly against <see cref="ChatToolGate"/> for the decision shapes, and through the real
/// <see cref="GeminiChatService"/> tool loop (against the stub Gemini server that
/// <c>GeminiChatServiceTests</c> already provides) for the one claim a unit test cannot make — that the
/// tools <i>actually ask</i>, and that a refused mutation never reaches the facade.</para>
///
/// <para><b>No Gemini API key is involved and none is needed.</b> The authorization layer is testable
/// without the model, which is the right test anyway: what is under test is what happens to a tool
/// call, not how one is produced.</para>
/// </summary>
public class ChatEntitlementTests
{
    private const string Human = "alice";

    // =============================================================================================
    // The table: every tool the model is offered must declare a permission.
    // =============================================================================================

    /// <summary>The failure this prevents: someone adds a seventeenth tool and forgets the permission
    /// row. <see cref="ChatToolGate"/> fails closed on an undeclared tool, so the consequence would be
    /// a dead tool rather than an ungated one — but a dead tool discovered in production is still a
    /// bug, and this catches it at build time.</summary>
    [Fact]
    public void Every_tool_offered_to_the_model_declares_a_permission()
    {
        var declared = ChatToolPermissions.DeclaredTools.OrderBy(n => n, StringComparer.Ordinal).ToList();
        var permitted = ChatToolPermissions.ByTool.Keys.OrderBy(n => n, StringComparer.Ordinal).ToList();

        Assert.Equal(declared, permitted);
    }

    /// <summary>Every action in the table is a real <see cref="Actions"/> constant. A permission
    /// spelled <c>source.wirte</c> would deny everything forever and read as a working grant check.</summary>
    [Fact]
    public void Every_declared_permission_is_a_real_action_constant()
    {
        var known = typeof(Actions)
            .GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToHashSet(StringComparer.Ordinal);

        Assert.All(ChatToolPermissions.ByTool, kv => Assert.Contains(kv.Value, known));
    }

    /// <summary>An undeclared tool is refused rather than defaulted to anything.</summary>
    [Fact]
    public async Task An_undeclared_tool_is_denied_by_the_gate()
    {
        var audit = new RecordingAuditSink();
        var gate = Gate(audit: audit, grants: [AllowAll()]);

        var refusal = await gate.AuthorizeAsync("exfiltrate_everything", "*", null, Args("{}"));

        var denied = Assert.IsType<ChatToolDenied>(refusal);
        Assert.Contains("declares no permission", denied.Reason);
    }

    // =============================================================================================
    // Denied — and the model is told WHY.
    // =============================================================================================

    /// <summary>The core claim of the wave, at the unit level: a read-only principal is refused the
    /// write action, at the scope the REST equivalent would have used.</summary>
    [Fact]
    public async Task A_viewer_entitled_principal_is_refused_a_mutating_tool()
    {
        var gate = Gate(grants: [Allow(Actions.SourceRead), Allow(Actions.PipelineRead), Allow(Actions.TableRead)]);

        var refusal = await gate.AuthorizeAsync("create_source", "trades", [], Args("""{"name":"trades"}"""));

        var denied = Assert.IsType<ChatToolDenied>(refusal);
        Assert.Equal(Actions.SourceWrite, denied.Action);
        Assert.Equal("trades", denied.Scope);
        // AccessResult.Reason verbatim, so the model can explain the refusal instead of retrying
        // blindly or inventing a cause.
        Assert.Contains("no grant matches 'source.write' on 'trades'", denied.Reason);
    }

    /// <summary>…and the same principal keeps the reads its entitlement actually covers. A gate that
    /// refused everything would pass the test above and be useless.</summary>
    [Fact]
    public async Task The_same_principal_keeps_the_reads_it_is_entitled_to()
    {
        var gate = Gate(grants: [Allow(Actions.SourceRead)]);

        Assert.Null(await gate.AuthorizeAsync("list_sources", "*", null, Args("{}")));
        Assert.Null(await gate.AuthorizeAsync("get_source", "trades", [], Args("""{"name":"trades"}""")));
    }

    /// <summary>A Deny written by an administrator names itself in the reason, which is the difference
    /// between "you are missing a grant" and "somebody denied this three months ago".</summary>
    [Fact]
    public async Task A_denying_grant_names_itself_in_the_reason_handed_to_the_model()
    {
        var gate = Gate(grants:
        [
            Allow(Actions.SourceWrite),
            new PermissionGrant { Action = Actions.SourceWrite, Scope = "prod-*", Effect = PermissionEffect.Deny, Note = "change freeze" },
        ]);

        var refusal = await gate.AuthorizeAsync("update_source", "prod-feed", [], Args("""{"name":"prod-feed"}"""));

        var denied = Assert.IsType<ChatToolDenied>(refusal);
        Assert.Contains("denied by grant", denied.Reason);
        Assert.Contains("change freeze", denied.Reason);
    }

    /// <summary>A tag-scoped entitlement matches because the tool passes the resource's Tags. Omitting
    /// them would silently narrow every tag grant to nothing, which is the kind of bug that reads as
    /// "authorization works" right up until nobody can do anything.</summary>
    [Fact]
    public async Task A_tag_scoped_grant_matches_when_the_tool_passes_the_resource_tags()
    {
        var gate = Gate(grants: [new PermissionGrant { Action = Actions.SourceWrite, Scope = "tag:finance" }]);

        Assert.Null(await gate.AuthorizeAsync("update_source", "trades", ["finance"], Args("{}")));
        Assert.IsType<ChatToolDenied>(await gate.AuthorizeAsync("update_source", "trades", ["marketing"], Args("{}")));
    }

    // =============================================================================================
    // RequiresApproval — the model proposes, a human approves.
    // =============================================================================================

    [Fact]
    public async Task With_MayExecutePrivileged_false_an_approval_is_filed_instead_of_executing()
    {
        var filer = new RecordingApprovalFiler { AssignedId = "apr-42" };
        var audit = new RecordingAuditSink();
        var gate = Gate(
            grants: [new PermissionGrant { Action = Actions.SourceDelete, Scope = "*", RequiresApproval = true }],
            mayExecutePrivileged: false,
            filer: filer,
            audit: audit);

        var refusal = await gate.AuthorizeAsync("delete_source", "trades", [], Args("""{"name":"trades","confirmed":true}"""));

        // The tool did NOT run: AuthorizeAsync answered with an object, and a non-null answer is what
        // every tool returns straight to the model instead of proceeding.
        var required = Assert.IsType<ChatToolApprovalRequired>(refusal);
        Assert.True(required.ApprovalRequired);
        Assert.Equal("apr-42", required.ApprovalId);
        Assert.Equal(Actions.SourceDelete, required.Action);
        Assert.Equal("trades", required.Scope);
        Assert.Equal(Human, required.RequestedBy);

        var draft = Assert.Single(filer.Filed);
        Assert.Equal(Actions.SourceDelete, draft.Action);
        Assert.Equal("trades", draft.Scope);
        // The HUMAN is who the request is filed for — an approver needs to know whose authority is
        // being asked for. That an LLM proposed it is carried by Origin, not by overwriting this.
        Assert.Equal(Human, draft.RequestedBy);
        Assert.Equal("chat", draft.Origin);
        Assert.Contains("model:gemini-test", draft.Reason);
        // The payload is the request that would have executed — the only replay mechanism there is.
        Assert.Contains("\"confirmed\":true", draft.PayloadJson);

        var row = Assert.Single(audit.Entries);
        Assert.Equal("requires-approval", row.Outcome);
        Assert.Equal("apr-42", row.ApprovalId);
    }

    /// <summary>Wave 4 owns the approval store. Until it lands the path must fail closed and legible:
    /// nothing executes, nothing is invented, and the model is handed the correlation id the refusal
    /// was logged under — explicitly not an approval id, because there is no approval.</summary>
    [Fact]
    public async Task Without_an_approval_store_nothing_executes_and_the_model_gets_an_honest_id()
    {
        var filer = new RecordingApprovalFiler { AssignedId = null };
        var gate = Gate(
            grants: [new PermissionGrant { Action = Actions.SourceWrite, Scope = "*", RequiresApproval = true }],
            filer: filer);

        var refusal = await gate.AuthorizeAsync("create_source", "trades", [], Args("{}"));

        var required = Assert.IsType<ChatToolApprovalRequired>(refusal);
        Assert.True(required.ApprovalRequired);
        Assert.Null(required.ApprovalId);
        Assert.NotEmpty(required.CorrelationId);
        Assert.Contains("no approval store is configured", required.Message);
        Assert.Contains(required.CorrelationId, required.Message);
        Assert.Single(filer.Filed);
    }

    /// <summary>The conspicuous configuration. Under <c>Chat:MayExecutePrivileged=true</c> the same
    /// decision executes, nothing is filed, and the audit row says in words which setting allowed
    /// it.</summary>
    [Fact]
    public async Task With_MayExecutePrivileged_true_the_same_decision_executes_and_says_so()
    {
        var filer = new RecordingApprovalFiler();
        var audit = new RecordingAuditSink();
        var gate = Gate(
            grants: [new PermissionGrant { Action = Actions.SourceWrite, Scope = "*", RequiresApproval = true }],
            mayExecutePrivileged: true,
            filer: filer,
            audit: audit);

        Assert.Null(await gate.AuthorizeAsync("create_source", "trades", [], Args("{}")));

        Assert.Empty(filer.Filed);
        var row = Assert.Single(audit.Entries);
        Assert.Equal("allowed", row.Outcome);
        Assert.Contains(ChatToolGate.MayExecutePrivilegedKey, row.Detail);
        Assert.Contains("WITHOUT approval", row.Detail);
    }

    // =============================================================================================
    // Attribution: the model acted, the human is who it acted for, and those never merge.
    // =============================================================================================

    [Fact]
    public async Task Every_audit_row_carries_the_model_the_human_and_the_origin_separately()
    {
        var audit = new RecordingAuditSink();
        var gate = Gate(grants: [Allow(Actions.SourceRead)], audit: audit);

        await gate.AuthorizeAsync("list_sources", "*", null, Args("{}"));          // allowed
        await gate.AuthorizeAsync("create_source", "trades", [], Args("{}"));      // denied

        Assert.Equal(2, audit.Entries.Count);
        Assert.All(audit.Entries, e =>
        {
            Assert.Equal("model:gemini-test", e.Actor);
            Assert.Equal(Human, e.OnBehalfOf);
            Assert.Equal("chat", e.Origin);
            Assert.NotEqual(e.Actor, e.OnBehalfOf);
        });
        Assert.Equal(["allowed", "denied"], audit.Entries.Select(e => e.Outcome));
    }

    /// <summary>The decision is made for the human's entitlements, not for a model identity nobody
    /// granted anything to. <c>ChatAttribution</c> is an attribution fact; the principal is the
    /// permission fact.</summary>
    [Fact]
    public void The_attribution_prefixes_the_model_so_no_reader_mistakes_it_for_a_person()
    {
        var attribution = ChatAttribution.For("gemini-3.6-flash", PrincipalFor("bob"));

        Assert.Equal("model:gemini-3.6-flash", attribution.Actor);
        Assert.Equal("bob", attribution.OnBehalfOf);
        Assert.Equal("chat", attribution.Origin);
    }

    /// <summary>The unguarded gate exists for constructing the service outside the HTTP pipeline and
    /// nowhere else; a gate built the normal way is never unguarded.</summary>
    [Fact]
    public void Only_the_explicit_Unguarded_gate_checks_nothing()
    {
        Assert.True(ChatToolGate.Unguarded.IsUnguarded);
        Assert.False(Gate(grants: [Allow(Actions.SourceRead)]).IsUnguarded);
    }

    // =============================================================================================
    // Through the real tool loop: the tools actually ask, and a refusal never reaches the facade.
    // =============================================================================================

    [Fact]
    public async Task A_refused_mutating_tool_never_reaches_the_catalog_facade()
    {
        using var stub = new StubGeminiServer(
        [
            """{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"create_source","args":{"name":"chat_demo","fields":[{"name":"price","type":"Double"}]}}}]},"finishReason":"STOP"}]}""",
            """{"candidates":[{"content":{"role":"model","parts":[{"text":"You are not allowed to do that."}]},"finishReason":"STOP"}]}""",
        ]);

        var catalog = new FakeChatCatalogFacade();
        var service = new GeminiChatService(new HttpClient(), stub.BaseUrl, "gemini-test", "test-key",
            catalog, new FakeChatTableReadFacade(), new FakeChatTableHistoryFacade());

        var response = await service.HandleAsync(
            new ChatRequest([new ChatMessage("user", "create chat_demo")]),
            PrincipalFor(Human),
            CancellationToken.None,
            Gate(grants: [Allow(Actions.SourceRead)]));

        Assert.Empty(catalog.Sources);
        var result = Assert.Single(response.ToolCalls).Result.GetRawText();
        Assert.Contains("source.write", result);
        Assert.Contains("no grant matches", result);
    }

    [Fact]
    public async Task An_entitled_principal_runs_the_very_same_tool()
    {
        using var stub = new StubGeminiServer(
        [
            """{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"create_source","args":{"name":"chat_demo","fields":[{"name":"price","type":"Double"}]}}}]},"finishReason":"STOP"}]}""",
            """{"candidates":[{"content":{"role":"model","parts":[{"text":"Created."}]},"finishReason":"STOP"}]}""",
        ]);

        var catalog = new FakeChatCatalogFacade();
        var service = new GeminiChatService(new HttpClient(), stub.BaseUrl, "gemini-test", "test-key",
            catalog, new FakeChatTableReadFacade(), new FakeChatTableHistoryFacade());

        var response = await service.HandleAsync(
            new ChatRequest([new ChatMessage("user", "create chat_demo")]),
            PrincipalFor(Human),
            CancellationToken.None,
            Gate(grants: [Allow(Actions.SourceWrite)]));

        Assert.Contains(catalog.Sources, s => s.Name == "chat_demo");
        Assert.Equal("Created.", response.Reply);
    }

    /// <summary>A destructive tool refused for lack of entitlement must not even reach the
    /// confirmation prompt — and must certainly not reach the delete.</summary>
    [Fact]
    public async Task An_unentitled_delete_never_reaches_the_facade_delete()
    {
        using var stub = new StubGeminiServer(
        [
            """{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"delete_source","args":{"name":"chat_demo","confirmed":true}}}]},"finishReason":"STOP"}]}""",
            """{"candidates":[{"content":{"role":"model","parts":[{"text":"Refused."}]},"finishReason":"STOP"}]}""",
        ]);

        var catalog = new FakeChatCatalogFacade();
        catalog.Sources.Add(new SourceDefinition { Name = "chat_demo", Fields = [new FieldDef("price", FieldType.Double)] });
        var service = new GeminiChatService(new HttpClient(), stub.BaseUrl, "gemini-test", "test-key",
            catalog, new FakeChatTableReadFacade(), new FakeChatTableHistoryFacade());

        await service.HandleAsync(
            new ChatRequest([new ChatMessage("user", "delete chat_demo")]),
            PrincipalFor(Human),
            CancellationToken.None,
            // Everything EXCEPT source.delete — the Editor-shaped mistake this wave is about.
            Gate(grants: [Allow(Actions.SourceRead), Allow(Actions.SourceWrite)]));

        Assert.False(catalog.DeleteSourceCalled);
        Assert.Contains(catalog.Sources, s => s.Name == "chat_demo");
    }

    /// <summary>A read tool checks the read action, at the entity's id — the scope
    /// <c>GET /api/tables/{id}</c> is checked at.</summary>
    [Fact]
    public async Task A_read_tool_is_refused_without_the_read_entitlement()
    {
        using var stub = new StubGeminiServer(
        [
            """{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"table_rows","args":{"table":"t-1"}}}]},"finishReason":"STOP"}]}""",
            """{"candidates":[{"content":{"role":"model","parts":[{"text":"Refused."}]},"finishReason":"STOP"}]}""",
        ]);

        var catalog = new FakeChatCatalogFacade();
        catalog.Tables.Add(new TableDefinition { Id = "t-1", Name = "positions" });
        var service = new GeminiChatService(new HttpClient(), stub.BaseUrl, "gemini-test", "test-key",
            catalog, new FakeChatTableReadFacade(), new FakeChatTableHistoryFacade());

        var response = await service.HandleAsync(
            new ChatRequest([new ChatMessage("user", "show me the positions table")]),
            PrincipalFor(Human),
            CancellationToken.None,
            Gate(grants: [Allow(Actions.SourceRead)]));

        var result = Assert.Single(response.ToolCalls).Result.GetRawText();
        Assert.Contains("table.read", result);
        // The refusal names the table's NAME, not the "t-1" the model addressed it by. An id is a
        // Guid("n") the registry minted, so an entitlement scope an operator would actually write can
        // only match the name — and REST, gRPC and the chat all had to settle on one answer or the same
        // grant would mean different things on different transports. See ChatToolPermissions' comment.
        Assert.Contains("positions", result);
    }

    // =============================================================================================
    // Helpers
    // =============================================================================================

    private static PermissionGrant Allow(string action) => new() { Action = action, Scope = "*" };

    private static PermissionGrant AllowAll() => new() { Action = "*", Scope = "*" };

    private static JsonElement Args(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static ClaimsPrincipal PrincipalFor(string username) =>
        new(new ClaimsIdentity([new Claim(ClaimTypes.Name, username)], "test"));

    /// <summary>A real <see cref="ChatToolGate"/> over a real <see cref="AccessGuard"/> over a real
    /// <see cref="PermissionResolver"/> — only the store behind it is a fake. Nothing about the
    /// decision path is stubbed, which is the point: these tests would not catch a guard wired to the
    /// wrong evaluator.</summary>
    private static ChatToolGate Gate(
        PermissionGrant[] grants,
        bool mayExecutePrivileged = false,
        IChatApprovalFiler? filer = null,
        IChatAuditSink? audit = null,
        string username = Human)
    {
        var document = new AccessPolicyDocument
        {
            Version = 1,
            Users = [new UserAccessEntry { Username = username, Grants = [.. grants] }],
        };
        var resolver = new PermissionResolver(
            new CountingAccessPolicyFacade(document),
            NullLogger<PermissionResolver>.Instance,
            policyCacheSeconds: 600);

        return new ChatToolGate(
            new AccessGuard(resolver, entitlementsEnabled: true),
            PrincipalFor(username),
            ChatAttribution.For("gemini-test", PrincipalFor(username)),
            mayExecutePrivileged,
            filer ?? new RecordingApprovalFiler(),
            audit ?? new RecordingAuditSink(),
            NullLogger.Instance);
    }
}

/// <summary>Stands in for wave 4's approval store. Records the drafts so a test can assert what the
/// chat call site already knows about the request it is filing.</summary>
internal sealed class RecordingApprovalFiler : IChatApprovalFiler
{
    public List<ApprovalRequest> Filed { get; } = [];

    /// <summary>The id the store would assign; null models "wave 4 has not landed".</summary>
    public string? AssignedId { get; set; }

    public Task<string?> FileAsync(ApprovalRequest draft, CancellationToken ct)
    {
        Filed.Add(draft);
        return Task.FromResult(AssignedId);
    }
}

/// <summary>Stands in for wave 4's audit channel.</summary>
internal sealed class RecordingAuditSink : IChatAuditSink
{
    public List<AuditEntry> Entries { get; } = [];

    public void Record(AuditEntry entry) => Entries.Add(entry);
}

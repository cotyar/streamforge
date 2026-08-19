using System.Net.Http;
using System.Security.Claims;
using Microsoft.Extensions.Logging.Abstractions;
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.Api.Auth;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 015 wave 5: the chat's mutating tools call <see cref="ICatalogFacade"/> directly and never pass
/// through the REST handlers, so wave 5-B's before/after detail did not reach them — the one surface
/// where "what did it change" matters most was the one with no answer. These pin the wiring that closed
/// that, and the two properties it must not lose on the way.
/// </summary>
public class ChatChangeAuditTests
{
    private const string Human = "alice";
    private const string SecretToken = "s3cr3t-token-do-not-log";

    [Fact]
    public async Task AChatSourceUpdate_RecordsBeforeAndAfter_WithoutTheSecretInPlaintext()
    {
        using var stub = new StubGeminiServer(
        [
            """{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"update_source","args":{"name":"feed","description":"renamed by the model"}}}]},"finishReason":"STOP"}]}""",
            """{"candidates":[{"content":{"role":"model","parts":[{"text":"Done."}]},"finishReason":"STOP"}]}""",
        ]);

        var catalog = new FakeChatCatalogFacade();
        catalog.Sources.Add(new SourceDefinition
        {
            Name = "feed",
            Description = "before",
            Kind = SourceKinds.Grpc,
            Fields = [new FieldDef("symbol", FieldType.String)],
            Connector = new ConnectorConfig
            {
                Grpc = new GrpcSubConfig { Address = "h:1", EntityKey = "source:upstream", Token = SecretToken },
            },
        });

        var audit = new RecordingAuditSink();
        var response = await Run(stub, catalog, audit);

        Assert.Single(response.ToolCalls);
        Assert.True(audit.Entries.Any(r => r.Outcome == "executed"),
            "no executed row; rows were: " + string.Join(" | ", audit.Entries.Select(r => $"{r.Outcome}/{r.Action}/{r.Scope}")) +
            "; tool result: " + response.ToolCalls[0].Result.GetRawText());
        var change = Assert.Single(audit.Entries, r => r.Outcome == "executed");

        // The change itself is legible…
        Assert.NotNull(change.BeforeJson);
        Assert.NotNull(change.AfterJson);
        Assert.Contains("before", change.BeforeJson);
        Assert.Contains("renamed by the model", change.AfterJson);

        // …and the credential is not, anywhere in the row. This is the whole reason the mutation sites
        // may only reach the sink through CatalogChangeAudit, which has no unmasked overload.
        foreach (var field in new[] { change.BeforeJson, change.AfterJson, change.Detail })
        {
            Assert.DoesNotContain(SecretToken, field ?? "");
        }
    }

    [Fact]
    public async Task TheChangeRowKeepsTheModelAsActorAndTheHumanAsOnBehalfOf()
    {
        using var stub = new StubGeminiServer(
        [
            """{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"update_source","args":{"name":"feed","description":"touched"}}}]},"finishReason":"STOP"}]}""",
            """{"candidates":[{"content":{"role":"model","parts":[{"text":"Done."}]},"finishReason":"STOP"}]}""",
        ]);

        var catalog = new FakeChatCatalogFacade();
        catalog.Sources.Add(new SourceDefinition
        {
            Name = "feed",
            Description = "before",
            Fields = [new FieldDef("symbol", FieldType.String)],
        });

        var audit = new RecordingAuditSink();
        await Run(stub, catalog, audit);

        var change = Assert.Single(audit.Entries, r => r.Outcome == "executed");

        // Actor is the model, OnBehalfOf is whose token it carried. Collapsing these into one field is
        // the failure this plan repeatedly refuses; a row that said only "alice" would be a lie about
        // who typed it, and one that said only the model would lose accountability entirely.
        Assert.Contains("gemini-test", change.Actor);
        Assert.Equal(Human, change.OnBehalfOf);
        Assert.Equal("chat", change.Origin);
    }

    private static async Task<ChatResponse> Run(StubGeminiServer stub, FakeChatCatalogFacade catalog, RecordingAuditSink audit)
    {
        var service = new GeminiChatService(new HttpClient(), stub.BaseUrl, "gemini-test", "test-key",
            catalog, new FakeChatTableReadFacade(), new FakeChatTableHistoryFacade());

        return await service.HandleAsync(
            new ChatRequest([new ChatMessage("user", "rename the feed source")]),
            PrincipalFor(Human),
            CancellationToken.None,
            GateFor(audit));
    }

    private static ChatToolGate GateFor(RecordingAuditSink audit)
    {
        var document = new AccessPolicyDocument
        {
            Version = 1,
            Users = [new UserAccessEntry { Username = Human, Grants = [new PermissionGrant { Action = "*", Scope = "*" }] }],
        };
        var resolver = new PermissionResolver(
            new CountingAccessPolicyFacade(document),
            NullLogger<PermissionResolver>.Instance,
            policyCacheSeconds: 600);

        return new ChatToolGate(
            new AccessGuard(resolver, entitlementsEnabled: true),
            PrincipalFor(Human),
            ChatAttribution.For("gemini-test", PrincipalFor(Human)),
            mayExecutePrivileged: false,
            new RecordingApprovalFiler(),
            audit,
            NullLogger.Instance);
    }

    private static ClaimsPrincipal PrincipalFor(string username) =>
        new(new ClaimsIdentity([new Claim(ClaimTypes.Name, username)], "test"));
}

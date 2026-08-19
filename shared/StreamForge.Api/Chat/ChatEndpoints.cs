using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;

namespace StreamForge.Api;

/// <summary>Plan 007 W1C, decision D-D: POST /api/chat — the AI control chat, implemented once here
/// so both runtime flavors get it with zero host edits (registered from
/// StreamForgeApiExtensions.AddStreamForgeApi/MapStreamForgeApi exactly like every other shared
/// endpoint group).
///
/// <para><b>Plan 015 wave 3-C.</b> Two gates, the wave 2-C pattern: the route keeps
/// <c>RequireAuthorization("Editor")</c> as the compatibility floor — the metadata
/// <c>AuthorizationCoverageTests</c> pins and <c>tools/authz-matrix.sh</c> exercises live — and the
/// handler additionally checks <see cref="Actions.ChatUse"/> at <c>*</c>, which is the row
/// <c>LegacyEquivalenceMatrixTests</c> already assigns to this route. But the door was never the
/// problem: the tools behind it re-checked nothing, so an Editor-gated chat handed the model the
/// caller's whole Editor surface regardless of any narrower entitlement. That is fixed one layer down,
/// in <see cref="ChatToolGate"/>; what this file owes it is a gate carrying the right principal, the
/// right attribution and the <c>Chat:MayExecutePrivileged</c> switch.</para></summary>
public static class ChatEndpoints
{
    /// <summary>Verbatim 503 body when no Gemini API key is configured — pinned for the SPA to surface as-is.</summary>
    public const string NotConfiguredMessage = "AI chat is not configured — set GEMINI_API_KEY (or Gemini:ApiKey) and restart.";

    /// <summary>429 body once a session has spent its <see cref="ChatRateLimiter"/> budget.</summary>
    public static string RateLimitedMessage(int max) =>
        $"AI chat limit reached — {max} messages per session. Sign in again to start a new session.";

    /// <summary>Rate-limit bucket for the caller: the token's jti (one per login), falling back to
    /// the username for tokens minted before jti was issued.</summary>
    private static string SessionKey(ClaimsPrincipal principal) =>
        principal.FindFirstValue("jti") ?? principal.Identity?.Name ?? "anonymous";

    public static void MapChatEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/chat");

        group.MapPost("/", async (ChatRequest req, ClaimsPrincipal principal, GeminiChatService chat, ChatRateLimiter limiter, AccessGuard guard, IServiceProvider services, CancellationToken ct) =>
        {
            // Before the 503, before the rate limiter, before anything is spent: whether this caller may
            // use the chat at all is not a question whose answer should depend on whether an API key
            // happens to be configured.
            var mayChat = await guard.CheckAsync(principal, Actions.ChatUse, "*").ConfigureAwait(false);
            if (!mayChat.IsAllowed)
            {
                // RequiresApproval is refused here rather than filed. Filing an approval to hold a
                // *conversation* would park a request that expires long before anyone reads it, and the
                // per-tool gate is where an approval actually buys something — it names a concrete
                // action on a concrete resource, which is the only thing an approver can meaningfully
                // say yes to.
                return AccessGuard.Deny(mayChat);
            }

            if (!chat.IsConfigured)
            {
                return Results.Json(new ErrorResponse(NotConfiguredMessage), statusCode: StatusCodes.Status503ServiceUnavailable);
            }

            if (req.Messages is not { Count: > 0 })
            {
                return Results.BadRequest(new ErrorResponse("messages is required"));
            }

            // Counted before the call goes upstream — the quota this protects is spent on request,
            // not on success.
            if (!limiter.TryAcquire(SessionKey(principal), out _))
            {
                return Results.Json(
                    new ErrorResponse(RateLimitedMessage(limiter.MaxPerSession)),
                    statusCode: StatusCodes.Status429TooManyRequests);
            }

            try
            {
                var response = await chat.HandleAsync(req, principal, ct, BuildGate(chat, principal, services)).ConfigureAwait(false);
                return Results.Ok(response);
            }
            catch (GeminiProviderException ex)
            {
                return Results.Json(new ErrorResponse(ex.Message), statusCode: StatusCodes.Status502BadGateway);
            }
        }).RequireAuthorization("Editor");
    }

    /// <summary>The per-request gate the tool loop asks. Built here and nowhere else, which is what
    /// makes "the chat is guarded" a property of one line rather than of sixteen.
    ///
    /// <para>The two wave-4 seams are resolved <b>optionally</b> from the container, with a logging
    /// default, so wave 4 turns the approval store and the audit channel on with two
    /// <c>AddSingleton</c> lines in a file it owns — no signature here changes, and no call site inside
    /// <c>ChatTools.cs</c> is touched again.</para></summary>
    private static ChatToolGate BuildGate(GeminiChatService chat, ClaimsPrincipal principal, IServiceProvider services)
    {
        var config = services.GetRequiredService<IConfiguration>();
        var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger("StreamForge.Api.Chat");

        return new ChatToolGate(
            services.GetRequiredService<AccessGuard>(),
            principal,
            // Actor is the MODEL, OnBehalfOf is the human whose token this request carried. AuditEntry
            // keeps them in two fields on purpose and this is the call site that must never merge them.
            ChatAttribution.For(chat.Model, principal),
            // Default false. True is the configuration where an approval-gated action runs on the
            // model's say-so; see ChatToolGate for the three places that fact is made conspicuous.
            config.GetValue(ChatToolGate.MayExecutePrivilegedKey, false),
            services.GetService<IChatApprovalFiler>() ?? new UnwiredChatApprovalFiler(logger),
            services.GetService<IChatAuditSink>() ?? new LoggingChatAuditSink(logger),
            logger);
    }
}

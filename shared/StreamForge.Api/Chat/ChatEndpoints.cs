using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace StreamForge.Api;

/// <summary>Plan 007 W1C, decision D-D: POST /api/chat — the AI control chat, implemented once here
/// so both runtime flavors get it with zero host edits (registered from
/// StreamForgeApiExtensions.AddStreamForgeApi/MapStreamForgeApi exactly like every other shared
/// endpoint group).</summary>
public static class ChatEndpoints
{
    /// <summary>Verbatim 503 body when no Gemini API key is configured — pinned for the SPA to surface as-is.</summary>
    public const string NotConfiguredMessage = "AI chat is not configured — set GEMINI_API_KEY (or Gemini:ApiKey) and restart.";

    public static void MapChatEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/chat");

        group.MapPost("/", async (ChatRequest req, ClaimsPrincipal principal, GeminiChatService chat, CancellationToken ct) =>
        {
            if (!chat.IsConfigured)
            {
                return Results.Json(new ErrorResponse(NotConfiguredMessage), statusCode: StatusCodes.Status503ServiceUnavailable);
            }

            if (req.Messages is not { Count: > 0 })
            {
                return Results.BadRequest(new ErrorResponse("messages is required"));
            }

            try
            {
                var response = await chat.HandleAsync(req, principal, ct).ConfigureAwait(false);
                return Results.Ok(response);
            }
            catch (GeminiProviderException ex)
            {
                return Results.Json(new ErrorResponse(ex.Message), statusCode: StatusCodes.Status502BadGateway);
            }
        }).RequireAuthorization("Editor");
    }
}

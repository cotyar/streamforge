using System.Text.Json;

namespace StreamForge.Api;

// ============================================================================
// Plan 007 (Cloud Run prep + admin app + AI control chat) W1C, decision D-D (provider changed to
// Google Gemini after the plan was written — see GeminiOptions in ChatService.cs). Public wire DTOs
// for POST /api/chat. Pinned verbatim for the SPA wave (W2A) to copy into web/src/api/types.ts:
//
//   ChatRequest  { "messages": [ { "role": "user"|"assistant", "content": string } ] }
//   ChatResponse { "reply": string, "toolCalls": [ { "name": string, "input": <json>, "result": <json> } ], "model": string }
//
// Stateless server: the client resends the whole conversation as plain text turns on every call —
// tool-call traffic from a PRIOR request is never round-tripped back to the client; only the final
// assistant text lands in the client's own message history. See GeminiChatService's class doc for why
// this keeps the server-side loop simple (no session state to store/expire).
// ============================================================================

/// <summary>One turn of the plain-text conversation the client maintains. <see cref="Role"/> is
/// "user" or "assistant" (never a tool-call/tool-result detail — those live only inside one
/// server-side request's own Gemini loop, see <see cref="GeminiChatService"/>).</summary>
public sealed record ChatMessage(string Role, string Content);

public sealed record ChatRequest(List<ChatMessage> Messages);

/// <summary>One executed tool call, surfaced to the client for transparency/debugging. <see cref="Result"/>
/// is truncated to ~2KB (see <see cref="ChatToolExecutor.TruncateToElement"/>) before being placed here.</summary>
public sealed record ChatToolCallDto(string Name, JsonElement Input, JsonElement Result);

public sealed record ChatResponse(string Reply, List<ChatToolCallDto> ToolCalls, string Model);

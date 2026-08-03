using System.Text.Json.Serialization;

namespace StreamForge.Api;

// ============================================================================
// Plan 007 W1C, decision D-D: internal wire-format POCOs for Google Gemini's native REST
// generateContent endpoint (POST {BaseUrl}/v1beta/models/{model}:generateContent, header
// `x-goog-api-key`). Field casing here matches exactly what the binding wave spec pins (mixed —
// "system_instruction" is the one snake_case top-level field, everything else camelCase; Gemini's
// JSON parser is protobuf-JSON, which accepts either the original proto field name or its camelCase
// JSON name on decode, so this shape round-trips against the real API even though canonical server
// RESPONSES are all camelCase).
// ============================================================================

internal sealed class GeminiGenerateContentRequest
{
    [JsonPropertyName("system_instruction")]
    public GeminiContent? SystemInstruction { get; set; }

    [JsonPropertyName("contents")]
    public List<GeminiContent> Contents { get; set; } = [];

    [JsonPropertyName("tools")]
    public List<GeminiTool>? Tools { get; set; }

    [JsonPropertyName("generationConfig")]
    public GeminiGenerationConfig? GenerationConfig { get; set; }
}

/// <summary>One turn. <see cref="Role"/> is "user" | "model" for conversation turns, and null for the
/// standalone <see cref="GeminiGenerateContentRequest.SystemInstruction"/> content (which has no role).</summary>
internal sealed class GeminiContent
{
    [JsonPropertyName("role")]
    public string? Role { get; set; }

    [JsonPropertyName("parts")]
    public List<GeminiPart> Parts { get; set; } = [];
}

/// <summary>Exactly one of the three is non-null: plain text, a model-issued tool call, or (on a
/// client-authored turn sent back to Gemini) a tool result.</summary>
internal sealed class GeminiPart
{
    [JsonPropertyName("text")]
    public string? Text { get; set; }

    [JsonPropertyName("functionCall")]
    public GeminiFunctionCall? FunctionCall { get; set; }

    [JsonPropertyName("functionResponse")]
    public GeminiFunctionResponse? FunctionResponse { get; set; }
}

internal sealed class GeminiFunctionCall
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("args")]
    public System.Text.Json.JsonElement Args { get; set; }
}

/// <summary>Gemini requires <see cref="Response"/> to be a JSON OBJECT (never a bare array/scalar) —
/// see ChatToolExecutor's wrapping of non-object tool results as {"result": ...} before this is built.</summary>
internal sealed class GeminiFunctionResponse
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("response")]
    public System.Text.Json.JsonElement Response { get; set; }
}

internal sealed class GeminiTool
{
    [JsonPropertyName("functionDeclarations")]
    public List<GeminiFunctionDeclaration> FunctionDeclarations { get; set; } = [];
}

/// <summary><see cref="Parameters"/> is an OpenAPI-subset JSON schema (type/properties/required/enum/
/// items — no $ref), built once per tool from a raw JSON literal — see ChatToolCatalog.</summary>
internal sealed class GeminiFunctionDeclaration
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("description")]
    public string Description { get; set; } = "";

    [JsonPropertyName("parameters")]
    public System.Text.Json.JsonElement Parameters { get; set; }
}

internal sealed class GeminiGenerationConfig
{
    [JsonPropertyName("maxOutputTokens")]
    public int MaxOutputTokens { get; set; } = 2048;

    // gemini-2.5 models "think" by default, and thinking tokens count against maxOutputTokens —
    // a tool-heavy turn can burn the whole budget on thinking and come back with NO content parts
    // at all (no text, no functionCall; finishReason MAX_TOKENS). Observed live on Cloud Run.
    // Budget 0 disables thinking for this control chat; null omits the field entirely.
    [JsonPropertyName("thinkingConfig")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public GeminiThinkingConfig? ThinkingConfig { get; set; }
}

internal sealed class GeminiThinkingConfig
{
    [JsonPropertyName("thinkingBudget")]
    public int ThinkingBudget { get; set; }
}

internal sealed class GeminiGenerateContentResponse
{
    [JsonPropertyName("candidates")]
    public List<GeminiCandidate>? Candidates { get; set; }
}

internal sealed class GeminiCandidate
{
    [JsonPropertyName("content")]
    public GeminiContent? Content { get; set; }

    [JsonPropertyName("finishReason")]
    public string? FinishReason { get; set; }
}

using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using StreamForge.Abstractions;

namespace StreamForge.Api;

/// <summary>Thrown when Gemini itself can't be reached or returns a non-2xx/unusable response —
/// ChatEndpoints maps this to 502.</summary>
public sealed class GeminiProviderException(string message) : Exception(message);

/// <summary>
/// Plan 007 W1C, decision D-D: server-side tool loop over Google Gemini's native REST generateContent
/// endpoint. STATELESS: every POST /api/chat call re-derives the whole Gemini-format conversation from
/// the client's plain-text message history and re-runs the loop from scratch — no session state is
/// stored server-side, so there's nothing to expire or leak between users. Tool-call/tool-result
/// exchanges happen ENTIRELY within one HTTP request's own loop (capped at
/// <see cref="MaxToolIterations"/> round-trips to Gemini) and are surfaced back to the caller only as
/// the flattened <see cref="ChatToolCallDto"/> list — never round-tripped back in as input on a later
/// request (see ChatDtos.cs's class doc).
/// </summary>
public sealed class GeminiChatService(
    HttpClient httpClient,
    string baseUrl,
    string model,
    string? apiKey,
    ICatalogFacade catalog,
    ITableReadFacade tables,
    ITableHistoryFacade history,
    // Thinking control differs by model generation (both observed live):
    //   gemini-2.x — thinkingBudget (0 = off, our default: thinking tokens count against
    //     maxOutputTokens and can consume the whole budget, returning a candidate with NO content
    //     parts). Negative = omit. Config: Gemini:ThinkingBudget.
    //   gemini-3.x — rejects thinkingBudget with 400; uses thinkingLevel ("LOW" default here —
    //     keeps the empty-reply risk down and tool loops fast). Empty string = omit. Config:
    //     Gemini:ThinkingLevel.
    int thinkingBudget = 0,
    string? thinkingLevel = "LOW")
{
    private const int MaxToolIterations = 8;

    private const string SystemPrompt =
        "You are the StreamForge control assistant. StreamForge is a streaming-SQL platform over " +
        "financial market data: SOURCES emit streaming events (synthetic generators or external " +
        "connectors), PIPELINES run streaming SQL over sources to produce live derived results, and " +
        "TABLES are materialized SQL views (running aggregates, no windows) that pipelines and other " +
        "tables can query. Use the provided tools to inspect and manage sources/pipelines/tables on " +
        "the user's behalf instead of guessing. Never call delete_source with confirmed=true unless " +
        "the user has explicitly confirmed that exact deletion earlier in this conversation — if " +
        "unsure, ask first instead of calling the tool. Always answer in the same language the user " +
        "is writing in. Keep replies concise and to the point.";

    public bool IsConfigured => !string.IsNullOrWhiteSpace(apiKey);

    public string Model => model;

    public async Task<ChatResponse> HandleAsync(ChatRequest request, ClaimsPrincipal principal, CancellationToken ct)
    {
        var contents = request.Messages.Select(m => new GeminiContent
        {
            Role = string.Equals(m.Role, "assistant", StringComparison.OrdinalIgnoreCase) ? "model" : "user",
            Parts = [new GeminiPart { Text = m.Content }],
        }).ToList();

        var toolContext = new ChatToolContext(catalog, tables, history, principal);
        var toolCalls = new List<ChatToolCallDto>();
        var thoughts = new List<string>();

        for (var iteration = 0; iteration < MaxToolIterations; iteration++)
        {
            var candidate = await CallGeminiAsync(contents, ct).ConfigureAwait(false);
            var parts = candidate.Content?.Parts ?? [];
            thoughts.AddRange(parts.Where(p => p.Thought && !string.IsNullOrWhiteSpace(p.Text)).Select(p => p.Text!.Trim()));
            var functionCalls = parts.Where(p => p.FunctionCall is not null).ToList();

            if (functionCalls.Count == 0)
            {
                // Thought parts carry text too — they belong to Thinking, never to the reply.
                var reply = string.Concat(parts.Where(p => p.Text is not null && !p.Thought).Select(p => p.Text));
                if (string.IsNullOrWhiteSpace(reply))
                {
                    // Never hand the UI an empty bubble — happens when the model exhausts its
                    // output budget (e.g. thinking, see the ctor comment) or ends a turn silently.
                    reply = toolCalls.Count > 0
                        ? "(The model returned no closing text — the tool calls below carry the results.)"
                        : $"The model returned an empty response (finishReason: {candidate.FinishReason ?? "unknown"}) — please try again.";
                }

                return new ChatResponse(reply, toolCalls, model, thoughts.Count > 0 ? string.Join("\n\n", thoughts) : null);
            }

            // Append the model's own turn (verbatim — including any interleaved text parts) before
            // the tool-result turn that answers it, exactly as Gemini's multi-turn contract requires.
            contents.Add(candidate.Content!);

            var responseParts = new List<GeminiPart>();
            foreach (var call in functionCalls)
            {
                var fc = call.FunctionCall!;
                var rawResult = await ChatToolExecutor.ExecuteAsync(fc.Name, fc.Args, toolContext).ConfigureAwait(false);
                var truncated = ChatToolExecutor.TruncateToElement(rawResult);
                toolCalls.Add(new ChatToolCallDto(fc.Name, fc.Args, truncated));

                responseParts.Add(new GeminiPart
                {
                    FunctionResponse = new GeminiFunctionResponse
                    {
                        Name = fc.Name,
                        Response = ChatToolExecutor.WrapAsResponseObject(truncated),
                    },
                });
            }

            contents.Add(new GeminiContent { Role = "user", Parts = responseParts });
        }

        return new ChatResponse(
            "I've reached the tool-call budget for this turn without a final answer — please continue in a new message.",
            toolCalls,
            model,
            thoughts.Count > 0 ? string.Join("\n\n", thoughts) : null);
    }

    private async Task<GeminiCandidate> CallGeminiAsync(List<GeminiContent> contents, CancellationToken ct)
    {
        var body = new GeminiGenerateContentRequest
        {
            SystemInstruction = new GeminiContent { Parts = [new GeminiPart { Text = SystemPrompt }] },
            Contents = contents,
            Tools = [new GeminiTool { FunctionDeclarations = ChatToolCatalog.Declarations.ToList() }],
            GenerationConfig = new GeminiGenerationConfig
            {
                MaxOutputTokens = 2048,
                ThinkingConfig = model.StartsWith("gemini-2", StringComparison.OrdinalIgnoreCase)
                    ? (thinkingBudget < 0 ? null : new GeminiThinkingConfig { ThinkingBudget = thinkingBudget, IncludeThoughts = thinkingBudget > 0 ? true : null })
                    : (string.IsNullOrEmpty(thinkingLevel) ? null : new GeminiThinkingConfig { ThinkingLevel = thinkingLevel, IncludeThoughts = true }),
            },
        };

        var url = $"{baseUrl.TrimEnd('/')}/v1beta/models/{model}:generateContent";
        using var requestMessage = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = JsonContent.Create(body),
        };
        requestMessage.Headers.Add("x-goog-api-key", apiKey);

        HttpResponseMessage response;
        try
        {
            response = await httpClient.SendAsync(requestMessage, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            throw new GeminiProviderException($"could not reach Gemini: {ex.Message}");
        }

        using (response)
        {
            if (!response.IsSuccessStatusCode)
            {
                var errorBody = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                throw new GeminiProviderException($"Gemini returned HTTP {(int)response.StatusCode}: {Truncate(errorBody, 500)}");
            }

            GeminiGenerateContentResponse? parsed;
            try
            {
                parsed = await response.Content.ReadFromJsonAsync<GeminiGenerateContentResponse>(ct).ConfigureAwait(false);
            }
            catch (JsonException ex)
            {
                throw new GeminiProviderException($"Gemini returned an unparseable response: {ex.Message}");
            }

            var candidate = parsed?.Candidates?.FirstOrDefault();
            if (candidate is null)
            {
                throw new GeminiProviderException("Gemini returned no candidates.");
            }

            return candidate;
        }
    }

    private static string Truncate(string s, int max) => s.Length <= max ? s : s[..max];
}

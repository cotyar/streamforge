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
    ITableHistoryFacade history)
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

        for (var iteration = 0; iteration < MaxToolIterations; iteration++)
        {
            var candidate = await CallGeminiAsync(contents, ct).ConfigureAwait(false);
            var parts = candidate.Content?.Parts ?? [];
            var functionCalls = parts.Where(p => p.FunctionCall is not null).ToList();

            if (functionCalls.Count == 0)
            {
                var reply = string.Concat(parts.Where(p => p.Text is not null).Select(p => p.Text));
                return new ChatResponse(reply, toolCalls, model);
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
            model);
    }

    private async Task<GeminiCandidate> CallGeminiAsync(List<GeminiContent> contents, CancellationToken ct)
    {
        var body = new GeminiGenerateContentRequest
        {
            SystemInstruction = new GeminiContent { Parts = [new GeminiPart { Text = SystemPrompt }] },
            Contents = contents,
            Tools = [new GeminiTool { FunctionDeclarations = ChatToolCatalog.Declarations.ToList() }],
            GenerationConfig = new GeminiGenerationConfig { MaxOutputTokens = 2048 },
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

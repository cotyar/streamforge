package streamforge.consumer;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * StreamForge — polyglot pub/sub reach (plan 005, wave W8-B).
 *
 * <p>A minimal standalone HTTP server — {@code com.sun.net.httpserver.HttpServer} (JDK stdlib) plus
 * one small JSON dependency (Gson, see build.gradle) — that runs behind its own Dapr sidecar and
 * subscribes to two of the platform's frozen envelope topics (dapr/POLYGLOT.md):
 *
 * <ul>
 *   <li>{@code sf-table-delta}  -&gt; TableDeltaEnvelope {table, seq, deltas:[{row, weight}]}</li>
 *   <li>{@code sf-pipeline-out} -&gt; PipelineResultsEnvelope {pipelineId, results:[{pipelineId, seq,
 *       timestampMs, row}]}</li>
 * </ul>
 *
 * <p>The sidecar discovers subscriptions via {@code GET /dapr/subscribe} and POSTs each message,
 * CloudEvents-wrapped, to the declared route. This process unwraps {@code .data}, prints a compact
 * line per message, and always responds 200 — a non-2xx is exactly the signal that triggers Dapr's
 * at-least-once redelivery, so a malformed/unparseable payload is logged and dropped instead
 * (mirrors the .NET host's own poison-message handling documented in dapr/POLYGLOT.md).
 */
public final class Main {

    // ANSI colors — zero deps, matches the sibling ts-consumer's compact colored-line convention.
    private static final String RESET = "[0m";
    private static final String CYAN = "[36m";
    private static final String MAGENTA = "[35m";
    private static final String GREEN = "[32m";
    private static final String RED = "[31m";
    private static final String YELLOW = "[33m";
    private static final String GRAY = "[90m";

    private static final Gson GSON = new Gson();

    private static final AtomicLong tableDeltaMessages = new AtomicLong();
    private static final AtomicLong tableDeltaRows = new AtomicLong();
    private static final AtomicLong pipelineMessages = new AtomicLong();
    private static final AtomicLong pipelineResults = new AtomicLong();

    public static void main(String[] args) throws IOException {
        int appPort = intEnv("APP_PORT", intEnv("PORT", 8599));
        int daprHttpPort = intEnv("DAPR_HTTP_PORT", 4099);
        int daprGrpcPort = intEnv("DAPR_GRPC_PORT", 5099);
        String pubsubName = System.getenv().getOrDefault("PUBSUB_NAME", "pubsub");

        HttpServer server = HttpServer.create(new InetSocketAddress(appPort), 0);
        server.createContext("/dapr/subscribe", subscribeHandler(pubsubName));
        server.createContext("/healthz", healthHandler());
        server.createContext("/sf-table-delta", topicHandler("/sf-table-delta"));
        server.createContext("/sf-pipeline-out", topicHandler("/sf-pipeline-out"));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        ScheduledExecutorService counterTimer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "counter-printer");
            t.setDaemon(true);
            return t;
        });
        counterTimer.scheduleAtFixedRate(Main::printCounters, 10, 10, TimeUnit.SECONDS);

        System.out.printf(
                "sf-java-consumer listening on :%d (sidecar http :%d, grpc :%d, pubsub \"%s\")%n",
                appPort, daprHttpPort, daprGrpcPort, pubsubName);
        System.out.println(GRAY + "subscriptions: sf-table-delta, sf-pipeline-out" + RESET);
    }

    private static int intEnv(String name, int fallback) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) return fallback;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static HttpHandler subscribeHandler(String pubsubName) {
        return exchange -> {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, "{}");
                return;
            }
            JsonArray subs = new JsonArray();
            subs.add(subscription(pubsubName, "sf-table-delta", "/sf-table-delta"));
            subs.add(subscription(pubsubName, "sf-pipeline-out", "/sf-pipeline-out"));
            sendJson(exchange, 200, GSON.toJson(subs));
        };
    }

    private static JsonObject subscription(String pubsubName, String topic, String route) {
        JsonObject o = new JsonObject();
        o.addProperty("pubsubname", pubsubName);
        o.addProperty("topic", topic);
        o.addProperty("route", route);
        return o;
    }

    private static HttpHandler healthHandler() {
        return exchange -> sendJson(exchange, 200, "{\"status\":\"ok\"}");
    }

    private static HttpHandler topicHandler(String path) {
        return exchange -> {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, "{}");
                return;
            }
            try {
                String body = readBody(exchange.getRequestBody());
                JsonElement parsed = JsonParser.parseString(body);
                JsonElement data = extractData(parsed);
                if ("/sf-table-delta".equals(path)) {
                    handleTableDelta(data);
                } else {
                    handlePipelineOut(data);
                }
            } catch (Exception e) {
                // Always 200 even on malformed/unparseable JSON — see class doc: a non-2xx here
                // triggers Dapr's at-least-once redelivery, and a permanently-malformed message
                // would otherwise retry forever.
                System.out.println(YELLOW + path + ": request body was not valid JSON, dropped (" + e.getMessage() + ")" + RESET);
            }
            // Always ack 200 regardless of parse outcome (poison-message loop protection).
            sendJson(exchange, 200, "{\"status\":\"SUCCESS\"}");
        };
    }

    /** Unwraps a Dapr CloudEvents-wrapped body's {@code .data} (or base64-decodes {@code
     * .data_base64} for a raw/binary publish). Falls back to treating the whole body as the
     * payload if it isn't CloudEvents-shaped. */
    private static JsonElement extractData(JsonElement body) {
        if (body == null || !body.isJsonObject()) return body;
        JsonObject obj = body.getAsJsonObject();
        if (obj.has("data")) return obj.get("data");
        if (obj.has("data_base64") && obj.get("data_base64").isJsonPrimitive()) {
            byte[] decoded = Base64.getDecoder().decode(obj.get("data_base64").getAsString());
            return JsonParser.parseString(new String(decoded, StandardCharsets.UTF_8));
        }
        return obj;
    }

    /** Case-tolerant field lookup: dapr/POLYGLOT.md documents camelCase as canonical but PascalCase
     * as also-accepted (ASP.NET Core's PropertyNameCaseInsensitive default), so this sample mirrors
     * that same tolerance rather than hard-coding one casing. */
    private static JsonElement field(JsonObject o, String camel, String pascal) {
        if (o.has(camel)) return o.get(camel);
        if (o.has(pascal)) return o.get(pascal);
        return null;
    }

    private static void handleTableDelta(JsonElement data) {
        if (data == null || !data.isJsonObject()) {
            warnMalformed("/sf-table-delta", data);
            return;
        }
        JsonObject env = data.getAsJsonObject();
        JsonElement table = field(env, "table", "Table");
        JsonElement seq = field(env, "seq", "Seq");
        JsonElement deltasEl = field(env, "deltas", "Deltas");
        if (table == null || deltasEl == null || !deltasEl.isJsonArray()) {
            warnMalformed("/sf-table-delta", data);
            return;
        }
        JsonArray deltas = deltasEl.getAsJsonArray();
        tableDeltaMessages.incrementAndGet();
        tableDeltaRows.addAndGet(deltas.size());

        StringBuilder rows = new StringBuilder();
        for (JsonElement el : deltas) {
            if (rows.length() > 0) rows.append("  ");
            JsonObject d = el.getAsJsonObject();
            JsonElement weight = field(d, "weight", "Weight");
            JsonElement row = field(d, "row", "Row");
            long w = weight != null ? weight.getAsLong() : 0;
            String glyph = w >= 0 ? GREEN + "+" + w + RESET : RED + w + RESET;
            rows.append(glyph).append(" ").append(row != null ? row.toString() : "{}");
        }
        System.out.println(CYAN + "[sf-table-delta]" + RESET + " " + table.getAsString() + "#" + (seq != null ? seq.getAsString() : "?") + "  " + rows);
    }

    private static void handlePipelineOut(JsonElement data) {
        if (data == null || !data.isJsonObject()) {
            warnMalformed("/sf-pipeline-out", data);
            return;
        }
        JsonObject env = data.getAsJsonObject();
        JsonElement pipelineId = field(env, "pipelineId", "PipelineId");
        JsonElement resultsEl = field(env, "results", "Results");
        if (pipelineId == null || resultsEl == null || !resultsEl.isJsonArray()) {
            warnMalformed("/sf-pipeline-out", data);
            return;
        }
        JsonArray results = resultsEl.getAsJsonArray();
        pipelineMessages.incrementAndGet();
        pipelineResults.addAndGet(results.size());

        StringBuilder summary = new StringBuilder();
        if (results.isEmpty()) {
            summary.append("(no results)");
        } else {
            for (JsonElement el : results) {
                if (summary.length() > 0) summary.append("  ");
                JsonObject r = el.getAsJsonObject();
                JsonElement seq = field(r, "seq", "Seq");
                JsonElement ts = field(r, "timestampMs", "TimestampMs");
                JsonElement row = field(r, "row", "Row");
                summary.append("seq=").append(seq).append(" ts=").append(ts).append(" row=").append(row != null ? row.toString() : "{}");
            }
        }
        System.out.println(MAGENTA + "[sf-pipeline-out]" + RESET + " " + pipelineId.getAsString() + "  " + results.size() + " result(s): " + summary);
    }

    private static void warnMalformed(String topic, JsonElement data) {
        System.out.println(YELLOW + topic + ": malformed payload, dropped: " + (data == null ? "null" : data.toString()) + RESET);
    }

    private static void printCounters() {
        System.out.println(GRAY + "[" + Instant.now() + "] counters: table-delta msgs=" + tableDeltaMessages.get()
                + " rows=" + tableDeltaRows.get() + " | pipeline-out msgs=" + pipelineMessages.get()
                + " results=" + pipelineResults.get() + RESET);
    }

    private static String readBody(InputStream in) throws IOException {
        return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void sendJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private Main() {
    }
}

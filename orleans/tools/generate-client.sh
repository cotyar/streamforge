#!/usr/bin/env bash
#
# generate-client.sh — StreamsForge Tier-3 typed client codegen.
#
# Downloads the self-contained .proto for one StreamsForge source/pipeline/table and scaffolds a
# ready-to-build .NET client library project around it: a .csproj that compiles the downloaded .proto
# with Grpc.Tools (GrpcServices=Client), and a small StreamsForgeClient.cs convenience wrapper that logs
# in, opens the DynamicStreamService gRPC stream, and yields TYPED messages (parsed straight out of
# DynamicFrame.payload with the generated {Entity}Event/{Entity}Delta Parser). The script finishes by
# running `dotnet build` on the generated project and reporting success.
#
# Usage:
#   ./tools/generate-client.sh <entity-kind> <entity-id-or-name> [options]
#
#   <entity-kind>          One of: source, pipeline, table
#   <entity-id-or-name>    Source name (sources are keyed by name), or pipeline/table id
#
# Options:
#   --server <url>    StreamsForge REST base URL (default: http://localhost:5199)
#   --out <dir>        Output directory for the generated project (default: ./<entity-kind>-<entity-id>-client)
#   --user <username>  Login username (default: editor)
#   --pass <password>  Login password (default: editor123!)
#   -h, --help          Show this help and exit
#
# Example:
#   ./tools/generate-client.sh table gold_tier_orders_id --server http://localhost:7199 \
#       --user editor --pass 'editor123!' --out /tmp/gold-tier-client

set -euo pipefail

usage() {
  sed -n '2,27p' "$0" | sed 's/^# \{0,1\}//'
}

if [ $# -lt 1 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  usage
  exit 0
fi

if [ $# -lt 2 ]; then
  echo "error: expected <entity-kind> and <entity-id-or-name>" >&2
  usage
  exit 1
fi

ENTITY_KIND="$1"; shift
ENTITY_ID="$1"; shift

SERVER="http://localhost:5199"
OUT_DIR=""
USERNAME="editor"
PASSWORD="editor123!"

while [ $# -gt 0 ]; do
  case "$1" in
    --server) SERVER="$2"; shift 2 ;;
    --out) OUT_DIR="$2"; shift 2 ;;
    --user) USERNAME="$2"; shift 2 ;;
    --pass) PASSWORD="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

case "$ENTITY_KIND" in
  source) ROUTE="sources" ;;
  pipeline) ROUTE="pipelines" ;;
  table) ROUTE="tables" ;;
  *)
    echo "error: <entity-kind> must be one of: source, pipeline, table (got '${ENTITY_KIND}')" >&2
    exit 1
    ;;
esac

if [ -z "$OUT_DIR" ]; then
  OUT_DIR="./${ENTITY_KIND}-${ENTITY_ID}-client"
fi

# dotnet isn't on PATH in the StreamsForge dev environment — prefer the well-known location, falling
# back to PATH so this script also works wherever dotnet IS on PATH (e.g. most CI images).
if command -v dotnet >/dev/null 2>&1; then
  DOTNET="dotnet"
elif [ -x "$HOME/.dotnet/dotnet" ]; then
  DOTNET="$HOME/.dotnet/dotnet"
else
  echo "error: dotnet not found on PATH or at \$HOME/.dotnet/dotnet" >&2
  exit 1
fi

echo "==> Logging in to ${SERVER} as ${USERNAME}"
LOGIN_RESPONSE="$(curl -sS -f -X POST "${SERVER}/api/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"${USERNAME}\",\"password\":\"${PASSWORD}\"}")" || {
  echo "error: login request failed against ${SERVER}/api/auth/login" >&2
  exit 1
}

TOKEN="$(printf '%s' "$LOGIN_RESPONSE" | grep -o '"token"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | sed -E 's/.*:[[:space:]]*"([^"]*)"/\1/')"
if [ -z "$TOKEN" ]; then
  echo "error: login response had no token field: ${LOGIN_RESPONSE}" >&2
  exit 1
fi
echo "==> Logged in."

mkdir -p "$OUT_DIR"
PROTO_PATH="${OUT_DIR}/entity.proto"

echo "==> Downloading .proto for ${ENTITY_KIND} '${ENTITY_ID}'"
PROTO_URL="${SERVER}/api/${ROUTE}/${ENTITY_ID}/proto"
HTTP_STATUS="$(curl -sS -w '%{http_code}' -o "$PROTO_PATH" \
  -H "Authorization: Bearer ${TOKEN}" "$PROTO_URL")"

if [ "$HTTP_STATUS" != "200" ]; then
  echo "error: GET ${PROTO_URL} returned HTTP ${HTTP_STATUS}:" >&2
  cat "$PROTO_PATH" >&2 || true
  rm -f "$PROTO_PATH"
  exit 1
fi
echo "==> Saved $(wc -l < "$PROTO_PATH" | tr -d ' ') lines to ${PROTO_PATH}"

# Every StreamsForge dynamic entity gets both an {Entity}Event and an {Entity}Delta message (see
# DescriptorFactory) regardless of entity kind, so both message names are always present in the
# download — pull the real generated names straight out of the proto text instead of re-deriving
# PascalCase ourselves (which would risk drifting out of sync with the server's own naming rules).
EVENT_TYPE="$(grep -oE 'message [A-Za-z0-9_]+Event \{' "$PROTO_PATH" | head -1 | awk '{print $2}')"
DELTA_TYPE="$(grep -oE 'message [A-Za-z0-9_]+Delta \{' "$PROTO_PATH" | head -1 | awk '{print $2}')"

if [ -z "$EVENT_TYPE" ] || [ -z "$DELTA_TYPE" ]; then
  echo "error: could not find {Entity}Event/{Entity}Delta message names in ${PROTO_PATH}" >&2
  exit 1
fi

# Streaming contract: DynamicFrame.payload carries {Entity}Event bytes for sources/pipelines, and
# {Entity}Delta bytes for tables (Z-set weight semantics only make sense for a materialized table).
case "$ENTITY_KIND" in
  source|pipeline) PAYLOAD_TYPE="$EVENT_TYPE" ;;
  table) PAYLOAD_TYPE="$DELTA_TYPE" ;;
esac

# entity_key format matches EntitySchemas.SourceKey/PipelineKey/TableKey on the server
# ("source:{name}" / "pipeline:{id}" / "table:{id}") — must agree exactly, it's the subscribe key.
ENTITY_KEY="${ENTITY_KIND}:${ENTITY_ID}"

# C# namespace protoc generates for `package streamsforge.dynamic.v1;`: each dot-segment gets its
# first letter capitalized ("streamsforge" -> "Streamsforge", "v1" -> "V1"). Derived from the actual
# package line rather than hardcoded, so this keeps working if DescriptorFactory.PackageName ever changes.
PROTO_PACKAGE="$(grep -m1 '^package ' "$PROTO_PATH" | sed -E 's/^package ([^;]+);.*/\1/')"
CS_NAMESPACE=""
IFS='.' read -ra PKG_PARTS <<< "$PROTO_PACKAGE"
for part in "${PKG_PARTS[@]}"; do
  first_upper="$(printf '%s' "${part:0:1}" | tr '[:lower:]' '[:upper:]')"
  CS_NAMESPACE="${CS_NAMESPACE}${first_upper}${part:1}."
done
CS_NAMESPACE="${CS_NAMESPACE%.}" # trim trailing dot

echo "==> Entity: ${ENTITY_KIND} '${ENTITY_ID}' (entity_key=${ENTITY_KEY})"
echo "==> Typed payload: ${CS_NAMESPACE}.${PAYLOAD_TYPE}"

PROJECT_NAME="GeneratedClient"

echo "==> Writing project files to ${OUT_DIR}"

cat > "${OUT_DIR}/${PROJECT_NAME}.csproj" <<EOF
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Library</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <LangVersion>latest</LangVersion>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Google.Protobuf" Version="3.31.1" />
    <PackageReference Include="Grpc.Net.Client" Version="2.80.0" />
    <PackageReference Include="Grpc.Tools" Version="2.80.0" PrivateAssets="All" />
  </ItemGroup>
  <ItemGroup>
    <Protobuf Include="entity.proto" GrpcServices="Client" />
  </ItemGroup>
</Project>
EOF

cat > "${OUT_DIR}/StreamsForgeClient.cs" <<EOF
// Generated by tools/generate-client.sh for ${ENTITY_KIND} "${ENTITY_ID}". Do not edit by hand —
// re-run the script if the entity's schema changes.
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using ${CS_NAMESPACE};

namespace GeneratedClient;

/// <summary>Thin convenience wrapper around a StreamsForge REST login and a typed
/// DynamicStreamService.SubscribeEntity gRPC stream for THIS entity
/// (${ENTITY_KIND} "${ENTITY_ID}", entity_key "${ENTITY_KEY}").</summary>
public static class StreamsForgeClient
{
    private sealed record LoginRequest(string Username, string Password);
    private sealed record LoginResponse(string Token, string Username, string DisplayName, string Role);

    /// <summary>POSTs to /api/auth/login and returns the JWT to use as a Bearer token, both for REST
    /// calls and (as gRPC call metadata) for DynamicStreamService.</summary>
    public static async Task<string> LoginAsync(string httpBaseUrl, string username, string password, CancellationToken cancellationToken = default)
    {
        using var http = new HttpClient { BaseAddress = new Uri(httpBaseUrl) };
        using var response = await http.PostAsJsonAsync("/api/auth/login", new LoginRequest(username, password), cancellationToken);
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadFromJsonAsync<LoginResponse>(cancellationToken)
            ?? throw new InvalidOperationException("Login response body was empty.");
        return body.Token;
    }

    /// <summary>Opens DynamicStreamService.SubscribeEntity for <paramref name="entityKey"/> and yields
    /// TYPED messages, parsed out of each DynamicFrame.payload with <paramref name="parser"/>. Generic
    /// over the payload type so it works for both {Entity}Event (sources/pipelines) and {Entity}Delta
    /// (tables) — callers pass the matching generated Parser (see SubscribeAsync below for THIS
    /// entity's own typed convenience call).</summary>
    public static async IAsyncEnumerable<T> SubscribeTypedAsync<T>(
        string grpcBaseUrl,
        string jwt,
        string entityKey,
        MessageParser<T> parser,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : IMessage<T>
    {
        // StreamsForge's gRPC endpoint is cleartext h2c (HTTP/2 without TLS) — SocketsHttpHandler
        // requires this switch to allow HTTP/2 over an unencrypted channel.
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

        using var channel = GrpcChannel.ForAddress(grpcBaseUrl);
        var client = new DynamicStreamService.DynamicStreamServiceClient(channel);

        var headers = new Metadata { { "Authorization", \$"Bearer {jwt}" } };
        using var call = client.SubscribeEntity(
            new EntitySubscribeRequest { EntityKey = entityKey },
            headers: headers,
            cancellationToken: cancellationToken);

        await foreach (var frame in call.ResponseStream.ReadAllAsync(cancellationToken))
        {
            yield return parser.ParseFrom(frame.Payload);
        }
    }

    /// <summary>Typed convenience call for THIS entity: subscribes to entity_key "${ENTITY_KEY}" and
    /// yields ${PAYLOAD_TYPE} instances (parsed from DynamicFrame.payload with ${PAYLOAD_TYPE}.Parser).</summary>
    public static IAsyncEnumerable<${PAYLOAD_TYPE}> SubscribeAsync(string grpcBaseUrl, string jwt, CancellationToken cancellationToken = default) =>
        SubscribeTypedAsync(grpcBaseUrl, jwt, "${ENTITY_KEY}", ${PAYLOAD_TYPE}.Parser, cancellationToken);
}
EOF

echo "==> Building generated client project"
"$DOTNET" build "${OUT_DIR}/${PROJECT_NAME}.csproj" --nologo

echo "==> Success: typed client library for ${ENTITY_KIND} '${ENTITY_ID}' built at ${OUT_DIR}"
echo "    Payload type: ${CS_NAMESPACE}.${PAYLOAD_TYPE}"
echo "    Subscribe with: StreamsForgeClient.SubscribeAsync(grpcBaseUrl, jwt)"

#!/usr/bin/env bash
# Regenerates src/streamforge/_pb/ from the engine's streamforge.proto. The generated files are
# committed (so `pip install` needs no codegen step / no dotnet toolchain), but re-run this
# whenever streamforge.proto changes.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="$ROOT/../../orleans/src/StreamForge.Host/Protos"
OUT_DIR="$ROOT/src/streamforge/_pb"

if [[ ! -f "$PROTO_DIR/streamforge.proto" ]]; then
  echo "streamforge.proto not found at $PROTO_DIR" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

python -m grpc_tools.protoc \
  -I "$PROTO_DIR" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$PROTO_DIR/streamforge.proto"

# grpc_tools.protoc emits a flat top-level `import streamforge_pb2 as streamforge__pb2` in the
# _grpc.py stub, which breaks once the file lives inside a package (streamforge._pb). Rewrite it
# to a relative import -- the standard fix for this well-known protoc/grpc_python_plugin gap.
sed -i.bak 's/^import streamforge_pb2 as streamforge__pb2$/from . import streamforge_pb2 as streamforge__pb2/' \
  "$OUT_DIR/streamforge_pb2_grpc.py"
rm -f "$OUT_DIR/streamforge_pb2_grpc.py.bak"

touch "$OUT_DIR/__init__.py"

echo "generated into $OUT_DIR"

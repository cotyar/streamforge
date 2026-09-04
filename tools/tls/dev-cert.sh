#!/usr/bin/env bash
# Generate a self-signed development certificate for a StreamsForge host's TLS listeners.
#
#   tools/tls/dev-cert.sh <out-dir> [host-or-ip ...]
#
# Writes <out-dir>/cert.pem and <out-dir>/key.pem (RSA 2048, 10 years, CN=localhost) and prints the
# two configuration arguments to hand the host. Idempotent: an existing pair in <out-dir> is
# overwritten.
#
# SANs: DNS:localhost and IP:127.0.0.1 are always present; every extra argument is added as an
# IP: entry when it looks like a bare IPv4/IPv6 literal and a DNS: entry otherwise. A modern TLS
# client ignores CN entirely and matches the SAN list, so a name that is not in it will fail with a
# name mismatch no matter what CN says — add the hostname you will actually dial.
#
# This certificate is its OWN root: it is self-signed, so trusting it means trusting exactly this
# certificate. That is why a client on another machine gets it as `--Tls:TrustedCaPath cert.pem`
# (the file works as both leaf and trust anchor) rather than needing a separate CA file. It is a
# DEVELOPMENT convenience — a real deployment uses a certificate from a CA the clients already
# trust, or a private CA whose root is distributed on purpose.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <out-dir> [host-or-ip ...]" >&2
  exit 2
fi

out_dir="$1"
shift

command -v openssl >/dev/null 2>&1 || { echo "error: openssl not found on PATH" >&2; exit 3; }

mkdir -p "$out_dir"
# Resolve after mkdir so a relative out-dir prints as an absolute path in the hint below.
out_dir="$(cd "$out_dir" && pwd)"

cert="$out_dir/cert.pem"
key="$out_dir/key.pem"

# Always-present SANs, then the caller's extras. An argument matching a bare IPv4 dotted quad or
# containing a ':' (IPv6) becomes an IP: entry — an IP address placed in a DNS: entry does not match
# when a client dials that address, which is precisely the 127.0.0.1 case these tests live on.
sans="DNS:localhost,IP:127.0.0.1"
for extra in "$@"; do
  [[ -z "$extra" ]] && continue
  if [[ "$extra" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ || "$extra" == *:* ]]; then
    sans="$sans,IP:$extra"
  else
    sans="$sans,DNS:$extra"
  fi
done

openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
  -keyout "$key" -out "$cert" \
  -subj "/CN=localhost" \
  -addext "subjectAltName=$sans" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign" \
  -addext "extendedKeyUsage=serverAuth" \
  >/dev/null 2>&1

chmod 600 "$key"
chmod 644 "$cert"

echo "wrote $cert"
echo "wrote $key  (mode 600)"
echo "SANs: $sans"
echo
echo "Run a host with TLS on both ports:"
echo "  --Tls:Enabled true --Kestrel:Certificates:Default:Path $cert --Kestrel:Certificates:Default:KeyPath $key"
echo
echo "Point another StreamsForge instance (or any outbound caller) at it:"
echo "  --Tls:TrustedCaPath $cert"

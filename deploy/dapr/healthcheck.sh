#!/usr/bin/env bash
# Plan 007 W1B: docker HEALTHCHECK for the app container. mcr.microsoft.com/dotnet/aspnet:10.0 (Ubuntu
# 24.04, distroless-ish — no curl, no wget) does have bash, so we use bash's built-in /dev/tcp
# pseudo-device to speak raw HTTP instead of installing a client just for this.
set -euo pipefail

port="${PORT:-8080}"

exec 3<>"/dev/tcp/127.0.0.1/${port}"
printf 'GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n' >&3
read -r status_line <&3
exec 3<&-
exec 3>&-

[[ "$status_line" == *" 200 "* ]]

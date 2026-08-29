#!/usr/bin/env bash
set -euo pipefail

app="${1:?usage: launch_macos_app_stdio.sh <application.app>}"
test "$(uname -s)" = "Darwin" || {
  echo "LaunchServices stdio relay requires macOS" >&2
  exit 2
}
test -d "$app" || {
  echo "application bundle is missing: $app" >&2
  exit 2
}

relay_root="$(mktemp -d "${TMPDIR:-/tmp}/openagent-ls-stdio.XXXXXX")"
to_app="$relay_root/stdin.fifo"
from_app="$relay_root/stdout.fifo"
app_stderr="$relay_root/stderr.log"
stdin_relay=""
stdout_relay=""

cleanup() {
  if [[ -n "$stdin_relay" ]]; then kill "$stdin_relay" 2>/dev/null || true; fi
  if [[ -n "$stdout_relay" ]]; then kill "$stdout_relay" 2>/dev/null || true; fi
  rm -rf "$relay_root"
}
trap cleanup EXIT INT TERM

mkfifo "$to_app" "$from_app"
: >"$app_stderr"

# Hold both FIFOs open across the LaunchServices hand-off. Without these
# keepalive descriptors, the stdout reader can observe a transient EOF after
# ``open`` closes its writer but before launchd installs the descriptor in the
# app, causing the MCP initialize response to hit a closed pipe.
exec 3<>"$to_app"
exec 4<>"$from_app"
exec 5<&0
exec 6>&1

# LaunchServices accepts filesystem paths, not inherited file descriptors.
# These relays preserve the caller's MCP stdio while allowing launchd to open
# the signed application's FIFO endpoints independently.
# Explicit descriptor duplication is required: non-interactive bash otherwise
# gives an asynchronous command /dev/null as stdin before it can relay MCP.
cat <&5 >"$to_app" &
stdin_relay=$!
cat "$from_app" >&6 &
stdout_relay=$!
exec 5<&-
exec 6>&-

set +e
/usr/bin/open -n -g \
  --stdin "$to_app" \
  --stdout "$from_app" \
  --stderr "$app_stderr" \
  -a "$app"
open_status=$?
set -e
exec 3>&-
exec 4>&-

# ``open -W`` cannot wait for LSBackgroundOnly applications. Keep this wrapper
# alive through the app's stdout FIFO instead; it closes only when the signed
# helper exits. On a launch failure both FIFO relays are torn down immediately.
if [[ "$open_status" -eq 0 ]]; then
  wait "$stdout_relay" 2>/dev/null || true
else
  kill "$stdout_relay" 2>/dev/null || true
  wait "$stdout_relay" 2>/dev/null || true
fi
kill "$stdin_relay" 2>/dev/null || true
wait "$stdin_relay" 2>/dev/null || true

if [[ -s "$app_stderr" ]]; then
  sed 's/^/[computer-control] /' "$app_stderr" >&2
fi
exit "$open_status"

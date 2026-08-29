#!/usr/bin/env bash
set -euo pipefail

archive="${1:?usage: verify_macos_archive.sh <openagent-host-tools-darwin-*.tar.gz>}"
script_dir="$(cd "$(dirname "$0")" && pwd)"
archive="$(cd "$(dirname "$archive")" && pwd)/$(basename "$archive")"
archive_name="$(basename "$archive")"
case "$archive_name" in
  openagent-host-tools-darwin-arm64.tar.gz)
    platform="darwin-arm64"
    ;;
  openagent-host-tools-darwin-x64.tar.gz)
    platform="darwin-x64"
    ;;
  *)
    echo "expected a canonical macOS host-tools archive, got: $archive_name" >&2
    exit 1
    ;;
esac

test "$(uname -s)" = "Darwin" || {
  echo "macOS archive signature verification must run on macOS" >&2
  exit 1
}
test -n "${APPLE_TEAM_ID:-}" || {
  echo "APPLE_TEAM_ID is required to verify the release signer" >&2
  exit 1
}
python_bin="${PYTHON:-python3}"
command -v "$python_bin" >/dev/null || {
  echo "Python is required to verify the release archive" >&2
  exit 1
}

temp_base="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
work_root="$(mktemp -d "$temp_base/openagent-host-tools-verify.XXXXXX")"
cleanup() {
  rm -rf "$work_root"
}
trap cleanup EXIT
extract_root="$work_root/extracted"
mkdir "$extract_root"

"$python_bin" "$script_dir/verify_bundle_archive.py" \
  "$archive" "$extract_root" --expect-platform "$platform" >/dev/null
bundle="$extract_root/$platform"
host="$bundle/openagent-host-tools"
node="$bundle/node"
helper="$bundle/openagent-computer-control.app"
helper_executable="$helper/Contents/MacOS/openagent-computer-control"

test -x "$host"
test -x "$node"
test -x "$helper_executable"
test "$(/usr/libexec/PlistBuddy -c 'Print :CFBundleIdentifier' "$helper/Contents/Info.plist")" = \
  "com.openagent.computer-control"

codesign --verify --strict --verbose=2 "$host"
codesign --verify --strict --verbose=2 "$node"
codesign --verify --strict --verbose=2 "$helper_executable"
codesign --verify --deep --strict --verbose=2 "$helper"

verify_identity() {
  local target="$1"
  local expected_identifier="$2"
  local metadata
  local actual_identifier
  local actual_team
  metadata="$(codesign -d --verbose=4 "$target" 2>&1)"
  actual_identifier="$(printf '%s\n' "$metadata" | sed -n 's/^Identifier=//p')"
  actual_team="$(printf '%s\n' "$metadata" | sed -n 's/^TeamIdentifier=//p')"
  test "$actual_identifier" = "$expected_identifier" || {
    echo "unexpected signing identifier $actual_identifier for $target" >&2
    exit 1
  }
  test "$actual_team" = "$APPLE_TEAM_ID" || {
    echo "unexpected TeamIdentifier $actual_team for $target" >&2
    exit 1
  }
}

verify_identity "$host" "com.openagent.host-tools"
verify_identity "$node" "com.openagent.host-tools.node"
verify_identity "$helper_executable" "com.openagent.computer-control"
verify_identity "$helper" "com.openagent.computer-control"

xcrun stapler validate "$helper"
spctl --assess --type execute --verbose=4 "$helper"
"$node" -e "process.stdout.write('node-ok')" | grep -qx node-ok

# Exercise the opposite TCC state after the granted smoke performed before
# packaging. Reset only this helper's stable code-signing identity; never rely
# on the mutable permission state inherited from a hosted runner image.
tccutil reset Accessibility com.openagent.computer-control
tccutil reset ScreenCapture com.openagent.computer-control
sleep 1

# This proves the final, extracted helper starts as the real MCP and fails with
# the stable permission error instead of crashing, hanging, or bypassing macOS
# privacy controls.
"$python_bin" "$script_dir/smoke_bundle.py" "$bundle" \
  --core-only --computer-control expect-denied --macos-launchservices

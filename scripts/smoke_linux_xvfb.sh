#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <bundle-directory>" >&2
  exit 2
fi

bundle="$1"
python_bin="${PYTHON:-python}"
script_dir="$(cd "$(dirname "$0")" && pwd)"

# enigo/x11rb needs an unused X11 keycode when constructing its input
# connection.  Hosted Xvfb images commonly ship with every keycode mapped,
# so reserve the final keycode explicitly before starting the real sidecar.
xdpyinfo >/dev/null
xmodmap -e 'keycode 255 ='

"$python_bin" "$script_dir/smoke_bundle.py" "$bundle" \
  --computer-control expect-granted

#!/usr/bin/env python3
"""Regenerate the checked-in five-builtin public manifest lock."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from openagent_host_tools.builtins import EditorServer, FilesystemServer, ShellServer
from openagent_host_tools.manifests import build_manifest_lock
from openagent_host_tools.sidecars import AGENT_IN_CHROME_MANIFEST, COMPUTER_CONTROL_MANIFEST

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    cwd = ROOT.resolve()
    platforms = (
        "darwin-arm64",
        "darwin-x64",
        "linux-arm64",
        "linux-x64",
        "win32-arm64",
        "win32-x64",
    )
    core = [FilesystemServer(cwd), EditorServer(cwd), ShellServer(cwd)]
    manifests = [
        *(
            replace(
                server.manifest,
                platforms=platforms,
                os_requirements=("Runs with the signed-in user's OS permissions",),
            )
            for server in core
        ),
        COMPUTER_CONTROL_MANIFEST,
        AGENT_IN_CHROME_MANIFEST,
    ]
    output = ROOT / "src" / "openagent_host_tools" / "builtins-lock.json"
    output.write_text(
        json.dumps(build_manifest_lock(manifests), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()

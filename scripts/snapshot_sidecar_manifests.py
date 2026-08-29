#!/usr/bin/env python3
"""Probe the real sidecars and snapshot their public MCP catalogs.

Run this whenever either host-owned sidecar changes. Release CI runs it in
``--check`` mode against the native bundle so a stale placeholder or lock can
never be published.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
from pathlib import Path

from openagent_host_tools.config import PluginSpec
from openagent_host_tools.manifests import contract_payload, manifest_sha256
from openagent_host_tools.mcp_stdio import MCPStdioServer
from openagent_host_tools.sidecars import (
    AGENT_IN_CHROME_MANIFEST,
    COMPUTER_CONTROL_MANIFEST,
)

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = ROOT / "src" / "openagent_host_tools" / "sidecar-manifests.json"


def _commands(bundle: Path) -> dict[str, tuple[str, ...]]:
    suffix = ".exe" if os.name == "nt" else ""
    node_name = "node.exe" if os.name == "nt" else "node"
    node = bundle / node_name
    if not node.is_file():
        found = shutil.which("node")
        if found is None:
            raise SystemExit("Node.js is required to probe agent-in-chrome")
        node = Path(found)
    computer = bundle / f"openagent-computer-control{suffix}"
    app_computer = (
        bundle
        / "openagent-computer-control.app"
        / "Contents"
        / "MacOS"
        / "openagent-computer-control"
    )
    if app_computer.is_file():
        computer = app_computer
    return {
        "computer-control": (str(computer),),
        "agent-in-chrome": (
            str(node),
            str(bundle / "agent-in-chrome" / "host" / "mcp-server.js"),
        ),
    }


async def _probe(bundle: Path) -> dict[str, dict]:
    bases = {
        "computer-control": COMPUTER_CONTROL_MANIFEST,
        "agent-in-chrome": AGENT_IN_CHROME_MANIFEST,
    }
    result: dict[str, dict] = {}
    for name, command in _commands(bundle).items():
        if not Path(command[-1]).is_file():
            raise SystemExit(f"missing {name} sidecar: {command[-1]}")
        adapter = MCPStdioServer(PluginSpec(name, command), placeholder=bases[name])
        try:
            await adapter.start()
            initialized = adapter.raw_initialize or {}
            server_info = initialized.get("serverInfo") or {}
            expected = bases[name]
            if server_info.get("name") != expected.name:
                raise SystemExit(
                    f"{name} raw serverInfo.name mismatch: {server_info.get('name')!r}"
                )
            if server_info.get("version") != expected.version:
                raise SystemExit(
                    f"{name} raw serverInfo.version mismatch: "
                    f"{server_info.get('version')!r} != {expected.version!r}"
                )
            if initialized.get("instructions") != expected.instructions:
                raise SystemExit(f"{name} raw initialize instructions mismatch")
            result[name] = contract_payload(adapter.manifest)
        finally:
            await adapter.close()
    return result


def _canonical(value: dict) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    captured = asyncio.run(_probe(args.bundle.resolve()))
    if args.check:
        expected = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
        if captured != expected:
            for name in sorted(set(captured) | set(expected)):
                if captured.get(name) != expected.get(name):
                    actual_hash = (
                        manifest_sha256(_manifest_for_hash(captured[name]))
                        if name in captured
                        else "missing"
                    )
                    expected_hash = (
                        manifest_sha256(_manifest_for_hash(expected[name]))
                        if name in expected
                        else "missing"
                    )
                    print(f"{name}: expected {expected_hash}, live {actual_hash}")
            raise SystemExit("sidecar MCP catalog drift; regenerate the snapshot and lock")
        return
    SNAPSHOT.write_text(_canonical(captured), encoding="utf-8")


def _manifest_for_hash(raw: dict):
    # Local import avoids exposing a second public deserializer solely for the
    # human-readable check failure.
    from openagent_host_tools.types import HostMcpManifest, ToolClassification, ToolManifest

    return HostMcpManifest(
        name=str(raw["name"]),
        version=str(raw["version"]),
        instructions=str(raw.get("instructions") or ""),
        tools=tuple(
            ToolManifest(
                str(tool["name"]),
                str(tool.get("description") or ""),
                dict(tool.get("input_schema") or {}),
                ToolClassification(tool["classification"]),
            )
            for tool in raw.get("tools", ())
        ),
        platforms=tuple(raw.get("platforms") or ()),
        os_requirements=tuple(raw.get("os_requirements") or ()),
        manifest_version=int(raw.get("manifest_version") or 1),
    )


if __name__ == "__main__":
    main()

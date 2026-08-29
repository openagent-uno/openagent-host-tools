#!/usr/bin/env python3
"""Non-destructive MCP smoke for a freshly built/signed native bundle."""

from __future__ import annotations

import argparse
import asyncio
import os
import socket
import tempfile
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def _core(bundle: Path) -> None:
    executable = bundle / ("openagent-host-tools.exe" if os.name == "nt" else "openagent-host-tools")
    calls = {
        "filesystem": ("list_directory", {"path": "."}),
        "editor": ("grep", {"pattern": "never-matches", "path": "."}),
        "shell": ("shell_which", {"command": "sh" if os.name != "nt" else "cmd"}),
    }
    for server, (tool, arguments) in calls.items():
        params = StdioServerParameters(
            command=str(executable),
            args=["--mcp", server],
            cwd=str(bundle),
        )
        with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as errlog:
            async with stdio_client(params, errlog=errlog) as (reader, writer):
                async with ClientSession(reader, writer) as session:
                    initialized = await session.initialize()
                    if initialized.serverInfo.name != server:
                        raise RuntimeError(f"unexpected MCP identity: {initialized.serverInfo.name}")
                    result = await session.call_tool(tool, arguments)
                    if result.isError:
                        raise RuntimeError(f"{server}.{tool} failed: {result.content}")
            errlog.seek(0)
            stderr = errlog.read()
        if "Traceback" in stderr or "I/O operation on closed file" in stderr:
            raise RuntimeError(f"{server} emitted a shutdown traceback: {stderr}")


async def _chrome(bundle: Path) -> None:
    node = bundle / ("node.exe" if os.name == "nt" else "node")
    script = bundle / "agent-in-chrome" / "host" / "mcp-server.js"
    with tempfile.TemporaryDirectory(prefix="openagent-chrome-smoke-") as temporary:
        root = Path(temporary)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            cdp_port = reservation.getsockname()[1]
        params = StdioServerParameters(
            command=str(node),
            args=[str(script)],
            env={
                **os.environ,
                "OPENAGENT_CHROME_PROFILE_DIR": str(root / "profile"),
                "OPENAGENT_CHROME_EXTENSIONS_DIR": str(root / "extensions"),
                "OPENAGENT_CHROME_CDP_PORT": str(cdp_port),
            },
        )
        with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as errlog:
            async with stdio_client(params, errlog=errlog) as (reader, writer):
                async with ClientSession(reader, writer) as session:
                    initialized = await session.initialize()
                    if initialized.serverInfo.name != "agent-in-chrome":
                        raise RuntimeError(
                            f"unexpected Agent-in-Chrome identity: {initialized.serverInfo.name}"
                        )
                    result = await session.call_tool("list_extensions", {})
                    if result.isError:
                        raise RuntimeError(f"agent-in-chrome smoke failed: {result.content}")
                    invalid = await session.call_tool(
                        "upload_image", {"imageId": "missing", "tabId": 1}
                    )
                    if not invalid.isError:
                        raise RuntimeError("Agent-in-Chrome runtime errors must set isError")
            errlog.seek(0)
            stderr = errlog.read()
            if "Traceback" in stderr:
                raise RuntimeError(f"agent-in-chrome emitted a traceback: {stderr}")
        # The sidecar owns a detached Chromium process group. Give its SIGTERM
        # handler time to release the profile before removing the temporary dir.
        await asyncio.sleep(1.5)


async def _run(bundle: Path, *, core_only: bool) -> None:
    await _core(bundle)
    if not core_only:
        await _chrome(bundle)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--core-only", action="store_true")
    args = parser.parse_args()
    asyncio.run(_run(args.bundle.resolve(), core_only=args.core_only))


if __name__ == "__main__":
    main()

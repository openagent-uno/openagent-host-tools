"""Stand-alone MCP stdio entrypoints for the shared Python built-ins.

The process protocol is deliberately provided by the official MCP SDK rather
than by the local capability-host wire protocol. This keeps initialization,
JSON-RPC lifecycle, cancellation, schema validation and protocol negotiation
identical to every other MCP server consumed by OpenAgent.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import anyio
from mcp import types as mcp_types
from mcp.server.lowlevel import Server
from mcp.server.lowlevel.server import NotificationOptions
from mcp.server.stdio import stdio_server

from .builtins import EditorServer, FilesystemServer, ShellServer
from .context import current_principal
from .types import HostError, ToolClassification, tool_error_result


def _server(name: str):
    factories = {
        "filesystem": FilesystemServer,
        "editor": EditorServer,
        "shell": ShellServer,
    }
    try:
        return factories[name](Path.cwd())
    except KeyError as exc:
        raise SystemExit(f"unknown built-in MCP server: {name}") from exc


def _tool_wire(tool) -> mcp_types.Tool:
    classification = tool.classification
    return mcp_types.Tool(
        name=tool.name,
        description=tool.description,
        inputSchema=tool.input_schema,
        annotations=mcp_types.ToolAnnotations(
            readOnlyHint=classification == ToolClassification.READ_ONLY,
            idempotentHint=classification
            in {ToolClassification.READ_ONLY, ToolClassification.IDEMPOTENT},
            destructiveHint=classification == ToolClassification.MUTATING,
        ),
    )


async def serve(name: str) -> None:
    capability = _server(name)
    app = Server(
        capability.manifest.name,
        version=capability.manifest.version,
        instructions=capability.manifest.instructions,
    )

    @app.list_tools()
    async def list_tools() -> list[mcp_types.Tool]:
        return [_tool_wire(tool) for tool in capability.manifest.tools]

    @app.call_tool(validate_input=True)
    async def call_tool(tool_name: str, arguments: dict[str, Any]):
        token = current_principal.set("standalone-mcp-stdio")
        try:
            try:
                result = await capability.call(tool_name, arguments)
            except HostError as exc:
                result = tool_error_result(exc)
            # Validate against the official MCP envelope before it reaches the
            # transport. This catches non-object structuredContent at the
            # shared module boundary instead of failing in a remote client.
            return mcp_types.CallToolResult.model_validate(result.to_wire())
        finally:
            current_principal.reset(token)

    options = app.create_initialization_options(
        NotificationOptions(tools_changed=False)
    )
    try:
        # Passing the process streams explicitly avoids the SDK allocating an
        # owning TextIOWrapper around ``sys.stdout.buffer``. That wrapper can
        # close PyInstaller's stdout during interpreter teardown.
        try:
            sys.stdin.reconfigure(encoding="utf-8", errors="replace")
            sys.stdout.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass
        stdin = anyio.wrap_file(sys.stdin)
        stdout = anyio.wrap_file(sys.stdout)
        async with stdio_server(stdin=stdin, stdout=stdout) as (
            read_stream,
            write_stream,
        ):
            await app.run(read_stream, write_stream, options)
    finally:
        await capability.close()


def main(name: str | None = None) -> None:
    selected = name or (sys.argv[1] if len(sys.argv) > 1 else "")
    asyncio.run(serve(selected))


def filesystem_main() -> None:
    main("filesystem")


def editor_main() -> None:
    main("editor")


def shell_main() -> None:
    main("shell")


if __name__ == "__main__":
    main()

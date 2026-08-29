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

SHELL_COMPLETION_LOGGER = "openagent.shell"
SHELL_COMPLETION_CAPABILITY = "openagent/shell-completion"
SHELL_COMPLETION_CAPABILITY_VERSION = "1"


def _server(name: str, *, shell_event_sink=None):
    factories = {
        "filesystem": FilesystemServer,
        "editor": EditorServer,
        "shell": lambda cwd: ShellServer(cwd, event_sink=shell_event_sink),
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
    notification_session = None

    async def emit_shell_event(event: dict[str, Any]) -> None:
        # MCP deliberately has no general-purpose custom-notification frame.
        # A logging notification is the standard, typed server-to-client
        # channel, while the experimental capability below makes the event
        # contract discoverable instead of asking consumers to scrape logs.
        session = notification_session
        if session is None:
            return
        try:
            await session.send_log_message(
                "notice",
                event,
                logger=SHELL_COMPLETION_LOGGER,
            )
        except (
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
            anyio.EndOfStream,
        ):
            # The process may be shutting down while a background command
            # exits. The command has already been finalized at this point.
            return

    capability = _server(
        name,
        shell_event_sink=emit_shell_event if name == "shell" else None,
    )
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
        nonlocal notification_session
        notification_session = app.request_context.session
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

    if name == "shell":

        @app.set_logging_level()
        async def set_logging_level(_level: mcp_types.LoggingLevel) -> None:
            # Completion events are control-plane events rather than
            # diagnostic verbosity, so a client logging threshold must not
            # suppress them.
            return None

    experimental_capabilities = None
    if name == "shell":
        experimental_capabilities = {
            SHELL_COMPLETION_CAPABILITY: {
                "version": SHELL_COMPLETION_CAPABILITY_VERSION,
                "transport": "notifications/message",
                "logger": SHELL_COMPLETION_LOGGER,
                "data": "shell_completed event",
            }
        }
    options = app.create_initialization_options(
        NotificationOptions(tools_changed=False),
        experimental_capabilities=experimental_capabilities,
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

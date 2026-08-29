from __future__ import annotations

import asyncio
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from openagent_host_tools.builtins import EditorServer, FilesystemServer, ShellServer
from openagent_host_tools.context import current_principal
from openagent_host_tools.mcp_server import (
    SHELL_COMPLETION_CAPABILITY,
    SHELL_COMPLETION_CAPABILITY_VERSION,
    SHELL_COMPLETION_LOGGER,
)


def _short_background_command() -> str:
    argv = [
        sys.executable,
        "-c",
        (
            "import sys,time; time.sleep(0.1); "
            "sys.stdout.buffer.write(b'standalone-event\\n'); sys.stdout.buffer.flush()"
        ),
    ]
    return subprocess.list2cmdline(argv) if os.name == "nt" else shlex.join(argv)


def _long_background_command() -> str:
    argv = [sys.executable, "-c", "import time; time.sleep(30)"]
    return subprocess.list2cmdline(argv) if os.name == "nt" else shlex.join(argv)


async def _request(process, request_id: int, method: str, params: dict | None = None):
    payload = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        payload["params"] = params
    process.stdin.write((json.dumps(payload) + "\n").encode())
    await process.stdin.drain()
    response = json.loads(await asyncio.wait_for(process.stdout.readline(), timeout=5))
    assert response["id"] == request_id
    assert "error" not in response, response
    return response["result"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "server_type", "tool", "arguments"),
    [
        ("filesystem", FilesystemServer, "list_directory", {"path": "."}),
        ("editor", EditorServer, "grep", {"pattern": "never-matches", "path": "."}),
        ("shell", ShellServer, "shell_which", {"command": "sh"}),
    ],
)
async def test_standalone_builtin_mcp_jsonrpc_contract(
    tmp_path: Path, name, server_type, tool: str, arguments: dict
):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "openagent_host_tools.mcp_server",
        name,
        cwd=tmp_path,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdin is not None
    assert process.stdout is not None
    try:
        initialized = await _request(
            process,
            1,
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "contract-test", "version": "1"},
            },
        )
        assert initialized["serverInfo"] == {
            "name": name,
            "version": server_type.manifest.version,
        }
        assert initialized["protocolVersion"] == "2024-11-05"
        listed = await _request(process, 2, "tools/list")
        by_name = {item["name"]: item for item in listed["tools"]}
        assert set(by_name) == {item.name for item in server_type.manifest.tools}
        for manifest in server_type.manifest.tools:
            wire = by_name[manifest.name]
            assert wire["description"] == manifest.description
            assert wire["inputSchema"] == manifest.input_schema
            assert wire["annotations"]["readOnlyHint"] == (
                manifest.classification.value == "read_only"
            )
            assert wire["annotations"]["idempotentHint"] == (
                manifest.classification.value in {"read_only", "idempotent"}
            )
            assert wire["annotations"]["destructiveHint"] == (
                manifest.classification.value == "mutating"
            )
        result = await _request(
            process,
            3,
            "tools/call",
            {"name": tool, "arguments": arguments},
        )
        assert result["isError"] is False
        assert result["content"]
    finally:
        process.stdin.close()
        await asyncio.wait_for(process.wait(), timeout=5)


@pytest.mark.asyncio
async def test_official_mcp_client_session_calls_standalone_filesystem(tmp_path: Path):
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "openagent_host_tools.mcp_server", "filesystem"],
        cwd=str(tmp_path),
    )
    async with stdio_client(params) as (reader, writer):
        async with ClientSession(reader, writer) as session:
            initialized = await session.initialize()
            assert initialized.serverInfo.name == "filesystem"
            listed = await session.list_tools()
            assert {tool.name for tool in listed.tools} == {
                tool.name for tool in FilesystemServer.manifest.tools
            }
            called = await session.call_tool("list_directory", {"path": "."})
            assert called.isError is False
            assert called.structuredContent is not None
            one = tmp_path / "one.txt"
            two = tmp_path / "two.txt"
            one.write_text("one")
            two.write_text("two")
            multiple = await session.call_tool(
                "read_multiple_files", {"paths": [str(one), str(two)]}
            )
            assert multiple.isError is False
            assert multiple.structuredContent == {
                "files": [
                    {"path": str(one), "content": "one"},
                    {"path": str(two), "content": "two"},
                ]
            }


@pytest.mark.asyncio
async def test_official_mcp_client_validates_empty_and_nonempty_shell_lists(
    tmp_path: Path,
):
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "openagent_host_tools.mcp_server", "shell"],
        cwd=str(tmp_path),
    )
    async with stdio_client(params) as (reader, writer):
        async with ClientSession(reader, writer) as session:
            await session.initialize()
            invalid = await session.call_tool("shell_which", {"command": "sh", "unexpected": 1})
            assert invalid.isError is True
            assert "additional" in invalid.content[0].text.lower()
            empty = await session.call_tool("shell_list", {})
            assert empty.isError is False
            assert empty.structuredContent == {"shells": []}
            started = await session.call_tool(
                "shell_exec",
                {"command": _long_background_command(), "run_in_background": True},
            )
            assert started.isError is False, started.content
            shell_id = started.structuredContent["shell_id"]
            nonempty = await session.call_tool("shell_list", {})
            assert nonempty.isError is False
            assert nonempty.structuredContent["shells"][0]["shell_id"] == shell_id
            killed = await session.call_tool("shell_kill", {"shell_id": shell_id})
            assert killed.isError is False


@pytest.mark.asyncio
async def test_standalone_shell_completion_notification_matches_embedded_contract(
    tmp_path: Path,
):
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    embedded_events: asyncio.Queue[dict] = asyncio.Queue()
    embedded = ShellServer(tmp_path, event_sink=embedded_events.put_nowait)
    token = current_principal.set("standalone-mcp-stdio")
    try:
        embedded_started = await embedded.call(
            "shell_exec",
            {"command": _short_background_command(), "run_in_background": True},
        )
        embedded_event = await asyncio.wait_for(embedded_events.get(), timeout=10)
    finally:
        current_principal.reset(token)
        await embedded.close()

    standalone_events: asyncio.Queue[dict] = asyncio.Queue()

    async def logging_callback(params) -> None:
        if params.logger == SHELL_COMPLETION_LOGGER:
            standalone_events.put_nowait(params.data)

    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "openagent_host_tools.mcp_server", "shell"],
        cwd=str(tmp_path),
    )
    async with stdio_client(params) as (reader, writer):
        async with ClientSession(
            reader,
            writer,
            logging_callback=logging_callback,
        ) as session:
            initialized = await session.initialize()
            assert initialized.capabilities.logging is not None
            assert initialized.capabilities.experimental == {
                SHELL_COMPLETION_CAPABILITY: {
                    "version": SHELL_COMPLETION_CAPABILITY_VERSION,
                    "transport": "notifications/message",
                    "logger": SHELL_COMPLETION_LOGGER,
                    "data": "shell_completed event",
                }
            }
            standalone_started = await session.call_tool(
                "shell_exec",
                {"command": _short_background_command(), "run_in_background": True},
            )
            standalone_event = await asyncio.wait_for(standalone_events.get(), timeout=10)

    assert embedded_started.structured_content is not None
    assert standalone_started.structuredContent is not None
    assert embedded_event["shell_id"] == embedded_started.structured_content["shell_id"]
    assert standalone_event["shell_id"] == standalone_started.structuredContent["shell_id"]
    assert set(standalone_event) == set(embedded_event)
    for key in (
        "type",
        "server",
        "status",
        "exit_code",
        "stdout_bytes",
        "stderr_bytes",
        "output_bytes",
        "signal",
        "principal",
    ):
        assert standalone_event[key] == embedded_event[key]
    assert standalone_event["type"] == "shell_completed"
    assert standalone_event["principal"] == "standalone-mcp-stdio"
    assert standalone_event["stdout_bytes"] == len(b"standalone-event\n")


@pytest.mark.asyncio
async def test_official_mcp_lifecycle_and_unknown_version_negotiation(tmp_path: Path):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "openagent_host_tools.mcp_server",
        "filesystem",
        cwd=tmp_path,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdin is not None
    assert process.stdout is not None
    try:
        process.stdin.write(
            (
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/list",
                        "params": {},
                    }
                )
                + "\n"
            ).encode()
        )
        await process.stdin.drain()
        preinitialize = json.loads(await asyncio.wait_for(process.stdout.readline(), timeout=5))
        assert preinitialize["error"]["code"] == -32602

        negotiated = await _request(
            process,
            2,
            "initialize",
            {
                "protocolVersion": "bogus",
                "capabilities": {},
                "clientInfo": {"name": "contract-test", "version": "1"},
            },
        )
        assert negotiated["protocolVersion"] == "2025-11-25"
    finally:
        process.stdin.close()
        await asyncio.wait_for(process.wait(), timeout=5)


@pytest.mark.asyncio
async def test_mcp_tool_errors_are_call_results_and_cancel_stops_shell(tmp_path: Path):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "openagent_host_tools.mcp_server",
        "shell",
        cwd=tmp_path,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdin is not None
    assert process.stdout is not None
    try:
        await _request(
            process,
            1,
            "initialize",
            {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "cancel-test", "version": "1"},
            },
        )
        missing = await _request(
            process,
            2,
            "tools/call",
            {"name": "shell_output", "arguments": {"shell_id": "missing"}},
        )
        assert missing["isError"] is True
        assert missing["_meta"]["openagent/error"]["code"] == "shell_not_found"

        request = {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "shell_exec",
                "arguments": {"command": "sleep 30", "timeout": 30_000},
            },
        }
        process.stdin.write((json.dumps(request) + "\n").encode())
        await process.stdin.drain()
        await asyncio.sleep(0.1)
        process.stdin.write(
            (
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "method": "notifications/cancelled",
                        "params": {"requestId": 3, "reason": "test"},
                    }
                )
                + "\n"
            ).encode()
        )
        await process.stdin.drain()
        cancelled = json.loads(await asyncio.wait_for(process.stdout.readline(), timeout=5))
        assert cancelled["id"] == 3
        assert "error" in cancelled
        assert "cancel" in cancelled["error"]["message"].lower()
    finally:
        process.stdin.close()
        await asyncio.wait_for(process.wait(), timeout=5)

#!/usr/bin/env python3
"""Non-destructive MCP smoke for a freshly built/signed native bundle."""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import json
import os
import socket
import struct
import sys
import tempfile
import zlib
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

COMPUTER_CONTROL_MODES = ("expect-denied", "expect-granted", "skip")
CONTROLLED_CURSOR_TARGET = (64, 64)
CURSOR_TOLERANCE = 2
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def _result_text(result: object) -> str:
    content = getattr(result, "content", ())
    return "\n".join(
        str(item.text)
        for item in content
        if getattr(item, "type", None) == "text" and hasattr(item, "text")
    )


def _require_permission_error(result: object, permission: str) -> None:
    if not getattr(result, "isError", False):
        raise RuntimeError(f"expected macOS {permission} denial, but the call succeeded")
    text = _result_text(result)
    expected = f"macos {permission.lower()} permission required"
    if expected not in text.lower():
        raise RuntimeError(
            f"expected the stable macOS {permission} TCC error, got: {text or result!r}"
        )


def _validate_cursor_result(result: object) -> tuple[int, int]:
    if getattr(result, "isError", False):
        raise RuntimeError(f"computer-control cursor_position failed: {_result_text(result)}")
    for item in getattr(result, "content", ()):
        if getattr(item, "type", None) != "text":
            continue
        try:
            value = json.loads(item.text)
        except (AttributeError, TypeError, json.JSONDecodeError):
            continue
        if not isinstance(value, dict):
            continue
        coordinates = (value.get("x"), value.get("y"))
        if all(
            isinstance(coordinate, int) and not isinstance(coordinate, bool)
            for coordinate in coordinates
        ):
            return coordinates
    raise RuntimeError("computer-control cursor_position returned no integer x/y coordinates")


def _require_cursor_near(
    actual: tuple[int, int],
    expected: tuple[int, int],
    *,
    tolerance: int = CURSOR_TOLERANCE,
) -> None:
    if any(abs(value - target) > tolerance for value, target in zip(actual, expected)):
        raise RuntimeError(
            "computer-control mouse_move was not reflected by get_cursor_position: "
            f"expected {expected} ±{tolerance}, got {actual}"
        )


def _validate_screenshot_result(result: object) -> None:
    if getattr(result, "isError", False):
        raise RuntimeError(f"computer-control screenshot failed: {_result_text(result)}")
    for item in getattr(result, "content", ()):
        if getattr(item, "type", None) != "image" or getattr(item, "mimeType", None) != "image/png":
            continue
        try:
            payload = base64.b64decode(item.data, validate=True)
        except (AttributeError, TypeError, ValueError, binascii.Error) as exc:
            raise RuntimeError("computer-control returned invalid base64 image data") from exc
        _validate_png(payload)
        return
    raise RuntimeError("computer-control screenshot returned no image/png content")


def _validate_png(payload: bytes) -> None:
    if payload[:8] != PNG_SIGNATURE:
        raise RuntimeError("computer-control returned an invalid PNG image")
    position = len(PNG_SIGNATURE)
    chunk_index = 0
    width = height = 0
    compressed = bytearray()
    saw_iend = False
    while position < len(payload):
        if position + 12 > len(payload):
            raise RuntimeError("computer-control returned a truncated PNG image")
        size = struct.unpack(">I", payload[position : position + 4])[0]
        chunk_type = payload[position + 4 : position + 8]
        chunk_end = position + 12 + size
        if chunk_end > len(payload):
            raise RuntimeError("computer-control returned a truncated PNG chunk")
        data = payload[position + 8 : position + 8 + size]
        expected_crc = struct.unpack(">I", payload[position + 8 + size : chunk_end])[0]
        actual_crc = zlib.crc32(chunk_type + data) & 0xFFFFFFFF
        if expected_crc != actual_crc:
            raise RuntimeError("computer-control returned a PNG with an invalid checksum")
        if chunk_index == 0:
            if chunk_type != b"IHDR" or size != 13:
                raise RuntimeError("computer-control returned a PNG without a valid IHDR")
            width, height = struct.unpack(">II", data[:8])
        elif chunk_type == b"IDAT":
            compressed.extend(data)
        elif chunk_type == b"IEND":
            if size != 0 or chunk_end != len(payload):
                raise RuntimeError("computer-control returned an invalid PNG terminator")
            saw_iend = True
        position = chunk_end
        chunk_index += 1
    if width < 1 or height < 1 or not compressed or not saw_iend:
        raise RuntimeError("computer-control returned an incomplete PNG image")
    try:
        decoded = zlib.decompress(bytes(compressed))
    except zlib.error as exc:
        raise RuntimeError("computer-control returned corrupt PNG pixel data") from exc
    if not decoded:
        raise RuntimeError("computer-control returned an empty PNG image")


async def _core(bundle: Path) -> None:
    executable = bundle / (
        "openagent-host-tools.exe" if os.name == "nt" else "openagent-host-tools"
    )
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
                        raise RuntimeError(
                            f"unexpected MCP identity: {initialized.serverInfo.name}"
                        )
                    result = await session.call_tool(tool, arguments)
                    if result.isError:
                        raise RuntimeError(f"{server}.{tool} failed: {result.content}")
            errlog.seek(0)
            stderr = errlog.read()
        if "Traceback" in stderr or "I/O operation on closed file" in stderr:
            raise RuntimeError(f"{server} emitted a shutdown traceback: {stderr}")


def _computer_executable(bundle: Path) -> Path:
    if sys.platform == "darwin":
        return (
            bundle
            / "openagent-computer-control.app"
            / "Contents"
            / "MacOS"
            / "openagent-computer-control"
        )
    return bundle / (
        "openagent-computer-control.exe"
        if os.name == "nt"
        else "openagent-computer-control"
    )


async def _computer_control(
    bundle: Path,
    mode: str,
    *,
    macos_launchservices: bool = False,
) -> None:
    if macos_launchservices and (
        sys.platform != "darwin" or mode != "expect-denied"
    ):
        raise RuntimeError(
            "--macos-launchservices requires macOS and expect-denied"
        )
    if mode == "skip":
        return
    if mode == "expect-denied" and sys.platform != "darwin":
        raise RuntimeError("expect-denied is specific to macOS TCC")
    executable = _computer_executable(bundle)
    if not executable.is_file():
        raise RuntimeError(f"computer-control executable is missing: {executable}")

    if macos_launchservices:
        helper = bundle / "openagent-computer-control.app"
        command = str(Path(__file__).with_name("launch_macos_app_stdio.sh"))
        command_args = [str(helper)]
    else:
        command = str(executable)
        command_args = []

    # The MCP SDK intentionally starts stdio servers with a small safe
    # environment by default.  This native sidecar must inherit DISPLAY and
    # XAUTHORITY from xvfb-run (and the corresponding desktop variables on
    # other platforms) so the smoke exercises the real input/display path.
    params = StdioServerParameters(
        command=command,
        args=command_args,
        cwd=str(bundle),
        env=dict(os.environ),
    )
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as errlog:
        try:
            async with stdio_client(params, errlog=errlog) as (reader, writer):
                async with ClientSession(reader, writer) as session:
                    initialized = await session.initialize()
                    if initialized.serverInfo.name != "computer-control":
                        raise RuntimeError(
                            "unexpected computer-control MCP identity: "
                            f"{initialized.serverInfo.name}"
                        )
                    catalog = await session.list_tools()
                    if [tool.name for tool in catalog.tools] != ["computer"]:
                        raise RuntimeError(
                            "unexpected computer-control tool catalog: "
                            f"{[tool.name for tool in catalog.tools]}"
                        )

                    if mode == "expect-denied":
                        # Exercise both independent native permission gates.
                        # A screenshot also reads the cursor for its crosshair,
                        # so use the recording probe to reach Screen Recording
                        # without first requiring Accessibility.
                        with tempfile.TemporaryDirectory(
                            prefix="openagent-denied-recording-"
                        ) as denied_root:
                            recording = await session.call_tool(
                                "computer",
                                {
                                    "action": "start_screen_recording",
                                    "fps": 1,
                                    "max_duration_seconds": 1,
                                    "path": str(Path(denied_root) / "denied.mp4"),
                                },
                            )
                        _require_permission_error(recording, "Screen Recording")
                        cursor = await session.call_tool(
                            "computer", {"action": "get_cursor_position"}
                        )
                        _require_permission_error(cursor, "Accessibility")
                    else:
                        cursor = await session.call_tool(
                            "computer", {"action": "get_cursor_position"}
                        )
                        _validate_cursor_result(cursor)
                        movement = await session.call_tool(
                            "computer",
                            {
                                "action": "mouse_move",
                                "coordinate": list(CONTROLLED_CURSOR_TARGET),
                            },
                        )
                        if movement.isError:
                            raise RuntimeError(
                                "computer-control controlled mouse_move failed: "
                                f"{_result_text(movement)}"
                            )
                        await asyncio.sleep(0.1)
                        moved_cursor = await session.call_tool(
                            "computer", {"action": "get_cursor_position"}
                        )
                        _require_cursor_near(
                            _validate_cursor_result(moved_cursor),
                            CONTROLLED_CURSOR_TARGET,
                        )
                        screenshot = await session.call_tool(
                            "computer", {"action": "get_screenshot"}
                        )
                        _validate_screenshot_result(screenshot)
        except BaseException:
            errlog.seek(0)
            captured = errlog.read()
            if captured:
                print(captured, file=sys.stderr, end="")
            raise
        finally:
            errlog.seek(0)
            stderr = errlog.read()
    if "panicked at" in stderr or "stack backtrace" in stderr.lower():
        raise RuntimeError(f"computer-control emitted a panic: {stderr}")


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


async def _run(
    bundle: Path,
    *,
    core_only: bool,
    computer_control: str,
    macos_launchservices: bool = False,
) -> None:
    await _core(bundle)
    await _computer_control(
        bundle,
        computer_control,
        macos_launchservices=macos_launchservices,
    )
    if not core_only:
        await _chrome(bundle)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--core-only", action="store_true")
    parser.add_argument(
        "--computer-control",
        choices=COMPUTER_CONTROL_MODES,
        required=True,
        help=(
            "explicitly require a macOS TCC denial, require cursor/screenshot "
            "success, or skip the computer-control smoke"
        ),
    )
    parser.add_argument(
        "--macos-launchservices",
        action="store_true",
        help=(
            "launch the signed computer-control app through LaunchServices so "
            "macOS evaluates its own TCC identity (expect-denied only)"
        ),
    )
    return parser.parse_args(argv)


def main() -> None:
    args = _parse_args()
    asyncio.run(
        _run(
            args.bundle.resolve(),
            core_only=args.core_only,
            computer_control=args.computer_control,
            macos_launchservices=args.macos_launchservices,
        )
    )


if __name__ == "__main__":
    main()

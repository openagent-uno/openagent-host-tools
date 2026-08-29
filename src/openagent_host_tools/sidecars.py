"""Discovery metadata for optional computer-control and browser sidecars."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import shutil
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from ._version import __version__
from .types import ServerManifest, ToolClassification, ToolManifest

_HOST_TOOLS_VERSION = __version__
_VERIFIED_BUNDLES: dict[str, str | None] = {}


def _object(properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": required or [],
        "additionalProperties": False,
    }


_COMPUTER_ACTION_CLASSIFICATIONS = {
    "action": {
        "get_cursor_position": ToolClassification.READ_ONLY,
        "get_screenshot": ToolClassification.READ_ONLY,
    }
}


COMPUTER_CONTROL_MANIFEST = ServerManifest(
    name="computer-control",
    version="1.0.0",
    instructions=(
        "Mouse, keyboard, screenshot and screen recording control on the current client "
        "computer. The signed sidecar may require OS Accessibility and Screen Recording access."
    ),
    tools=(
        ToolManifest(
            "computer",
            "Control the local desktop, capture screenshots, or record the screen.",
            _object(
                {
                    "action": {
                        "type": "string",
                        "enum": [
                            "key",
                            "type",
                            "get_cursor_position",
                            "mouse_move",
                            "left_click",
                            "left_click_drag",
                            "right_click",
                            "middle_click",
                            "double_click",
                            "scroll",
                            "get_screenshot",
                            "start_screen_recording",
                            "stop_screen_recording",
                        ],
                    },
                    "coordinate": {"type": "array", "items": {"type": "integer"}},
                    "text": {"type": "string"},
                    "scroll_direction": {"type": "string"},
                    "scroll_amount": {"type": "integer"},
                    "region": {"type": "array", "items": {"type": "integer"}},
                    "fps": {"type": "integer"},
                    "path": {"type": "string"},
                    "max_duration_seconds": {"type": "number"},
                },
                ["action"],
            ),
            ToolClassification.MUTATING,
            classification_by_argument=_COMPUTER_ACTION_CLASSIFICATIONS,
        ),
    ),
    available=False,
    unavailable_reason="openagent-computer-control sidecar was not found",
    platforms=(
        "darwin-arm64",
        "darwin-x64",
        "linux-arm64",
        "linux-x64",
        "win32-arm64",
        "win32-x64",
    ),
    os_requirements=(
        "macOS: Accessibility and Screen Recording permissions",
        "Linux: an active graphical session with input/capture support",
        "Windows: interactive desktop session",
    ),
)


def _chrome_tool(
    name: str,
    description: str,
    properties: dict[str, Any],
    required: list[str] | None = None,
    *,
    mutating: bool = False,
) -> ToolManifest:
    return ToolManifest(
        name,
        description,
        _object(properties, required),
        ToolClassification.MUTATING if mutating else ToolClassification.READ_ONLY,
    )


_TAB_ID = {"type": "number", "description": "Tab ID from tabs_context_mcp"}
AGENT_IN_CHROME_MANIFEST = ServerManifest(
    name="agent-in-chrome",
    version="2.0.0",
    instructions=(
        "Control OpenAgent's dedicated Chromium profile on the current client. Call "
        "tabs_context_mcp before tab-specific tools. This is not the server's browser."
    ),
    tools=(
        _chrome_tool(
            "tabs_context_mcp",
            "List available browser tabs.",
            {"createIfEmpty": {"type": "boolean"}},
        ),
        _chrome_tool("tabs_create_mcp", "Open a blank tab.", {}, mutating=True),
        _chrome_tool(
            "navigate",
            "Navigate a tab to a URL or through history.",
            {"url": {"type": "string"}, "tabId": _TAB_ID},
            ["url", "tabId"],
            mutating=True,
        ),
        _chrome_tool(
            "computer",
            "Mouse, keyboard and screenshot control for a browser tab.",
            {
                "action": {"type": "string"},
                "tabId": _TAB_ID,
                "coordinate": {"type": "array"},
                "text": {"type": "string"},
                "ref": {"type": "string"},
            },
            ["action", "tabId"],
            mutating=True,
        ),
        _chrome_tool(
            "find",
            "Find page elements and return stable refs.",
            {"query": {"type": "string"}, "tabId": _TAB_ID},
            ["query", "tabId"],
        ),
        _chrome_tool(
            "form_input",
            "Set a form field by ref.",
            {"ref": {"type": "string"}, "value": {}, "tabId": _TAB_ID},
            ["ref", "value", "tabId"],
            mutating=True,
        ),
        _chrome_tool("get_page_text", "Extract readable page text.", {"tabId": _TAB_ID}, ["tabId"]),
        _chrome_tool(
            "read_page",
            "Read the accessibility tree with element refs.",
            {
                "tabId": _TAB_ID,
                "filter": {"type": "string"},
                "depth": {"type": "number"},
                "ref_id": {"type": "string"},
                "max_chars": {"type": "number"},
            },
            ["tabId"],
        ),
        _chrome_tool(
            "javascript_tool",
            "Evaluate JavaScript in a tab.",
            {"action": {"const": "javascript_exec"}, "text": {"type": "string"}, "tabId": _TAB_ID},
            ["action", "text", "tabId"],
            mutating=True,
        ),
        _chrome_tool(
            "read_console_messages",
            "Read captured console messages.",
            {
                "tabId": _TAB_ID,
                "pattern": {"type": "string"},
                "limit": {"type": "number"},
                "onlyErrors": {"type": "boolean"},
                "clear": {"type": "boolean"},
            },
            ["tabId"],
        ),
        _chrome_tool(
            "read_network_requests",
            "Read captured network requests.",
            {
                "tabId": _TAB_ID,
                "urlPattern": {"type": "string"},
                "limit": {"type": "number"},
                "clear": {"type": "boolean"},
            },
            ["tabId"],
        ),
        _chrome_tool(
            "resize_window",
            "Resize the browser viewport.",
            {"width": {"type": "number"}, "height": {"type": "number"}, "tabId": _TAB_ID},
            ["width", "height", "tabId"],
            mutating=True,
        ),
        _chrome_tool(
            "upload_image",
            "Upload a captured screenshot to a file input.",
            {
                "imageId": {"type": "string"},
                "tabId": _TAB_ID,
                "ref": {"type": "string"},
                "filename": {"type": "string"},
            },
            ["imageId", "tabId"],
            mutating=True,
        ),
        _chrome_tool("list_extensions", "List installed browser extensions.", {}),
        _chrome_tool(
            "install_extension",
            "Install a persistent Chromium extension.",
            {"source": {"type": "string"}},
            ["source"],
            mutating=True,
        ),
        _chrome_tool(
            "remove_extension",
            "Remove an agent-installed extension.",
            {"id": {"type": "string"}},
            ["id"],
            mutating=True,
        ),
    ),
    available=False,
    unavailable_reason="agent-in-chrome sidecar was not found",
    platforms=(
        "darwin-arm64",
        "darwin-x64",
        "linux-arm64",
        "linux-x64",
        "win32-arm64",
        "win32-x64",
    ),
    os_requirements=(
        "Chromium runtime and permission to create a persistent browser profile",
        "Linux ARM64 and Windows ARM64 require an installed Chromium-family browser or "
        "OPENAGENT_CHROME_BINARY; no verified automatic snapshot is available",
    ),
)


def _authoritative_catalog(base: ServerManifest) -> ServerManifest:
    """Overlay the catalog captured from the exact bundled sidecar.

    Optional sidecars are not always executable on the machine doing discovery
    (for example a headless server or a source checkout without Rust).  Keeping
    their real MCP catalog as package data makes the unavailable manifest, the
    release lock and the catalog announced after startup one contract.
    """

    snapshot = Path(__file__).with_name("sidecar-manifests.json")
    if not snapshot.is_file():
        return base
    try:
        raw = json.loads(snapshot.read_text(encoding="utf-8"))[base.name]
        tools = []
        for tool in raw.get("tools", ()):
            name = str(tool["name"])
            fallback = base.tool(name)
            tools.append(
                ToolManifest(
                    name=name,
                    description=str(tool.get("description") or ""),
                    input_schema=dict(tool.get("input_schema") or {"type": "object"}),
                    classification=ToolClassification(
                        tool.get("classification", ToolClassification.MUTATING.value)
                    ),
                    classification_by_argument=(
                        fallback.classification_by_argument if fallback is not None else {}
                    ),
                )
            )
        tools = tuple(tools)
        if not tools:
            return base
        return replace(
            base,
            version=str(raw.get("version") or base.version),
            instructions=str(raw.get("instructions") or base.instructions),
            tools=tools,
        )
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        # Discovery remains usable in a damaged development checkout. Frozen
        # release bundles fail closed earlier through bundle-manifest.json.
        return base


COMPUTER_CONTROL_MANIFEST = _authoritative_catalog(COMPUTER_CONTROL_MANIFEST)
AGENT_IN_CHROME_MANIFEST = _authoritative_catalog(AGENT_IN_CHROME_MANIFEST)


@dataclass(frozen=True)
class SidecarCandidate:
    name: str
    command: tuple[str, ...] | None
    placeholder: ServerManifest
    reason: str | None = None


def discover_sidecars() -> list[SidecarCandidate]:
    return [
        _discover(
            "computer-control",
            "OPENAGENT_COMPUTER_CONTROL_COMMAND",
            "openagent-computer-control",
            COMPUTER_CONTROL_MANIFEST,
        ),
        _discover_agent_in_chrome(),
    ]


def _discover_agent_in_chrome() -> SidecarCandidate:
    configured = os.environ.get("OPENAGENT_AGENT_IN_CHROME_COMMAND")
    if configured:
        try:
            return SidecarCandidate(
                "agent-in-chrome",
                _parse_command(configured),
                AGENT_IN_CHROME_MANIFEST,
            )
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            return SidecarCandidate(
                "agent-in-chrome",
                None,
                AGENT_IN_CHROME_MANIFEST,
                f"invalid OPENAGENT_AGENT_IN_CHROME_COMMAND: {exc}",
            )

    roots: list[Path] = []
    sidecar_dir = os.environ.get("OPENAGENT_HOST_TOOLS_SIDECAR_DIR")
    if sidecar_dir:
        roots.append(Path(sidecar_dir))
    roots.extend(
        [
            Path(sys.executable).resolve().parent,
            Path(__file__).resolve().parent / "bin",
        ]
    )
    node_name = "node.exe" if os.name == "nt" else "node"
    for root in roots:
        script = root / "agent-in-chrome" / "host" / "mcp-server.js"
        if not script.is_file():
            continue
        bundled_node = root / node_name
        node = str(bundled_node) if bundled_node.is_file() else shutil.which("node")
        if node:
            integrity_error = _bundle_integrity_error(root)
            if integrity_error:
                return SidecarCandidate(
                    "agent-in-chrome",
                    None,
                    AGENT_IN_CHROME_MANIFEST,
                    integrity_error,
                )
            return SidecarCandidate(
                "agent-in-chrome",
                (node, str(script)),
                AGENT_IN_CHROME_MANIFEST,
            )
        return SidecarCandidate(
            "agent-in-chrome",
            None,
            AGENT_IN_CHROME_MANIFEST,
            f"agent-in-chrome was staged at {script}, but no Node.js runtime was found",
        )

    # Development/source installs may provide an executable wrapper.
    return _discover(
        "agent-in-chrome",
        "OPENAGENT_AGENT_IN_CHROME_COMMAND",
        "openagent-agent-in-chrome",
        AGENT_IN_CHROME_MANIFEST,
    )


def _discover(
    name: str, env_name: str, executable: str, placeholder: ServerManifest
) -> SidecarCandidate:
    configured = os.environ.get(env_name)
    if configured:
        try:
            command = _parse_command(configured)
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            return SidecarCandidate(name, None, placeholder, f"invalid {env_name}: {exc}")
        return SidecarCandidate(name, command, placeholder)

    sidecar_dir = os.environ.get("OPENAGENT_HOST_TOOLS_SIDECAR_DIR")
    suffix = ".exe" if os.name == "nt" else ""
    candidates: list[tuple[Path, Path]] = []

    def add_root(root: Path) -> None:
        if sys.platform == "darwin":
            candidates.append(
                (
                    root / f"{executable}.app" / "Contents" / "MacOS" / executable,
                    root,
                )
            )
        candidates.append((root / f"{executable}{suffix}", root))

    if sidecar_dir:
        add_root(Path(sidecar_dir))
    add_root(Path(sys.executable).resolve().parent)
    add_root(Path(__file__).resolve().parent / "bin")
    for candidate, bundle_root in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            integrity_error = _bundle_integrity_error(bundle_root)
            if integrity_error:
                return SidecarCandidate(name, None, placeholder, integrity_error)
            return SidecarCandidate(name, (str(candidate),), placeholder)
    found = shutil.which(executable)
    if found:
        if getattr(sys, "frozen", False):
            integrity_error = _bundle_integrity_error(Path(found).resolve().parent)
            if integrity_error:
                return SidecarCandidate(name, None, placeholder, integrity_error)
        return SidecarCandidate(name, (found,), placeholder)
    return SidecarCandidate(name, None, placeholder, placeholder.unavailable_reason)


def _parse_command(raw: str) -> tuple[str, ...]:
    value = raw.strip()
    if value.startswith("["):
        parsed = json.loads(value)
        if (
            not isinstance(parsed, list)
            or not parsed
            or not all(isinstance(part, str) and part for part in parsed)
        ):
            raise ValueError("JSON command must be a non-empty string array")
        return tuple(parsed)
    command = tuple(shlex.split(value, posix=os.name != "nt"))
    if not command:
        raise ValueError("command is empty")
    return command


def _bundle_integrity_error(root: Path) -> str | None:
    """Verify every staged runtime/asset before a frozen host may spawn it."""
    root = root.resolve()
    manifest_path = root / "bundle-manifest.json"
    if not manifest_path.is_file():
        return (
            "packaged sidecar bundle-manifest.json is missing"
            if getattr(sys, "frozen", False)
            else None
        )
    try:
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
        if value.get("manifest_version") != 1 or value.get("version") != _HOST_TOOLS_VERSION:
            raise ValueError("unsupported host-tools bundle version")
        files = value.get("files")
        if not isinstance(files, dict) or not files:
            raise ValueError("bundle file checksum map is empty")
        links = [path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_symlink()]
        if links:
            raise ValueError(f"bundle contains unsupported links: {links}")
        actual_files = {
            path.relative_to(root).as_posix()
            for path in root.rglob("*")
            if path.is_file() and path != manifest_path
        }
        declared_files = set(files)
        if actual_files != declared_files:
            missing = sorted(declared_files - actual_files)
            extra = sorted(actual_files - declared_files)
            raise ValueError(f"bundle file set mismatch (missing={missing}, extra={extra})")
        for relative, expected in files.items():
            if not isinstance(relative, str) or not isinstance(expected, dict):
                raise ValueError("invalid bundle checksum entry")
            path = (root / relative).resolve()
            if root not in path.parents or not path.is_file():
                raise ValueError(f"bundle file missing or unsafe: {relative}")
            if path.stat().st_size != int(expected.get("size", -1)):
                raise ValueError(f"bundle file size mismatch: {relative}")
            digest = _file_sha256(path)
            if digest != expected.get("sha256"):
                raise ValueError(f"bundle file checksum mismatch: {relative}")
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return f"packaged sidecar integrity check failed: {exc}"
    return None


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

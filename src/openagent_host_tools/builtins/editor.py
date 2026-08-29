"""Surgical edit, grep, and glob tools for the current client computer."""

from __future__ import annotations

import asyncio
import fnmatch
import re
from pathlib import Path
from typing import Any

from ..types import HostError, ServerManifest, ToolClassification, ToolManifest, ToolResult
from ._util import integer_arg, json_result, require_string


def _schema(properties: dict[str, Any], required: list[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


class EditorServer:
    manifest = ServerManifest(
        name="editor",
        version="1.0.0",
        instructions=(
            "Search and surgically edit files on the current client computer. All paths "
            "resolve on that client, not on the OpenAgent server."
        ),
        tools=(
            ToolManifest(
                "edit",
                "Replace the first exact occurrence in a text file, or all occurrences.",
                _schema(
                    {
                        "file_path": {"type": "string"},
                        "old_string": {"type": "string"},
                        "new_string": {"type": "string"},
                        "replace_all": {"type": "boolean", "default": False},
                    },
                    ["file_path", "old_string", "new_string"],
                ),
                ToolClassification.MUTATING,
            ),
            ToolManifest(
                "grep",
                "Search a regular expression across a file or directory with context lines.",
                _schema(
                    {
                        "pattern": {"type": "string"},
                        "path": {"type": "string", "default": "."},
                        "file_pattern": {"type": "string"},
                        "context": {"type": "integer", "minimum": 0, "maximum": 100},
                        "case_insensitive": {"type": "boolean", "default": False},
                        "max_results": {"type": "integer", "minimum": 1, "maximum": 500},
                    },
                    ["pattern"],
                ),
            ),
            ToolManifest(
                "glob",
                "Find files matching a glob, newest first.",
                _schema(
                    {
                        "pattern": {"type": "string"},
                        "path": {"type": "string", "default": "."},
                        "max_results": {"type": "integer", "minimum": 1, "maximum": 500},
                    },
                    ["pattern"],
                ),
            ),
        ),
    )

    def __init__(self, cwd: str | Path | None = None):
        self.cwd = Path(cwd or Path.cwd()).expanduser().resolve()

    def _path(self, raw: str) -> Path:
        path = Path(raw).expanduser()
        return path.resolve() if path.is_absolute() else (self.cwd / path).resolve()

    async def call(self, tool: str, args: dict[str, Any]) -> ToolResult:
        handler = getattr(self, f"_tool_{tool}", None)
        if handler is None:
            raise HostError("tool_not_found", f"editor has no tool {tool!r}")
        return await asyncio.to_thread(handler, args)

    async def close(self) -> None:
        return None

    def _tool_edit(self, args: dict[str, Any]) -> ToolResult:
        path = self._path(require_string(args, "file_path"))
        old = require_string(args, "old_string")
        new = require_string(args, "new_string", allow_empty=True)
        try:
            content = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise HostError("editor_error", f"cannot read {path}: {exc}") from exc
        count = content.count(old)
        if count == 0:
            raise HostError("text_not_found", f"old_string not found in {path}")
        replace_all = bool(args.get("replace_all", False))
        updated = content.replace(old, new) if replace_all else content.replace(old, new, 1)
        try:
            path.write_text(updated, encoding="utf-8")
        except OSError as exc:
            raise HostError("editor_error", f"cannot write {path}: {exc}") from exc
        return json_result(
            {
                "ok": True,
                "file": str(path),
                "replacements": count if replace_all else 1,
                "total_occurrences": count,
            }
        )

    def _tool_grep(self, args: dict[str, Any]) -> ToolResult:
        pattern = require_string(args, "pattern")
        root = self._path(str(args.get("path", ".")))
        flags = re.IGNORECASE if bool(args.get("case_insensitive", False)) else 0
        try:
            regex = re.compile(pattern, flags)
        except re.error as exc:
            raise HostError("invalid_arguments", f"invalid regex: {exc}") from exc
        context = integer_arg(args, "context", 0, minimum=0, maximum=100)
        limit = integer_arg(args, "max_results", 100, minimum=1, maximum=500)
        file_pattern = args.get("file_pattern")
        if file_pattern is not None and not isinstance(file_pattern, str):
            raise HostError("invalid_arguments", "file_pattern must be a string")
        files = [root] if root.is_file() else root.rglob("*")
        matches: list[dict[str, Any]] = []
        for file in files:
            if len(matches) >= limit:
                break
            if not file.is_file():
                continue
            try:
                relative = file.relative_to(root).as_posix() if root.is_dir() else file.name
            except ValueError:
                relative = str(file)
            if any(part in {".git", "node_modules", "dist"} for part in file.parts):
                continue
            if file_pattern and not fnmatch.fnmatch(relative, file_pattern):
                continue
            try:
                if file.stat().st_size > 10 * 1024 * 1024:
                    continue
                lines = file.read_text(encoding="utf-8").splitlines()
            except (OSError, UnicodeError):
                continue
            for index, line in enumerate(lines):
                if regex.search(line):
                    matches.append(
                        {
                            "file": str(file),
                            "line": index + 1,
                            "content": line,
                            "context_before": lines[max(0, index - context) : index],
                            "context_after": lines[index + 1 : index + context + 1],
                        }
                    )
                    if len(matches) >= limit:
                        break
        return json_result(
            {
                "pattern": pattern,
                "total_matches": len(matches),
                "truncated": len(matches) >= limit,
                "matches": matches,
            }
        )

    def _tool_glob(self, args: dict[str, Any]) -> ToolResult:
        pattern = require_string(args, "pattern")
        root = self._path(str(args.get("path", ".")))
        limit = integer_arg(args, "max_results", 200, minimum=1, maximum=500)
        try:
            candidates = [item for item in root.glob(pattern) if item.is_file()]
        except (OSError, ValueError) as exc:
            raise HostError("editor_error", f"cannot glob {root}: {exc}") from exc
        values: list[dict[str, Any]] = []
        for file in candidates:
            if any(part in {".git", "node_modules"} for part in file.parts):
                continue
            try:
                stat = file.stat()
            except OSError:
                continue
            values.append({"path": str(file), "mtime": stat.st_mtime, "size": stat.st_size})
        values.sort(key=lambda value: value["mtime"], reverse=True)
        return json_result(
            {
                "pattern": pattern,
                "base_path": str(root),
                "total_files": min(len(values), limit),
                "truncated": len(values) > limit,
                "files": values[:limit],
            }
        )

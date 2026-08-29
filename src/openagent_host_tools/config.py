"""Shared TOML registry for explicitly configured local MCP plugins."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .paths import HostPaths

_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


@dataclass(frozen=True)
class PluginSpec:
    name: str
    command: tuple[str, ...]
    env: dict[str, str] = field(default_factory=dict)
    cwd: str | None = None
    enabled: bool = True

    def validate(self) -> None:
        if not _NAME_RE.fullmatch(self.name):
            raise ValueError(
                f"invalid plugin name {self.name!r}; use lowercase letters, digits, '-' or '_'"
            )
        if not self.command or not all(isinstance(part, str) and part for part in self.command):
            raise ValueError(f"plugin {self.name!r} requires a non-empty command array")
        if not all(isinstance(k, str) and isinstance(v, str) for k, v in self.env.items()):
            raise ValueError(f"plugin {self.name!r} env must contain string values")

    @classmethod
    def from_wire(cls, raw: dict[str, Any]) -> "PluginSpec":
        spec = cls(
            name=str(raw.get("name", "")),
            command=tuple(raw.get("command") or ()),
            env=dict(raw.get("env") or {}),
            cwd=str(raw["cwd"]) if raw.get("cwd") else None,
            enabled=bool(raw.get("enabled", True)),
        )
        spec.validate()
        return spec

    def to_wire(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "name": self.name,
            "command": list(self.command),
            "enabled": self.enabled,
        }
        if self.env:
            value["env"] = self.env
        if self.cwd:
            value["cwd"] = self.cwd
        return value


class PluginConfigStore:
    def __init__(self, paths: HostPaths | None = None):
        self.paths = paths or HostPaths.discover()

    def load(self) -> list[PluginSpec]:
        path = self.paths.plugins
        if not path.exists():
            return []
        with path.open("rb") as fh:
            raw = tomllib.load(fh)
        if int(raw.get("version", 1)) != 1:
            raise ValueError("unsupported client-mcps.toml version")
        entries = raw.get("mcp", raw.get("plugins", []))
        specs = [PluginSpec.from_wire(item) for item in entries]
        names: set[str] = set()
        for spec in specs:
            if spec.name in names:
                raise ValueError(f"duplicate plugin name {spec.name!r}")
            names.add(spec.name)
        return specs

    def save(self, specs: list[PluginSpec]) -> None:
        for spec in specs:
            spec.validate()
        names = [spec.name for spec in specs]
        if len(names) != len(set(names)):
            raise ValueError("plugin names must be unique")
        self.paths.ensure()
        lines = [
            "# Local MCPs shared by OpenAgent Desktop and CLI.",
            "# Only entries explicitly present here are launched.",
            "version = 1",
            "",
        ]
        for spec in specs:
            lines.extend(
                [
                    "[[mcp]]",
                    f"name = {_toml_value(spec.name)}",
                    f"command = {_toml_value(list(spec.command))}",
                    f"enabled = {'true' if spec.enabled else 'false'}",
                ]
            )
            if spec.cwd:
                lines.append(f"cwd = {_toml_value(spec.cwd)}")
            if spec.env:
                lines.append(f"env = {_toml_value(spec.env)}")
            lines.append("")
        _atomic_text(self.paths.plugins, "\n".join(lines))

    def ensure_example(self) -> Path:
        """Create an empty registry only when no user registry exists."""
        if not self.paths.plugins.exists():
            self.save([])
        return self.paths.plugins


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    if isinstance(value, dict):
        return "{" + ", ".join(
            f"{_toml_key(str(key))} = {_toml_value(val)}" for key, val in value.items()
        ) + "}"
    return str(value)


def _toml_key(value: str) -> str:
    return value if re.fullmatch(r"[A-Za-z0-9_-]+", value) else _toml_value(value)


def _atomic_text(path: Path, text: str) -> None:
    import os
    import tempfile

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        if os.name != "nt":
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            if not text.endswith("\n"):
                fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise

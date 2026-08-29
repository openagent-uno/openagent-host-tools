"""Fail-closed, durable device-level consent."""

from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from .paths import HostPaths

CONSENT_VERSION = 1


@dataclass(frozen=True)
class ConsentState:
    enabled: bool = False
    version: int = CONSENT_VERSION
    updated_at: float | None = None
    granted_at: float | None = None

    def to_wire(self) -> dict[str, object]:
        return asdict(self)


class ConsentStore:
    def __init__(self, paths: HostPaths | None = None):
        self.paths = paths or HostPaths.discover()

    def load(self) -> ConsentState:
        path = self.paths.consent
        if not path.exists():
            return ConsentState()
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            version = int(raw.get("version", 0))
            if version != CONSENT_VERSION:
                return ConsentState(version=CONSENT_VERSION)
            return ConsentState(
                enabled=bool(raw.get("enabled", False)),
                version=version,
                updated_at=_float_or_none(raw.get("updated_at")),
                granted_at=_float_or_none(raw.get("granted_at")),
            )
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return ConsentState()

    def set_enabled(self, enabled: bool, *, version: int = CONSENT_VERSION) -> ConsentState:
        if version != CONSENT_VERSION:
            raise ValueError(f"unsupported consent version {version}")
        previous = self.load()
        now = time.time()
        state = ConsentState(
            enabled=bool(enabled),
            version=version,
            updated_at=now,
            granted_at=(previous.granted_at or now) if enabled else previous.granted_at,
        )
        self.paths.ensure()
        _atomic_json(self.paths.consent, state.to_wire())
        return state


def _float_or_none(value: object) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        if os.name != "nt":
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(value, fh, indent=2, sort_keys=True)
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

"""Platform-specific paths for local host-tool state."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def default_home() -> Path:
    override = os.environ.get("OPENAGENT_HOST_TOOLS_HOME")
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / ".openagent" / "user"


@dataclass(frozen=True)
class HostPaths:
    home: Path

    @classmethod
    def discover(cls, home: str | Path | None = None) -> "HostPaths":
        return cls(Path(home).expanduser().resolve() if home is not None else default_home())

    @property
    def consent(self) -> Path:
        return self.home / "client-tools-consent.json"

    @property
    def plugins(self) -> Path:
        return self.home / "client-mcps.toml"

    @property
    def internal(self) -> Path:
        return self.home / "host-tools"

    @property
    def state_db(self) -> Path:
        return self.internal / "state.sqlite3"

    @property
    def audit_db(self) -> Path:
        return self.internal / "audit.sqlite3"

    @property
    def broker_authkey(self) -> Path:
        return self.internal / "broker-authkey"

    @property
    def broker_pid(self) -> Path:
        return self.internal / "broker.pid"

    def ensure(self) -> None:
        self.home.mkdir(parents=True, exist_ok=True)
        self.internal.mkdir(parents=True, exist_ok=True)
        if os.name != "nt":
            try:
                self.home.chmod(0o700)
                self.internal.chmod(0o700)
            except OSError:
                pass

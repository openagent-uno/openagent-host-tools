"""Local append-only audit records for capability calls."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path
from typing import Any


class AuditLedger:
    """SQLite-backed audit which records metadata, not argument values."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS audit (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    call_id TEXT NOT NULL,
                    principal TEXT NOT NULL,
                    server TEXT NOT NULL,
                    tool TEXT NOT NULL,
                    classification TEXT,
                    outcome TEXT NOT NULL,
                    duration_ms INTEGER,
                    argument_keys TEXT NOT NULL,
                    error_code TEXT,
                    replayed INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            columns = {row[1] for row in conn.execute("PRAGMA table_info(audit)")}
            if "arguments_sha256" not in columns:
                conn.execute("ALTER TABLE audit ADD COLUMN arguments_sha256 TEXT")
            if "target" not in columns:
                conn.execute(
                    "ALTER TABLE audit ADD COLUMN target TEXT NOT NULL DEFAULT 'client'"
                )

    async def append(
        self,
        *,
        call_id: str,
        principal: str,
        server: str,
        tool: str,
        classification: str | None,
        outcome: str,
        argument_keys: list[str],
        duration_ms: int | None = None,
        error_code: str | None = None,
        replayed: bool = False,
        arguments_sha256: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._append_sync,
            call_id,
            principal,
            server,
            tool,
            classification,
            outcome,
            sorted(argument_keys),
            duration_ms,
            error_code,
            replayed,
            arguments_sha256,
        )

    def _append_sync(
        self,
        call_id: str,
        principal: str,
        server: str,
        tool: str,
        classification: str | None,
        outcome: str,
        argument_keys: list[str],
        duration_ms: int | None,
        error_code: str | None,
        replayed: bool,
        arguments_sha256: str | None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO audit(timestamp,call_id,principal,server,tool,classification,"
                "outcome,duration_ms,argument_keys,error_code,replayed,arguments_sha256,target) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    time.time(),
                    call_id,
                    principal,
                    server,
                    tool,
                    classification,
                    outcome,
                    duration_ms,
                    json.dumps(argument_keys, separators=(",", ":")),
                    error_code,
                    int(replayed),
                    arguments_sha256,
                    "client",
                ),
            )

    async def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._recent_sync, max(1, min(int(limit), 500)))

    def _recent_sync(self, limit: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM audit ORDER BY seq DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

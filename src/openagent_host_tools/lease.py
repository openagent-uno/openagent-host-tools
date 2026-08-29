"""Cross-process single-writer lease for mutating local tool calls."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path
from typing import Any

from .types import HostError


class MutatingLease:
    """A machine-local, single-call exclusive lease stored in SQLite.

    Desktop and CLI may each launch their own host sidecar. Keeping the lease in
    their shared state database prevents two client instances from mutating the
    same computer concurrently. Expiry is wall-clock based because the state is
    shared across processes; active calls renew well before the deadline.
    """

    def __init__(self, path: Path, *, lease_seconds: float = 15.0):
        self.path = path
        self.lease_seconds = max(3.0, float(lease_seconds))
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
                CREATE TABLE IF NOT EXISTS mutating_lease (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    holder TEXT NOT NULL,
                    acquired_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS mutating_lease_calls (
                    call_id TEXT PRIMARY KEY,
                    holder TEXT NOT NULL,
                    tool TEXT NOT NULL,
                    started_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )

    async def enter(self, principal: str, call_id: str, tool: str) -> None:
        await asyncio.to_thread(self._enter_sync, principal, call_id, tool)

    def _enter_sync(self, principal: str, call_id: str, tool: str) -> None:
        now = time.time()
        expires = now + self.lease_seconds
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._expire_locked(conn, now)
            row = conn.execute(
                "SELECT holder, acquired_at, expires_at FROM mutating_lease WHERE singleton=1"
            ).fetchone()
            active_call = None
            if row is not None:
                active_call = conn.execute(
                    "SELECT call_id FROM mutating_lease_calls LIMIT 1"
                ).fetchone()
            same_call = (
                row is not None
                and row["holder"] == principal
                and active_call is not None
                and active_call["call_id"] == call_id
            )
            if row is not None and not same_call:
                raise HostError(
                    "lease_held",
                    "another local tool call is currently mutating this computer",
                    {
                        "holder": row["holder"],
                        "call_id": active_call["call_id"] if active_call else None,
                        "ttl_ms": max(0, int((row["expires_at"] - now) * 1000)),
                    },
                )
            if row is None:
                conn.execute(
                    "INSERT INTO mutating_lease(singleton, holder, acquired_at, expires_at) "
                    "VALUES(1, ?, ?, ?)",
                    (principal, now, expires),
                )
            else:
                conn.execute(
                    "UPDATE mutating_lease SET expires_at=? WHERE singleton=1",
                    (expires,),
                )
            conn.execute(
                "INSERT OR REPLACE INTO mutating_lease_calls"
                "(call_id, holder, tool, started_at, expires_at) VALUES(?,?,?,?,?)",
                (call_id, principal, tool, now, expires),
            )

    async def renew(self, principal: str, call_id: str) -> bool:
        return await asyncio.to_thread(self._renew_sync, principal, call_id)

    def _renew_sync(self, principal: str, call_id: str) -> bool:
        now = time.time()
        expires = now + self.lease_seconds
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._expire_locked(conn, now)
            row = conn.execute(
                "SELECT holder FROM mutating_lease_calls WHERE call_id=?", (call_id,)
            ).fetchone()
            if row is None or row["holder"] != principal:
                return False
            conn.execute(
                "UPDATE mutating_lease_calls SET expires_at=? WHERE call_id=?",
                (expires, call_id),
            )
            conn.execute(
                "UPDATE mutating_lease SET expires_at=? WHERE singleton=1 AND holder=?",
                (expires, principal),
            )
            return True

    async def leave(self, principal: str, call_id: str) -> None:
        await asyncio.to_thread(self._leave_sync, principal, call_id)

    def _leave_sync(self, principal: str, call_id: str) -> None:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "DELETE FROM mutating_lease_calls WHERE call_id=? AND holder=?",
                (call_id, principal),
            )
            row = conn.execute(
                "SELECT MAX(expires_at) AS expiry FROM mutating_lease_calls WHERE holder=?",
                (principal,),
            ).fetchone()
            expiry = row["expiry"] if row is not None else None
            if expiry is None:
                # No mutation remains in flight: hand the machine to another
                # local client immediately. The lease exists to survive a
                # process crash during a call, not to impose an idle cooldown.
                conn.execute("DELETE FROM mutating_lease WHERE holder=?", (principal,))
            else:
                conn.execute(
                    "UPDATE mutating_lease SET expires_at=? WHERE singleton=1 AND holder=?",
                    (expiry, principal),
                )

    async def release_principal(self, principal: str) -> None:
        await asyncio.to_thread(self._release_sync, principal)

    def _release_sync(self, principal: str) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM mutating_lease_calls WHERE holder=?", (principal,))
            conn.execute("DELETE FROM mutating_lease WHERE holder=?", (principal,))

    async def status(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._status_sync)

    def _status_sync(self) -> dict[str, Any]:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._expire_locked(conn, now)
            row = conn.execute(
                "SELECT holder, acquired_at, expires_at FROM mutating_lease WHERE singleton=1"
            ).fetchone()
            if row is None:
                return {"state": "free", "holder": None, "ttl_ms": 0, "inflight": []}
            calls = conn.execute(
                "SELECT call_id, tool, started_at FROM mutating_lease_calls "
                "WHERE holder=? ORDER BY started_at",
                (row["holder"],),
            ).fetchall()
            return {
                "state": "held",
                "holder": row["holder"],
                "acquired_at": row["acquired_at"],
                "ttl_ms": max(0, int((row["expires_at"] - now) * 1000)),
                "inflight": [
                    {"call_id": item["call_id"], "tool": item["tool"]} for item in calls
                ],
            }

    @staticmethod
    def _expire_locked(conn: sqlite3.Connection, now: float) -> None:
        row = conn.execute(
            "SELECT expires_at FROM mutating_lease WHERE singleton=1"
        ).fetchone()
        if row is not None and row["expires_at"] <= now:
            conn.execute("DELETE FROM mutating_lease_calls")
            conn.execute("DELETE FROM mutating_lease WHERE singleton=1")

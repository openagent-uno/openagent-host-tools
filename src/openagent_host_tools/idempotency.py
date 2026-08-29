"""Durable at-most-once ledger for mutating tool calls."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from .types import HostError, ToolResult


@dataclass(frozen=True)
class Claim:
    state: Literal["new", "replay"]
    result: ToolResult | None = None


class IdempotencyLedger:
    """A process-safe ledger that never guesses after a crash.

    An expired in-flight mutation becomes ``indeterminate`` and is not executed
    again automatically. This is stricter than a memory cache: a client retry
    cannot duplicate a mutation merely because the host restarted.
    """

    def __init__(
        self,
        path: Path,
        *,
        result_ttl_seconds: float = 24 * 3600,
        inflight_lease_seconds: float = 30.0,
    ):
        self.path = path
        self.result_ttl_seconds = max(60.0, float(result_ttl_seconds))
        self.inflight_lease_seconds = max(5.0, float(inflight_lease_seconds))
        self.owner_id = uuid.uuid4().hex
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
                CREATE TABLE IF NOT EXISTS idempotency (
                    principal TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    state TEXT NOT NULL,
                    owner_id TEXT,
                    result_json TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    lease_until REAL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY(principal, idem_key)
                )
                """
            )

    @staticmethod
    def fingerprint(server: str, tool: str, args: dict[str, Any]) -> str:
        canonical = json.dumps(
            {"server": server, "tool": tool, "args": _normalize_json_numbers(args)},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    @staticmethod
    def arguments_sha256(args: dict[str, Any]) -> str:
        normalized = _normalize_json_numbers(args)
        canonical = json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()
    async def claim(
        self,
        principal: str,
        key: str,
        *,
        server: str,
        tool: str,
        args: dict[str, Any],
        retry_stale: bool = False,
    ) -> Claim:
        return await asyncio.to_thread(
            self._claim_sync,
            principal,
            key,
            self.fingerprint(server, tool, args),
            retry_stale,
        )

    def _claim_sync(
        self, principal: str, key: str, fingerprint: str, retry_stale: bool
    ) -> Claim:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "DELETE FROM idempotency WHERE state='done' AND expires_at <= ?", (now,)
            )
            row = conn.execute(
                "SELECT * FROM idempotency WHERE principal=? AND idem_key=?",
                (principal, key),
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO idempotency"
                    "(principal,idem_key,fingerprint,state,owner_id,result_json,created_at,"
                    "updated_at,lease_until,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        principal,
                        key,
                        fingerprint,
                        "executing",
                        self.owner_id,
                        None,
                        now,
                        now,
                        now + self.inflight_lease_seconds,
                        now + self.result_ttl_seconds,
                    ),
                )
                return Claim("new")
            if row["fingerprint"] != fingerprint:
                raise HostError(
                    "idempotency_conflict",
                    "idempotency key was already used for different arguments",
                    {"idempotency_key": key},
                )
            if row["state"] == "done":
                return Claim("replay", ToolResult.from_wire(json.loads(row["result_json"])))
            if (
                row["state"] == "executing"
                and retry_stale
                and row["owner_id"] != self.owner_id
            ):
                # A restarted singleton broker has a new owner id. Safe calls
                # may be taken over immediately so the Gateway's exact retry is
                # not stranded behind the dead owner's 30-second lease.
                # Mutations never set retry_stale and keep strict ambiguity.
                conn.execute(
                    "UPDATE idempotency SET owner_id=?, updated_at=?, lease_until=?, "
                    "expires_at=? WHERE principal=? AND idem_key=?",
                    (
                        self.owner_id,
                        now,
                        now + self.inflight_lease_seconds,
                        now + self.result_ttl_seconds,
                        principal,
                        key,
                    ),
                )
                return Claim("new")
            if row["state"] == "executing" and float(row["lease_until"] or 0) > now:
                raise HostError(
                    "idempotency_in_flight",
                    "a call with this idempotency key is already running",
                    {"idempotency_key": key, "retry_after_ms": 1000},
                )
            if row["state"] == "executing" and retry_stale:
                conn.execute(
                    "UPDATE idempotency SET state='executing', owner_id=?, updated_at=?, "
                    "lease_until=?, expires_at=? WHERE principal=? AND idem_key=?",
                    (
                        self.owner_id,
                        now,
                        now + self.inflight_lease_seconds,
                        now + self.result_ttl_seconds,
                        principal,
                        key,
                    ),
                )
                return Claim("new")
            if row["state"] == "executing":
                conn.execute(
                    "UPDATE idempotency SET state='indeterminate', updated_at=? "
                    "WHERE principal=? AND idem_key=?",
                    (now, principal, key),
                )
            raise HostError(
                "idempotency_indeterminate",
                "the previous mutation may have completed before its result was recorded",
                {"idempotency_key": key, "manual_reconciliation_required": True},
            )

    async def renew(self, principal: str, key: str) -> bool:
        return await asyncio.to_thread(self._renew_sync, principal, key)

    def _renew_sync(self, principal: str, key: str) -> bool:
        now = time.time()
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE idempotency SET lease_until=?, updated_at=? "
                "WHERE principal=? AND idem_key=? AND state='executing' AND owner_id=?",
                (
                    now + self.inflight_lease_seconds,
                    now,
                    principal,
                    key,
                    self.owner_id,
                ),
            )
            return cur.rowcount == 1

    async def complete(self, principal: str, key: str, result: ToolResult) -> None:
        await asyncio.to_thread(self._complete_sync, principal, key, result.to_wire())

    def _complete_sync(self, principal: str, key: str, result: dict[str, Any]) -> None:
        now = time.time()
        payload = json.dumps(result, separators=(",", ":"), ensure_ascii=False)
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE idempotency SET state='done', result_json=?, updated_at=?, "
                "lease_until=NULL, expires_at=? WHERE principal=? AND idem_key=? "
                "AND state='executing' AND owner_id=?",
                (
                    payload,
                    now,
                    now + self.result_ttl_seconds,
                    principal,
                    key,
                    self.owner_id,
                ),
            )
            if cur.rowcount != 1:
                raise HostError("idempotency_lost", "idempotency claim was lost")

    async def abandon(self, principal: str, key: str) -> None:
        """Remove a claim only when dispatch was rejected before tool execution."""
        await asyncio.to_thread(self._abandon_sync, principal, key)

    def _abandon_sync(self, principal: str, key: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM idempotency WHERE principal=? AND idem_key=? "
                "AND state='executing' AND owner_id=?",
                (principal, key, self.owner_id),
            )

    async def mark_indeterminate(self, principal: str, key: str) -> None:
        """Persist that dispatch may have produced an effect without a result."""
        await asyncio.to_thread(self._mark_indeterminate_sync, principal, key)

    def _mark_indeterminate_sync(self, principal: str, key: str) -> None:
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                "UPDATE idempotency SET state='indeterminate', updated_at=?, "
                "lease_until=NULL WHERE principal=? AND idem_key=? "
                "AND state='executing' AND owner_id=?",
                (now, principal, key, self.owner_id),
            )


def _normalize_json_numbers(value: Any) -> Any:
    """Canonical numeric normalization shared with server and JS clients."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise HostError("invalid_arguments", "NaN and Infinity are not valid tool arguments")
        if value == 0.0:
            return 0
        if value.is_integer():
            return int(value)
        return value
    if isinstance(value, list):
        return [_normalize_json_numbers(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise HostError("invalid_arguments", "tool argument object keys must be strings")
        return {key: _normalize_json_numbers(item) for key, item in value.items()}
    raise HostError(
        "invalid_arguments", f"tool arguments contain unsupported value {type(value).__name__}"
    )

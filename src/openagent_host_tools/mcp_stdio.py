"""Minimal MCP stdio client used for configured plugins and optional sidecars."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import socket
from dataclasses import replace
from pathlib import Path
from typing import Any

from ._version import __version__
from .config import PluginSpec
from .context import current_principal
from .types import (
    HostError,
    ServerManifest,
    ToolClassification,
    ToolManifest,
    ToolResult,
)

_MCP_PROTOCOL_VERSION = "2024-11-05"


class MCPStdioServer:
    def __init__(
        self,
        spec: PluginSpec,
        *,
        placeholder: ServerManifest | None = None,
        startup_timeout: float = 20.0,
    ):
        self.spec = spec
        self.placeholder = placeholder
        self.startup_timeout = startup_timeout
        self.manifest = placeholder or ServerManifest(spec.name, "unknown", "", ())
        self.process: asyncio.subprocess.Process | None = None
        self._reader: asyncio.Task[None] | None = None
        self._stderr_reader: asyncio.Task[None] | None = None
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._write_lock = asyncio.Lock()
        self._next_id = 0
        self._stderr_tail = ""
        self.raw_initialize: dict[str, Any] | None = None

    async def start(self) -> None:
        if self.process is not None:
            return
        env = os.environ.copy()
        env.update(self.spec.env)
        try:
            self.process = await asyncio.create_subprocess_exec(
                *self.spec.command,
                cwd=self.spec.cwd,
                env=env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as exc:
            raise HostError(
                "plugin_unavailable", f"cannot start MCP {self.spec.name!r}: {exc}"
            ) from exc
        self._reader = asyncio.create_task(self._read_loop(), name=f"mcp-read-{self.spec.name}")
        self._stderr_reader = asyncio.create_task(
            self._read_stderr(), name=f"mcp-stderr-{self.spec.name}"
        )
        try:
            initialized = await asyncio.wait_for(
                self._request(
                    "initialize",
                    {
                        "protocolVersion": _MCP_PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {
                            "name": "openagent-host-tools",
                            "version": __version__,
                        },
                    },
                ),
                timeout=self.startup_timeout,
            )
            self.raw_initialize = dict(initialized)
            server_info = initialized.get("serverInfo") or {}
            if self.placeholder is not None and server_info.get("name") != self.placeholder.name:
                raise HostError(
                    "plugin_identity_mismatch",
                    f"MCP {self.spec.name!r} reported serverInfo.name {server_info.get('name')!r}",
                )
            await self._notify("notifications/initialized", {})
            listed = await asyncio.wait_for(
                self._request("tools/list", {}), timeout=self.startup_timeout
            )
        except Exception:
            await self.close()
            raise
        server_info = initialized.get("serverInfo") or {}
        tools = tuple(self._tool_manifest(raw) for raw in listed.get("tools", []))
        instructions = str(initialized.get("instructions") or "")
        base = self.placeholder or self.manifest
        self.manifest = replace(
            base,
            name=self.spec.name,
            version=str(server_info.get("version") or "unknown"),
            instructions=instructions or base.instructions,
            tools=tools,
            available=True,
            unavailable_reason=None,
        )

    def _tool_manifest(self, raw: dict[str, Any]) -> ToolManifest:
        annotations = raw.get("annotations") or {}
        read_only = annotations.get("readOnlyHint") is True
        idempotent = annotations.get("idempotentHint") is True
        classification = (
            ToolClassification.READ_ONLY
            if read_only
            else ToolClassification.IDEMPOTENT
            if idempotent
            else ToolClassification.MUTATING
        )
        name = str(raw.get("name") or "")
        fallback = self.placeholder.tool(name) if self.placeholder is not None else None
        return ToolManifest(
            name=name,
            description=str(raw.get("description") or ""),
            input_schema=dict(raw.get("inputSchema") or {"type": "object"}),
            classification=classification,
            classification_by_argument=(
                fallback.classification_by_argument if fallback is not None else {}
            ),
        )

    async def call(self, tool: str, args: dict[str, Any]) -> ToolResult:
        if self.process is None:
            raise HostError("plugin_unavailable", f"MCP {self.spec.name!r} is not running")
        result = await self._request("tools/call", {"name": tool, "arguments": args})
        return ToolResult.from_wire(result)

    async def close(self) -> None:
        process = self.process
        self.process = None
        if process is not None and process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=3)
            except TimeoutError:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()
        for task in (self._reader, self._stderr_reader):
            if task is not None and not task.done():
                task.cancel()
        for future in self._pending.values():
            if not future.done():
                future.set_exception(HostError("plugin_disconnected", self._disconnect_message()))
        self._pending.clear()

    async def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if self.process is None or self.process.stdin is None:
            raise HostError("plugin_disconnected", self._disconnect_message())
        self._next_id += 1
        request_id = self._next_id
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        await self._send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        try:
            return await future
        except asyncio.CancelledError:
            await self._notify("notifications/cancelled", {"requestId": request_id})
            raise
        finally:
            self._pending.pop(request_id, None)

    async def _notify(self, method: str, params: dict[str, Any]) -> None:
        await self._send({"jsonrpc": "2.0", "method": method, "params": params})

    async def _send(self, value: dict[str, Any]) -> None:
        process = self.process
        if process is None or process.stdin is None or process.returncode is not None:
            raise HostError("plugin_disconnected", self._disconnect_message())
        data = (json.dumps(value, separators=(",", ":")) + "\n").encode()
        async with self._write_lock:
            process.stdin.write(data)
            await process.stdin.drain()

    async def _read_loop(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        try:
            while True:
                line = await self.process.stdout.readline()
                if not line:
                    break
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                request_id = value.get("id")
                if request_id is None:
                    continue
                future = self._pending.get(int(request_id))
                if future is None or future.done():
                    continue
                if "error" in value:
                    error = value.get("error") or {}
                    future.set_exception(
                        HostError(
                            "plugin_error",
                            str(error.get("message") or "MCP request failed"),
                            {"mcp_code": error.get("code"), "mcp_data": error.get("data")},
                        )
                    )
                else:
                    future.set_result(dict(value.get("result") or {}))
        except asyncio.CancelledError:
            raise
        finally:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(
                        HostError("plugin_disconnected", self._disconnect_message())
                    )

    async def _read_stderr(self) -> None:
        assert self.process is not None and self.process.stderr is not None
        try:
            while True:
                chunk = await self.process.stderr.read(4096)
                if not chunk:
                    break
                self._stderr_tail = (self._stderr_tail + chunk.decode(errors="replace"))[-8192:]
        except asyncio.CancelledError:
            raise

    def _disconnect_message(self) -> str:
        suffix = f": {self._stderr_tail.strip()}" if self._stderr_tail.strip() else ""
        return f"MCP {self.spec.name!r} disconnected{suffix}"


class PerPrincipalMCPPool:
    """Lazy MCP stdio pool isolated by certified network and account.

    Agent-in-Chrome must never share a browser profile, extensions directory,
    or CDP port between networks, even when an account identifier is reused.
    Release closes only that network/account's sidecar while retaining its
    profile for the next connection.
    """

    def __init__(
        self,
        spec: PluginSpec,
        *,
        placeholder: ServerManifest,
        data_root: str | Path,
    ):
        self.spec = spec
        self.placeholder = placeholder
        self.data_root = Path(data_root).expanduser().resolve()
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.manifest = replace(
            placeholder,
            available=True,
            unavailable_reason=None,
            data_directory=str(self.data_root),
        )
        self._instances: dict[str, MCPStdioServer] = {}
        self._ports: dict[str, _PortLease] = {}
        self._principal_accounts: dict[str, str] = {}
        self._account_principals: dict[str, set[str]] = {}
        self._guard = asyncio.Lock()

    async def start(self) -> None:
        """Probe the real MCP handshake/catalog in an isolated throwaway slot."""
        probe_key = "catalog-probe"
        adapter, lease = self._build(probe_key)
        try:
            await adapter.start()
            self.manifest = replace(
                adapter.manifest,
                platforms=self.placeholder.platforms,
                os_requirements=self.placeholder.os_requirements,
                data_directory=str(self.data_root),
                available=True,
                unavailable_reason=None,
            )
        finally:
            await adapter.close()
            lease.close()

    async def call(self, tool: str, args: dict[str, Any]) -> ToolResult:
        principal = current_principal.get()
        key = _chrome_principal_key(principal)
        assert principal is not None
        async with self._guard:
            self._principal_accounts[principal] = key
            self._account_principals.setdefault(key, set()).add(principal)
            adapter = self._instances.get(key)
            if adapter is None:
                adapter, lease = self._build(key)
                try:
                    await adapter.start()
                except Exception:
                    lease.close()
                    raise
                self._instances[key] = adapter
                self._ports[key] = lease
        return await adapter.call(tool, args)

    async def release_principal(self, principal: str) -> None:
        async with self._guard:
            key = self._principal_accounts.pop(principal, None)
            if key is None:
                return
            principals = self._account_principals.get(key)
            if principals is not None:
                principals.discard(principal)
                if principals:
                    return
                self._account_principals.pop(key, None)
            adapter = self._instances.pop(key, None)
            lease = self._ports.pop(key, None)
        if adapter is not None:
            await adapter.close()
        if lease is not None:
            lease.close()

    async def close(self) -> None:
        async with self._guard:
            instances = list(self._instances.values())
            leases = list(self._ports.values())
            self._instances.clear()
            self._ports.clear()
            self._principal_accounts.clear()
            self._account_principals.clear()
        await asyncio.gather(*(adapter.close() for adapter in instances), return_exceptions=True)
        for lease in leases:
            lease.close()

    def _build(self, key: str) -> tuple[MCPStdioServer, "_PortLease"]:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        principal_root = self.data_root / "principals" / digest[:24]
        profile = principal_root / "profile"
        extensions = principal_root / "extensions"
        profile.mkdir(parents=True, exist_ok=True)
        extensions.mkdir(parents=True, exist_ok=True)
        lease = _PortLease.acquire(self.data_root / "ports", key)
        env = {
            **self.spec.env,
            "OPENAGENT_CHROME_PROFILE_DIR": str(profile),
            "OPENAGENT_CHROME_EXTENSIONS_DIR": str(extensions),
            "OPENAGENT_CHROME_CDP_PORT": str(lease.port),
        }
        isolated = replace(self.spec, env=env)
        return MCPStdioServer(isolated, placeholder=self.placeholder), lease


def _chrome_principal_key(principal: str | None) -> str:
    if not principal:
        raise HostError(
            "account_context_required",
            "agent-in-chrome requires a certified local account principal",
        )
    try:
        value = json.loads(principal)
    except (TypeError, json.JSONDecodeError) as exc:
        raise HostError(
            "account_context_required",
            "agent-in-chrome cannot run without an isolated account id",
        ) from exc
    if (
        not isinstance(value, dict)
        or not isinstance(value.get("account_id"), (str, int))
        or not str(value.get("account_id")).strip()
    ):
        raise HostError(
            "account_context_required",
            "agent-in-chrome cannot run without an isolated account id",
        )
    if (
        not isinstance(value.get("network_id"), (str, int))
        or not str(value.get("network_id")).strip()
    ):
        raise HostError(
            "network_context_required",
            "agent-in-chrome cannot run without a certified network id",
        )
    return json.dumps(
        {
            "account_id": str(value["account_id"]).strip(),
            "network_id": str(value["network_id"]).strip(),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


class _PortLease:
    def __init__(self, port: int, handle):
        self.port = port
        self._handle = handle

    @classmethod
    def acquire(cls, directory: Path, key: str) -> "_PortLease":
        directory.mkdir(parents=True, exist_ok=True)
        start = 28000 + int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % 18000
        for offset in range(512):
            port = 28000 + ((start - 28000 + offset) % 18000)
            if not _port_is_free(port):
                continue
            handle = open(directory / f"{port}.lock", "a+b")
            try:
                if os.name == "nt":  # pragma: no cover - Windows CI
                    import msvcrt

                    handle.seek(0)
                    if handle.read(1) == b"":
                        handle.write(b"0")
                        handle.flush()
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                handle.close()
                continue
            return cls(port, handle)
        raise HostError("chrome_port_unavailable", "no isolated Chrome CDP port is available")

    def close(self) -> None:
        if self._handle is None:
            return
        try:
            if os.name == "nt":  # pragma: no cover - Windows CI
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None


def _port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True

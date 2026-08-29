"""Foreground/background shell contract shared with the OpenAgent server."""

from __future__ import annotations

import asyncio
import inspect
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..context import current_principal
from ..shell_core import BackgroundShell
from ..types import HostError, ServerManifest, ToolClassification, ToolManifest, ToolResult
from ._util import integer_arg, json_result, require_string

DEFAULT_TIMEOUT_MS = 120_000
MAX_TIMEOUT_MS = 1_800_000
_MAX_STREAM_BYTES = 1_000_000


def _schema(properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": required or [],
        "additionalProperties": False,
    }


@dataclass
class _Background:
    shell: BackgroundShell
    principal: str | None
    description: str | None = None
    stdout_cursor: int = 0
    stderr_cursor: int = 0
    reader: asyncio.Task[None] | None = None

    @property
    def shell_id(self) -> str:
        return self.shell.shell_id

    @property
    def command(self) -> str:
        return self.shell.command


class ShellServer:
    """Run the six canonical shell tools in the current process.

    Background resource visibility is additionally scoped to the authenticated
    local principal. ``session_id`` remains in the shared public contract for
    server parity but is never used as an authorization selector here.
    """

    manifest = ServerManifest(
        name="shell",
        version="1.0.0",
        instructions=(
            "Execute commands on the current host. In a client capability host these are "
            "client-local; in the server adapter they are server-local. Timeout is in milliseconds."
        ),
        tools=(
            ToolManifest(
                "shell_exec",
                "Execute a shell command. Returns foreground output, or a shell_id when run_in_background=True.",
                _schema(
                    {
                        "command": {"type": "string"},
                        "cwd": {"type": "string"},
                        "env": {"type": "object", "additionalProperties": {"type": "string"}},
                        "timeout": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": MAX_TIMEOUT_MS,
                            "default": DEFAULT_TIMEOUT_MS,
                            "description": "Timeout in milliseconds.",
                        },
                        "run_in_background": {"type": "boolean", "default": False},
                        "stdin": {"type": "string"},
                        "description": {"type": "string"},
                    },
                    ["command"],
                ),
                ToolClassification.MUTATING,
            ),
            ToolManifest(
                "shell_output",
                "Read new output from a background shell since the last call.",
                _schema(
                    {
                        "shell_id": {"type": "string"},
                        "filter": {"type": "string"},
                        "since_last": {"type": "boolean", "default": True},
                    },
                    ["shell_id"],
                ),
                ToolClassification.MUTATING,
            ),
            ToolManifest(
                "shell_input",
                "Write text to a running background shell's stdin.",
                _schema(
                    {
                        "shell_id": {"type": "string"},
                        "text": {"type": "string"},
                        "press_enter": {"type": "boolean", "default": True},
                    },
                    ["shell_id", "text"],
                ),
                ToolClassification.MUTATING,
            ),
            ToolManifest(
                "shell_kill",
                "Kill a background shell by id.",
                _schema(
                    {
                        "shell_id": {"type": "string"},
                        "signal": {
                            "type": "string",
                            "enum": ["TERM", "INT", "KILL"],
                            "default": "TERM",
                        },
                    },
                    ["shell_id"],
                ),
                ToolClassification.MUTATING,
            ),
            ToolManifest(
                "shell_list",
                "List active and recently-completed background shells.",
                _schema({"session_id": {"type": "string"}}),
            ),
            ToolManifest(
                "shell_which",
                "Check whether a command is available on PATH.",
                _schema({"command": {"type": "string"}}, ["command"]),
            ),
        ),
    )

    def __init__(self, cwd: str | Path | None = None, *, event_sink=None):
        self.cwd = Path(cwd or Path.cwd()).expanduser().resolve()
        self._background: dict[str, _Background] = {}
        self._guard = asyncio.Lock()
        self._event_sink = event_sink

    async def call(self, tool: str, args: dict[str, Any]) -> ToolResult:
        handler = getattr(self, f"_tool_{tool}", None)
        if handler is None:
            raise HostError("tool_not_found", f"shell has no tool {tool!r}")
        return await handler(args)

    async def close(self) -> None:
        async with self._guard:
            jobs = list(self._background.values())
        await self._stop_jobs(jobs)

    async def release_principal(self, principal: str) -> None:
        async with self._guard:
            jobs = [job for job in self._background.values() if job.principal == principal]
        await self._stop_jobs(jobs)
        async with self._guard:
            for job in jobs:
                self._background.pop(job.shell_id, None)

    async def _stop_jobs(self, jobs: list[_Background]) -> None:
        for job in jobs:
            if job.shell.is_running:
                await job.shell.kill(signal_name="KILL", grace_seconds=0)
            if job.reader is not None:
                await asyncio.gather(job.reader, return_exceptions=True)
            else:
                await job.shell.finalise()

    async def _tool_shell_exec(self, args: dict[str, Any]) -> ToolResult:
        command = require_string(args, "command")
        cwd = Path(str(args.get("cwd") or self.cwd)).expanduser().resolve()
        env_arg = args.get("env") or {}
        if not isinstance(env_arg, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in env_arg.items()
        ):
            raise HostError("invalid_arguments", "env must be an object of string values")
        timeout_ms = integer_arg(
            args, "timeout", DEFAULT_TIMEOUT_MS, minimum=1, maximum=MAX_TIMEOUT_MS
        )
        stdin_text = args.get("stdin")
        if stdin_text is not None and not isinstance(stdin_text, str):
            raise HostError("invalid_arguments", "stdin must be a string")
        shell_id = f"sh_{uuid.uuid4().hex[:6]}"
        shell = BackgroundShell(
            shell_id=shell_id,
            command=command,
            cwd=str(cwd),
            env=env_arg,
        )
        if bool(args.get("run_in_background", False)):
            description = args.get("description")
            if description is not None and not isinstance(description, str):
                raise HostError("invalid_arguments", "description must be a string")
            try:
                await shell.start()
            except (OSError, ValueError) as exc:
                raise HostError("shell_error", f"cannot start command: {exc}") from exc
            job = _Background(
                shell=shell,
                principal=current_principal.get(),
                description=description,
            )
            job.reader = asyncio.create_task(self._pump(job), name=f"host-shell-{shell_id}")
            async with self._guard:
                self._background[shell_id] = job
            if stdin_text:
                await shell.write_stdin(stdin_text, press_enter=False)
            return json_result(
                {
                    "shell_id": shell_id,
                    "started_at": shell.started_at,
                    "description": description,
                }
            )
        try:
            result = await shell.run_with_timeout(
                timeout_seconds=timeout_ms / 1000.0,
                stdin_data=stdin_text,
            )
        except (OSError, ValueError) as exc:
            raise HostError("shell_error", f"cannot start command: {exc}") from exc
        return json_result(
            {
                "exit_code": result.exit_code,
                "signal": result.signal,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "duration_ms": result.duration_ms,
                "timed_out": result.timed_out,
                "truncated_stdout": result.stdout_dropped > 0,
                "truncated_stderr": result.stderr_dropped > 0,
            }
        )

    async def _tool_shell_output(self, args: dict[str, Any]) -> ToolResult:
        job = await self._job(require_string(args, "shell_id"))
        since_last = bool(args.get("since_last", True))
        stdout_delta, stderr_delta = job.shell.read(
            since_stdout=job.stdout_cursor if since_last else 0,
            since_stderr=job.stderr_cursor if since_last else 0,
        )
        if since_last:
            job.stdout_cursor = job.shell.stdout_bytes_total
            job.stderr_cursor = job.shell.stderr_bytes_total
        filter_pattern = args.get("filter")
        if filter_pattern is not None:
            if not isinstance(filter_pattern, str):
                raise HostError("invalid_arguments", "filter must be a string")
            try:
                regex = re.compile(filter_pattern)
            except re.error as exc:
                raise HostError("invalid_arguments", f"invalid filter regex: {exc}") from exc
            stdout_delta = "\n".join(
                line for line in stdout_delta.splitlines() if regex.search(line)
            )
            stderr_delta = "\n".join(
                line for line in stderr_delta.splitlines() if regex.search(line)
            )
        return json_result(
            {
                "stdout_delta": stdout_delta,
                "stderr_delta": stderr_delta,
                "still_running": job.shell.is_running,
                "exit_code": job.shell.exit_code,
                "signal": job.shell.signal,
                "stdout_bytes_total": job.shell.stdout_bytes_total,
                "stderr_bytes_total": job.shell.stderr_bytes_total,
                "truncated_stdout": job.shell.stdout_dropped > 0,
                "truncated_stderr": job.shell.stderr_dropped > 0,
            }
        )

    async def _tool_shell_input(self, args: dict[str, Any]) -> ToolResult:
        job = await self._job(require_string(args, "shell_id"))
        text = require_string(args, "text", allow_empty=True)
        if not job.shell.is_running:
            raise HostError("shell_not_running", f"shell {job.shell_id} is not running")
        written = await job.shell.write_stdin(
            text, press_enter=bool(args.get("press_enter", True))
        )
        return json_result({"bytes_written": written})

    async def _tool_shell_kill(self, args: dict[str, Any]) -> ToolResult:
        job = await self._job(require_string(args, "shell_id"))
        signal_name = str(args.get("signal", "TERM")).upper()
        if signal_name not in {"TERM", "INT", "KILL"}:
            raise HostError("invalid_arguments", f"unsupported signal: {signal_name}")
        if job.shell.is_running:
            await job.shell.kill(signal_name=signal_name)  # type: ignore[arg-type]
        if job.reader is not None:
            await asyncio.gather(job.reader, return_exceptions=True)
        return json_result(
            {
                "killed": True,
                "exit_code": job.shell.exit_code,
                "signal": job.shell.signal or signal_name,
            }
        )

    async def _tool_shell_list(self, args: dict[str, Any]) -> ToolResult:
        if args.get("session_id") is not None and not isinstance(args["session_id"], str):
            raise HostError("invalid_arguments", "session_id must be a string")
        principal = current_principal.get()
        if principal is None:
            raise HostError(
                "account_context_required",
                "background shell listing requires a local account principal",
            )
        async with self._guard:
            jobs = [job for job in self._background.values() if job.principal == principal]
        now = time.time()
        return json_result(
            {"shells": [
                {
                    "shell_id": job.shell_id,
                    "command": job.command,
                    "state": (
                        "running"
                        if job.shell.is_running
                        else "killed"
                        if job.shell.signal is not None
                        else "completed"
                    ),
                    "started_at": job.shell.started_at,
                    "runtime_ms": int(
                        ((job.shell.completed_at or now) - (job.shell.started_at or now))
                        * 1000
                    ),
                    "stdout_bytes": job.shell.stdout_bytes_total,
                    "stderr_bytes": job.shell.stderr_bytes_total,
                    "exit_code": job.shell.exit_code,
                    "session_id": None,
                }
                for job in jobs
            ]}
        )

    async def _tool_shell_which(self, args: dict[str, Any]) -> ToolResult:
        command = require_string(args, "command")
        if "/" in command or "\\" in command:
            raise HostError(
                "invalid_arguments", "command must be a bare program name (no path separator)"
            )
        path = shutil.which(command)
        return json_result({"available": path is not None, **({"path": path} if path else {})})

    async def _pump(self, job: _Background) -> None:
        process = job.shell.process
        assert process is not None
        await process.wait()
        await job.shell.finalise()
        completed_at = job.shell.completed_at or time.time()
        await self._emit(
            {
                "type": "shell_completed",
                "server": "shell",
                "shell_id": job.shell_id,
                "status": "killed" if job.shell.signal else "completed",
                "exit_code": job.shell.exit_code,
                "stdout_bytes": job.shell.stdout_bytes_total,
                "stderr_bytes": job.shell.stderr_bytes_total,
                "output_bytes": (
                    job.shell.stdout_bytes_total + job.shell.stderr_bytes_total
                ),
                "signal": job.shell.signal,
                "at": completed_at,
                "principal": job.principal,
            }
        )

    async def _emit(self, event: dict[str, Any]) -> None:
        if self._event_sink is None:
            return
        result = self._event_sink(event)
        if inspect.isawaitable(result):
            await result

    async def _job(self, shell_id: str) -> _Background:
        async with self._guard:
            job = self._background.get(shell_id)
        if job is None:
            raise HostError("shell_not_found", f"unknown shell id {shell_id!r}")
        principal = current_principal.get()
        if principal is None:
            raise HostError(
                "account_context_required",
                "background shell access requires a local account principal",
            )
        if job.principal is not None and job.principal != principal:
            raise HostError(
                "resource_owner_mismatch",
                "background shell belongs to a different local client account",
            )
        return job

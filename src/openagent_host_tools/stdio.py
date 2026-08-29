"""NDJSON stdio shim backed by the single-instance local capability broker."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import threading
from typing import Any

from .control import dispatch_control
from .host import CapabilityHost
from .local_broker import BrokerAlreadyRunning, LocalBrokerClient, LocalBrokerServer
from .paths import HostPaths


class StdioBroker:
    """Direct in-process broker retained for tests and constrained embedders."""

    def __init__(self, host: CapabilityHost | None = None):
        self.host = host or CapabilityHost()
        self._write_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[None]] = set()

    async def run(self) -> None:
        await self.host.start()

        async def process(request: dict[str, Any]) -> None:
            reply = await dispatch_control(self.host, request)
            await self._write(reply.frame)

        try:
            while True:
                raw = await asyncio.to_thread(sys.stdin.buffer.readline)
                if not raw:
                    break
                try:
                    request = json.loads(raw)
                    if not isinstance(request, dict):
                        raise ValueError("request must be an object")
                except (ValueError, json.JSONDecodeError) as exc:
                    await self._write(
                        {
                            "id": None,
                            "type": "response",
                            "ok": False,
                            "error": {"code": "invalid_json", "message": str(exc)},
                        }
                    )
                    continue
                task = asyncio.create_task(process(request))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
                if request.get("type") == "shutdown":
                    await task
                    break
        finally:
            for task in self._tasks:
                task.cancel()
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)
            await self.host.close()

    async def _write(self, value: dict[str, Any]) -> None:
        data = json.dumps(value, separators=(",", ":"), ensure_ascii=False) + "\n"
        async with self._write_lock:
            await asyncio.to_thread(_write_stdout, data)


async def run_stdio_shim(paths: HostPaths | None = None) -> None:
    """Forward stdio NDJSON to the per-user broker, starting it if necessary."""
    paths = paths or HostPaths.discover()
    client = await _connect_or_start(paths)
    shutdown_sent = False
    stdin_eof = False
    forwarded_ids: set[str] = set()
    stdin_reader = _DaemonStdinReader()

    async def stdin_to_broker() -> None:
        nonlocal shutdown_sent, stdin_eof
        while True:
            raw = await stdin_reader.readline()
            if not raw:
                stdin_eof = True
                return
            try:
                value = json.loads(raw)
                if not isinstance(value, dict):
                    raise ValueError("request must be an object")
            except (ValueError, json.JSONDecodeError) as exc:
                await asyncio.to_thread(
                    _write_stdout,
                    json.dumps(
                        {
                            "id": None,
                            "type": "response",
                            "ok": False,
                            "error": {"code": "invalid_json", "message": str(exc)},
                        },
                        separators=(",", ":"),
                    )
                    + "\n",
                )
                continue
            request_id = value.get("id")
            if request_id is not None:
                forwarded_ids.add(str(request_id))
            await client.send(value)
            if value.get("type") == "shutdown":
                shutdown_sent = True
                return

    async def broker_to_stdout() -> None:
        while True:
            value = await client.receive()
            if value is None:
                return
            await asyncio.to_thread(
                _write_stdout,
                json.dumps(value, separators=(",", ":"), ensure_ascii=False) + "\n",
            )
            response_id = value.get("id")
            if response_id is not None:
                forwarded_ids.discard(str(response_id))
            if shutdown_sent and value.get("result", {}).get("shutting_down"):
                return
            if stdin_eof and not forwarded_ids:
                return

    input_task = asyncio.create_task(stdin_to_broker(), name="host-tools-stdin")
    output_task = asyncio.create_task(broker_to_stdout(), name="host-tools-stdout")
    try:
        done, _pending = await asyncio.wait(
            {input_task, output_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if input_task in done and shutdown_sent:
            try:
                await asyncio.wait_for(output_task, timeout=5)
            except TimeoutError:
                output_task.cancel()
        elif input_task in done:
            # Plain stdin EOF is a graceful half-close. Drain every response
            # for a request already forwarded before closing stdout.
            if forwarded_ids:
                await output_task
            else:
                output_task.cancel()
        elif output_task in done:
            # Broker EOF is a transport failure. Exit immediately so Desktop
            # drops /ws/capabilities; do not leave stdin blocking until an
            # arbitrary host timeout that could turn an uncertain mutation
            # into an explicit determinate error.
            input_task.cancel()
        else:
            output_task.cancel()
    finally:
        for task in (input_task, output_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(input_task, output_task, return_exceptions=True)
        await client.close()


class _DaemonStdinReader:
    """Cancellation-safe stdin reader for a short-lived stdio shim.

    ``asyncio.to_thread(stdin.readline)`` registers work in asyncio's default
    executor, whose shutdown waits forever while Desktop keeps the pipe open.
    A daemon reader can remain blocked without preventing process exit after a
    broker transport loss.
    """

    def __init__(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._thread = threading.Thread(
            target=self._read,
            name="openagent-host-tools-stdin",
            daemon=True,
        )
        self._thread.start()

    def _read(self) -> None:
        pending = b""
        while True:
            try:
                chunk = os.read(sys.stdin.fileno(), 64 * 1024)
            except OSError:
                chunk = b""
            if chunk:
                pending += chunk
                while b"\n" in pending:
                    raw, pending = pending.split(b"\n", 1)
                    raw += b"\n"
                    try:
                        self._loop.call_soon_threadsafe(self._queue.put_nowait, raw)
                    except RuntimeError:
                        return
                continue
            raw = pending
            try:
                self._loop.call_soon_threadsafe(self._queue.put_nowait, raw)
            except RuntimeError:
                return
            return

    async def readline(self) -> bytes:
        return await self._queue.get()


async def _connect_or_start(paths: HostPaths) -> LocalBrokerClient:
    client = LocalBrokerClient(paths)
    try:
        await client.connect()
        return client
    except (OSError, EOFError, ConnectionRefusedError, FileNotFoundError):
        await client.close()

    _spawn_broker(paths)
    deadline = asyncio.get_running_loop().time() + 8.0
    last_error: Exception | None = None
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.05)
        candidate = LocalBrokerClient(paths)
        try:
            await candidate.connect()
            return candidate
        except (OSError, EOFError, ConnectionRefusedError, FileNotFoundError) as exc:
            last_error = exc
            await candidate.close()
    raise RuntimeError(f"local capability broker did not start: {last_error}")


def _spawn_broker(paths: HostPaths) -> None:
    env = os.environ.copy()
    env["OPENAGENT_HOST_TOOLS_HOME"] = str(paths.home)
    if getattr(sys, "frozen", False):
        command = [sys.executable, "--broker"]
    else:
        command = [sys.executable, "-m", "openagent_host_tools", "--broker"]
    kwargs: dict[str, Any] = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "env": env,
        "close_fds": True,
    }
    if os.name == "nt":  # pragma: no cover - exercised on Windows CI
        kwargs["creationflags"] = (
            subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
        )
    else:
        kwargs["start_new_session"] = True
    subprocess.Popen(command, **kwargs)


def _write_stdout(data: str) -> None:
    sys.stdout.write(data)
    sys.stdout.flush()


async def _run_broker_until_signal() -> None:
    """Turn Unix termination into cancellation so endpoint cleanup can run."""

    server_task = asyncio.create_task(
        LocalBrokerServer().run(), name="openagent-host-tools-broker"
    )
    if os.name == "nt":  # named pipes and locks are reclaimed by the kernel
        await server_task
        return

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    installed: list[signal.Signals] = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
        except (NotImplementedError, RuntimeError, ValueError):
            continue
        installed.append(signum)
    if not installed:
        await server_task
        return

    stop_task = asyncio.create_task(stop.wait(), name="host-tools-signal-wait")
    try:
        done, _pending = await asyncio.wait(
            {server_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if server_task in done:
            await server_task
        else:
            server_task.cancel()
            await asyncio.gather(server_task, return_exceptions=True)
    finally:
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        for signum in installed:
            loop.remove_signal_handler(signum)


async def _async_main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="openagent-host-tools")
    parser.add_argument("--broker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--direct", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--mcp", choices=("filesystem", "editor", "shell"), help=argparse.SUPPRESS
    )
    args = parser.parse_args(argv)
    if args.mcp:
        from .mcp_server import serve

        await serve(args.mcp)
    elif args.broker:
        try:
            await _run_broker_until_signal()
        except BrokerAlreadyRunning:
            return
    elif args.direct:
        await StdioBroker().run()
    else:
        await run_stdio_shim()


def main() -> None:
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

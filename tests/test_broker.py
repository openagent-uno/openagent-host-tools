from __future__ import annotations

import asyncio
import json
import os
import shlex
import socket
import sys
import time
from pathlib import Path

import pytest

from openagent_host_tools import HostError, HostPaths
from openagent_host_tools.local_broker import (
    BrokerAlreadyRunning,
    LocalBrokerClient,
    LocalBrokerServer,
    LocalCapabilityClient,
    _unix_socket_path,
)


async def _broker_response(client: LocalBrokerClient, request_id: str) -> dict:
    while True:
        frame = await asyncio.wait_for(client.receive(), timeout=3)
        assert frame is not None
        if frame.get("id") == request_id:
            return frame


@pytest.mark.skipif(os.name == "nt", reason="Unix socket variant")
@pytest.mark.asyncio
async def test_single_instance_broker_vertical_slice(tmp_path: Path):
    paths = HostPaths.discover(tmp_path / "user")
    server = LocalBrokerServer(paths)
    task = asyncio.create_task(server.run())
    for _ in range(100):
        if server.unix_socket_path.exists():
            break
        await asyncio.sleep(0.01)
    assert server.unix_socket_path.exists()

    client = LocalCapabilityClient(paths)
    events: list[dict] = []

    async def on_event(event: dict) -> None:
        events.append(event)

    try:
        await client.start()
        client.subscribe_events(on_event)
        await client.set_consent(True)
        catalog = await client.catalog()
        assert {item["name"] for item in catalog} >= {"filesystem", "editor", "shell"}
        target = tmp_path / "broker.txt"
        await client.call(
            "filesystem",
            "write_file",
            {"path": str(target), "content": "through-broker"},
            principal="desktop-instance",
            call_id="broker-call",
            idempotency_key="broker-write",
        )
        assert target.read_text() == "through-broker"
        shell_principal = {
            "kind": "cli",
            "client_instance_id": "desktop-instance",
            "device_label": "test",
            "account_id": "account-1",
        }
        shell_result = await client.call(
            "shell",
            "shell_exec",
            {"command": "printf broker-event", "run_in_background": True},
            principal=shell_principal,
            call_id="broker-shell",
        )
        assert shell_result.structured_content["shell_id"].startswith("sh_")
        assert shell_result.structured_content["started_at"] > 0
        for _ in range(200):
            if events:
                break
            await asyncio.sleep(0.01)
        assert events[0]["type"] == "shell_completed"
        assert events[0]["stdout_bytes"] == len(b"broker-event")
        assert events[0]["stderr_bytes"] == 0
        assert events[0]["at"] > 0
        client.unsubscribe_events(on_event)
        replayed: list[dict] = []
        client.subscribe_events(replayed.append)
        await asyncio.sleep(0)
        assert replayed and replayed[0]["shell_id"] == events[0]["shell_id"]
        client.unsubscribe_events(replayed.append)
        await client.release_principal(shell_principal)
        after_release: list[dict] = []
        client.subscribe_events(after_release.append)
        await asyncio.sleep(0)
        assert after_release == []
    finally:
        await client.close()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert not server.unix_socket_path.exists()


@pytest.mark.skipif(os.name == "nt", reason="Unix socket variant")
@pytest.mark.asyncio
async def test_sigterm_gracefully_removes_broker_socket(tmp_path: Path):
    paths = HostPaths.discover(tmp_path / "user")
    socket_path = _unix_socket_path(paths)
    env = os.environ.copy()
    env["OPENAGENT_HOST_TOOLS_HOME"] = str(paths.home)
    broker = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "openagent_host_tools",
        "--broker",
        env=env,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        for _ in range(200):
            if socket_path.exists():
                break
            if broker.returncode is not None:
                break
            await asyncio.sleep(0.01)
        assert socket_path.exists()

        broker.terminate()
        await asyncio.wait_for(broker.wait(), timeout=3)

        assert broker.returncode == 0
        assert not socket_path.exists()
    finally:
        if broker.returncode is None:
            broker.kill()
            await broker.wait()
        socket_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_failed_host_start_releases_singleton_and_endpoint(tmp_path: Path):
    class FailingHost:
        def __init__(self) -> None:
            self.close_count = 0

        async def start(self) -> None:
            raise RuntimeError("host startup failed")

        async def close(self) -> None:
            self.close_count += 1

    paths = HostPaths.discover(tmp_path / "user")
    server = LocalBrokerServer(paths)
    failing_host = FailingHost()
    server.host = failing_host  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="host startup failed"):
        await server.run()

    assert failing_host.close_count == 1
    if os.name != "nt":
        assert not server.unix_socket_path.exists()

    # A failed startup must not poison this user's singleton lock.
    replacement = LocalBrokerServer(paths)
    replacement._acquire_singleton()
    replacement._release_singleton()


@pytest.mark.skipif(os.name == "nt", reason="Unix socket variant")
@pytest.mark.asyncio
async def test_stale_socket_is_replaced_and_removed_on_close(tmp_path: Path):
    paths = HostPaths.discover(tmp_path / "user")
    socket_path = _unix_socket_path(paths)
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(str(socket_path))
    stale.close()
    assert socket_path.exists()

    server = LocalBrokerServer(paths)
    task = asyncio.create_task(server.run())
    client = LocalBrokerClient(paths)
    try:
        for _ in range(200):
            try:
                await client.connect()
                break
            except (OSError, EOFError, ConnectionRefusedError, FileNotFoundError):
                await client.close()
                await asyncio.sleep(0.01)
        assert client._writer is not None
    finally:
        await client.close()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert not socket_path.exists()


@pytest.mark.skipif(os.name == "nt", reason="Unix socket variant")
@pytest.mark.asyncio
async def test_active_unrelated_socket_is_never_unlinked(tmp_path: Path):
    paths = HostPaths.discover(tmp_path / "user")
    socket_path = _unix_socket_path(paths)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(socket_path))
    listener.listen(8)
    original = socket_path.stat()
    server = LocalBrokerServer(paths)
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        with pytest.raises(BrokerAlreadyRunning, match="socket"):
            await server.run()

        current = socket_path.stat()
        assert (current.st_dev, current.st_ino) == (original.st_dev, original.st_ino)
        probe.connect(str(socket_path))
    finally:
        probe.close()
        listener.close()
        socket_path.unlink(missing_ok=True)


@pytest.mark.skipif(os.name == "nt", reason="Unix socket variant")
@pytest.mark.asyncio
async def test_shutdown_preserves_replacement_socket(tmp_path: Path):
    paths = HostPaths.discover(tmp_path / "user")
    socket_path = _unix_socket_path(paths)
    server = LocalBrokerServer(paths)
    task = asyncio.create_task(server.run())
    client = LocalBrokerClient(paths)
    replacement = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        for _ in range(200):
            try:
                await client.connect()
                break
            except (OSError, EOFError, ConnectionRefusedError, FileNotFoundError):
                await client.close()
                await asyncio.sleep(0.01)
        assert client._writer is not None
        await client.close()

        # Simulate another owner replacing the pathname while this broker's
        # already-open server socket is winding down. Cleanup may only unlink
        # the inode originally bound by this LocalBrokerServer instance.
        socket_path.unlink()
        replacement.bind(str(socket_path))
        replacement.listen(8)
        original = socket_path.stat()

        task.cancel()
        results = await asyncio.gather(task, return_exceptions=True)
        assert isinstance(results[0], asyncio.CancelledError)
        current = socket_path.stat()
        assert (current.st_dev, current.st_ino) == (original.st_dev, original.st_ino)
        probe.connect(str(socket_path))
    finally:
        await client.close()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        probe.close()
        replacement.close()
        socket_path.unlink(missing_ok=True)


@pytest.mark.skipif(os.name == "nt", reason="Unix socket variant")
@pytest.mark.asyncio
async def test_control_socket_eof_releases_its_background_principals(tmp_path: Path):
    paths = HostPaths.discover(tmp_path / "user")
    server = LocalBrokerServer(paths)
    server_task = asyncio.create_task(server.run())
    for _ in range(100):
        if server.unix_socket_path.exists():
            break
        await asyncio.sleep(0.01)

    principal = {
        "kind": "desktop",
        "client_instance_id": "crashing-instance",
        "account_id": "account-1",
        "generation": 1,
    }
    raw = LocalBrokerClient(paths)
    observer = LocalCapabilityClient(paths)
    try:
        await raw.connect()
        await raw.send({"id": "consent", "type": "set_consent", "enabled": True})
        assert (await _broker_response(raw, "consent"))["ok"] is True
        await raw.send(
            {
                "id": "start",
                "type": "call",
                "call_id": "crash-shell-start",
                "server": "shell",
                "tool": "shell_exec",
                "args": {"command": "sleep 30", "run_in_background": True},
                "principal": principal,
            }
        )
        started = await _broker_response(raw, "start")
        assert started["result"]["structuredContent"]["shell_id"].startswith("sh_")

        # Capability-WebSocket reconnects happen above this still-open control
        # socket and therefore must not reclaim the principal.
        await raw.send(
            {
                "id": "before-eof",
                "type": "call",
                "call_id": "before-eof",
                "server": "shell",
                "tool": "shell_list",
                "args": {},
                "principal": principal,
            }
        )
        before = await _broker_response(raw, "before-eof")
        assert len(before["result"]["structuredContent"]["shells"]) == 1

        # Simulate a killed Desktop/CLI process: no shutdown and no explicit
        # release_principal frame, only control-socket EOF.
        await raw.close()
        await asyncio.sleep(0.1)

        await observer.start()
        await observer.catalog()
        after = await observer.call(
            "shell",
            "shell_list",
            {},
            principal=principal,
            call_id="after-control-eof",
        )
        assert after.structured_content == {"shells": []}
    finally:
        await raw.close()
        await observer.close()
        server_task.cancel()
        await asyncio.gather(server_task, return_exceptions=True)


@pytest.mark.skipif(os.name == "nt", reason="Unix SIGKILL/socket regression")
@pytest.mark.asyncio
async def test_stdio_shim_exits_immediately_when_broker_dies_with_stdin_open(
    tmp_path: Path,
):
    paths = HostPaths.discover(tmp_path / "user")
    env = os.environ.copy()
    env["OPENAGENT_HOST_TOOLS_HOME"] = str(paths.home)
    broker = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "openagent_host_tools",
        "--broker",
        env=env,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    shim = None
    try:
        socket_path = _unix_socket_path(paths)
        for _ in range(200):
            if socket_path.exists():
                break
            await asyncio.sleep(0.01)
        assert socket_path.exists()
        shim = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "openagent_host_tools",
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert shim.stdin is not None and shim.stdout is not None
        shim.stdin.write(
            (json.dumps({"id": "init", "type": "initialize"}) + "\n").encode()
        )
        await shim.stdin.drain()
        response = json.loads(await asyncio.wait_for(shim.stdout.readline(), timeout=2))
        assert response["ok"] is True

        shim.stdin.write(
            (json.dumps({"id": "consent", "type": "set_consent", "enabled": True}) + "\n").encode()
        )
        await shim.stdin.drain()
        consent = json.loads(await asyncio.wait_for(shim.stdout.readline(), timeout=2))
        assert consent["ok"] is True
        marker = tmp_path / "effect-happened"
        command = shlex.join(
            [
                sys.executable,
                "-c",
                f"from pathlib import Path; import sys; Path({str(marker)!r}).write_text('yes'); sys.stdin.read()",
            ]
        )
        shim.stdin.write(
            (
                json.dumps(
                    {
                        "id": "effect-call",
                        "type": "call",
                        "call_id": "effect-call",
                        "idempotency_key": "effect-call",
                        "server": "shell",
                        "tool": "shell_exec",
                        "args": {"command": command},
                        "principal": "account-a",
                    }
                )
                + "\n"
            ).encode()
        )
        await shim.stdin.drain()
        for _ in range(200):
            if marker.exists():
                break
            await asyncio.sleep(0.01)
        assert marker.read_text() == "yes"

        started = time.monotonic()
        broker.kill()
        await broker.wait()
        await asyncio.wait_for(shim.wait(), timeout=1)
        assert time.monotonic() - started < 1
        assert shim.stderr is not None
        stderr = await shim.stderr.read()
        assert b"_enter_buffered_busy" not in stderr
        # Deliberately never close shim.stdin before wait(): this is the
        # regression for the old default-executor readline deadlock.
    finally:
        if shim is not None and shim.returncode is None:
            shim.kill()
            await shim.wait()
        if broker.returncode is None:
            broker.kill()
            await broker.wait()


@pytest.mark.skipif(os.name == "nt", reason="Unix socket subprocess regression")
@pytest.mark.asyncio
async def test_stdio_shim_drains_forwarded_responses_after_plain_stdin_eof(
    tmp_path: Path,
):
    paths = HostPaths.discover(tmp_path / "user")
    env = os.environ.copy()
    env["OPENAGENT_HOST_TOOLS_HOME"] = str(paths.home)
    broker = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "openagent_host_tools",
        "--broker",
        env=env,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    shim = None
    try:
        for _ in range(200):
            if _unix_socket_path(paths).exists():
                break
            await asyncio.sleep(0.01)
        shim = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "openagent_host_tools",
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        payload = b"".join(
            (json.dumps(frame) + "\n").encode()
            for frame in (
                {"id": "init", "type": "initialize"},
                {"id": "status", "type": "status"},
            )
        )
        stdout, stderr = await asyncio.wait_for(shim.communicate(payload), timeout=3)
        assert shim.returncode == 0
        assert stderr == b""
        frames = [json.loads(line) for line in stdout.splitlines()]
        assert {frame["id"] for frame in frames} == {"init", "status"}
        assert all(frame["ok"] is True for frame in frames)
    finally:
        if shim is not None and shim.returncode is None:
            shim.kill()
            await shim.wait()
        if broker.returncode is None:
            broker.kill()
            await broker.wait()


@pytest.mark.asyncio
async def test_idempotent_plugin_broker_disconnect_is_retryable_not_indeterminate(
    tmp_path: Path,
):
    class EofTransport:
        def __init__(self):
            self.sent = asyncio.Event()

        async def send(self, value):
            self.sent.set()

        async def receive(self):
            await self.sent.wait()
            return None

        async def close(self):
            return None

    class ResponseTransport:
        def __init__(self):
            self.frames = asyncio.Queue()

        async def send(self, value):
            await self.frames.put(
                {
                    "id": value["id"],
                    "type": "response",
                    "ok": True,
                    "result": {
                        "content": [{"type": "text", "text": "retried"}],
                        "structuredContent": {"ok": True},
                        "isError": False,
                    },
                }
            )

        async def receive(self):
            return await self.frames.get()

        async def close(self):
            return None

    paths = HostPaths.discover(tmp_path / "user")
    client = LocalCapabilityClient(paths)
    first = EofTransport()
    client._transport = first
    client._tool_classifications[("plugin", "ensure_state")] = "idempotent"
    client._listener = asyncio.create_task(client._listen(first))
    with pytest.raises(HostError) as disconnected:
        await client.call(
            "plugin",
            "ensure_state",
            {"enabled": True},
            principal="account-a",
            call_id="idem-retry",
            idempotency_key="idem-retry",
        )
    assert disconnected.value.code == "broker_disconnected"

    second = ResponseTransport()
    client._transport = second
    client._listener = asyncio.create_task(client._listen(second))
    retried = await client.call(
        "plugin",
        "ensure_state",
        {"enabled": True},
        principal="account-a",
        call_id="idem-retry",
        idempotency_key="idem-retry",
    )
    assert retried.structured_content == {"ok": True}
    listener = client._listener
    assert listener is not None
    listener.cancel()
    await asyncio.gather(listener, return_exceptions=True)
    client._transport = None


@pytest.mark.skipif(os.name == "nt", reason="Unix SIGKILL/socket restart regression")
@pytest.mark.asyncio
async def test_capability_client_restarts_after_real_broker_sigkill(tmp_path: Path):
    paths = HostPaths.discover(tmp_path / "user")
    env = os.environ.copy()
    env["OPENAGENT_HOST_TOOLS_HOME"] = str(paths.home)

    async def spawn_broker():
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "openagent_host_tools",
            "--broker",
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        for _ in range(200):
            probe = LocalBrokerClient(paths)
            try:
                await probe.connect()
            except (OSError, EOFError, ConnectionRefusedError, FileNotFoundError):
                await probe.close()
                await asyncio.sleep(0.01)
                continue
            await probe.close()
            return process
        process.kill()
        await process.wait()
        raise AssertionError("broker did not accept connections")

    first = await spawn_broker()
    second = None
    client = LocalCapabilityClient(paths)
    target = tmp_path / "restart.txt"
    target.write_text("survived", encoding="utf-8")
    try:
        await client.start()
        await client.set_consent(True)
        await client.catalog()
        first.kill()
        await first.wait()
        for _ in range(200):
            if client._transport is None:
                break
            await asyncio.sleep(0.01)
        assert client._transport is None

        second = await spawn_broker()
        await asyncio.wait_for(client.start(), timeout=3)
        await client.catalog()
        result = await asyncio.wait_for(
            client.call(
                "filesystem",
                "read_text_file",
                {"path": str(target)},
                principal="account-a",
                call_id="read-after-broker-restart",
            ),
            timeout=3,
        )
        assert result.content[0]["text"] == "survived"
    finally:
        await client.close()
        if first.returncode is None:
            first.kill()
            await first.wait()
        if second is not None and second.returncode is None:
            second.kill()
            await second.wait()

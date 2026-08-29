from __future__ import annotations

import asyncio
import json
import os
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from openagent_host_tools import CapabilityHost, HostError, HostPaths
from openagent_host_tools.config import PluginConfigStore, PluginSpec
from openagent_host_tools.lease import MutatingLease
from openagent_host_tools.idempotency import IdempotencyLedger
from openagent_host_tools.builtins import FilesystemServer
from openagent_host_tools.types import (
    ServerManifest,
    ToolClassification,
    ToolManifest,
    ToolResult,
)


@pytest.fixture
def paths(tmp_path: Path) -> HostPaths:
    return HostPaths.discover(tmp_path / "user")


def _shell_command(argv: list[str]) -> str:
    """Quote argv for the platform shell used by ``BackgroundShell``."""

    return subprocess.list2cmdline(argv) if os.name == "nt" else shlex.join(argv)


@pytest.mark.asyncio
async def test_fail_closed_inventory_and_manifest_metadata(paths: HostPaths, tmp_path: Path):
    host = CapabilityHost(paths=paths, cwd=tmp_path)
    await host.start()
    try:
        assert await host.catalog() == []
        status = await host.status()
        assert status["config_path"] == str(tmp_path / "user" / "client-mcps.toml")
        servers = {server["name"]: server for server in status["servers"]}
        assert set(servers) >= {
            "filesystem",
            "editor",
            "shell",
            "computer-control",
            "agent-in-chrome",
        }
        for name in ("filesystem", "editor", "shell", "computer-control", "agent-in-chrome"):
            assert servers[name]["platforms"]
            assert servers[name]["os_requirements"]
            assert servers[name]["data_directory"]
        assert servers["computer-control"]["available"] is False
        assert servers["agent-in-chrome"]["available"] is False

        await host.set_consent(True)
        assert {server["name"] for server in await host.catalog()} >= {
            "filesystem",
            "editor",
            "shell",
        }
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_filesystem_editor_shell_and_durable_idempotency(paths: HostPaths, tmp_path: Path):
    host = CapabilityHost(paths=paths, cwd=tmp_path)
    await host.start()
    await host.set_consent(True)
    target = tmp_path / "note.txt"
    try:
        first = await host.call(
            "filesystem",
            "write_file",
            {"path": str(target), "content": "alpha beta"},
            principal="test-client",
            idempotency_key="write-1",
        )
        replay = await host.call(
            "filesystem",
            "write_file",
            {"path": str(target), "content": "alpha beta"},
            principal="test-client",
            idempotency_key="write-1",
        )
        assert first.meta["openagent/replayed"] is False
        assert first.meta["openagent/location"] == "client"
        assert first.meta["openagent/pathSemantics"] == "client-local"
        assert replay.meta["openagent/replayed"] is True
        assert replay.meta["openagent/location"] == "client"
        with pytest.raises(HostError, match="different arguments"):
            await host.call(
                "filesystem",
                "write_file",
                {"path": str(target), "content": "different"},
                principal="test-client",
                idempotency_key="write-1",
            )
        call_id_target = tmp_path / "call-id.txt"
        await host.call(
            "filesystem",
            "write_file",
            {"path": str(call_id_target), "content": "first"},
            principal="test-client",
            call_id="same-call-id",
        )
        with pytest.raises(HostError) as conflict:
            await host.call(
                "filesystem",
                "write_file",
                {"path": str(call_id_target), "content": "second"},
                principal="test-client",
                call_id="same-call-id",
            )
        assert conflict.value.code == "idempotency_conflict"

        await host.call(
            "editor",
            "edit",
            {
                "file_path": str(target),
                "old_string": "beta",
                "new_string": "gamma",
            },
            principal="test-client",
            idempotency_key="edit-1",
        )
        read = await host.call(
            "filesystem",
            "read_text_file",
            {"path": str(target)},
            principal="test-client",
            idempotency_key="read-1",
        )
        read_replay = await host.call(
            "filesystem",
            "read_text_file",
            {"path": str(target)},
            principal="test-client",
            idempotency_key="read-1",
        )
        assert read.content[0]["text"] == "alpha gamma"
        assert read.meta["openagent/location"] == "client"
        assert read_replay.meta["openagent/replayed"] is True
        with pytest.raises(HostError) as read_conflict:
            await host.call(
                "filesystem",
                "read_text_file",
                {"path": str(call_id_target)},
                principal="test-client",
                idempotency_key="read-1",
            )
        assert read_conflict.value.code == "idempotency_conflict"

        command = _shell_command([sys.executable, "-c", "print('shell-ok')"])
        shell = await host.call(
            "shell",
            "shell_exec",
            {"command": command},
            principal="test-client",
            idempotency_key="shell-1",
        )
        assert shell.structured_content["exit_code"] == 0
        assert "shell-ok" in shell.structured_content["stdout"]
        assert shell.structured_content["stderr"] == ""
        assert shell.structured_content["timed_out"] is False
        recent = await host.audit.recent(20)
        assert recent
        assert all(row["target"] == "client" for row in recent)
        assert all(row["arguments_sha256"] for row in recent)
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_revocation_rejects_calls_and_is_shared(paths: HostPaths, tmp_path: Path):
    one = CapabilityHost(paths=paths, cwd=tmp_path)
    two = CapabilityHost(paths=paths, cwd=tmp_path)
    await one.start()
    await two.start()
    try:
        await one.set_consent(True)
        assert two.consent().enabled is True
        await two.set_consent(False)
        with pytest.raises(HostError) as exc:
            await one.call(
                "filesystem", "list_directory", {"path": str(tmp_path)}, principal="one"
            )
        assert exc.value.code == "consent_required"
    finally:
        await one.close()
        await two.close()


@pytest.mark.asyncio
async def test_disable_is_a_barrier_for_calls_waiting_before_dispatch(
    paths: HostPaths, tmp_path: Path
):
    host = CapabilityHost(paths=paths, cwd=tmp_path)
    await host.start()
    await host.set_consent(True)
    target = tmp_path / "must-not-exist-after-disable.txt"
    entered_claim = asyncio.Event()
    release_claim = asyncio.Event()
    original_claim = host.idempotency.claim

    async def blocked_claim(*args, **kwargs):
        entered_claim.set()
        await release_claim.wait()
        return await original_claim(*args, **kwargs)

    host.idempotency.claim = blocked_claim
    call = asyncio.create_task(
        host.call(
            "filesystem",
            "write_file",
            {"path": str(target), "content": "forbidden"},
            principal="test",
            call_id="disable-admission-race",
        )
    )
    try:
        await asyncio.wait_for(entered_claim.wait(), timeout=1)
        disabling = asyncio.create_task(host.set_consent(False))

        # set_consent has already persisted the revoke, but it must not return
        # until the hidden pre-dispatch call crosses the admission barrier.
        await asyncio.sleep(0.02)
        assert host.consent().enabled is False
        assert disabling.done() is False
        assert target.exists() is False

        release_claim.set()
        await asyncio.wait_for(disabling, timeout=2)
        with pytest.raises(HostError) as rejected:
            await call
        assert rejected.value.code == "consent_required"
        assert target.exists() is False
        assert (await host.lease.status())["state"] == "free"
    finally:
        release_claim.set()
        await host.close()


@pytest.mark.asyncio
async def test_mutating_lease_releases_immediately(paths: HostPaths):
    first = MutatingLease(paths.state_db, lease_seconds=15)
    second = MutatingLease(paths.state_db, lease_seconds=15)
    await first.enter("desktop", "a", "filesystem.write_file")
    with pytest.raises(HostError) as same_principal:
        await second.enter("desktop", "other-session", "shell.shell_exec")
    assert same_principal.value.code == "lease_held"
    with pytest.raises(HostError) as exc:
        await second.enter("cli", "b", "editor.edit")
    assert exc.value.code == "lease_held"
    await first.leave("desktop", "a")
    await second.enter("cli", "b", "editor.edit")
    await second.leave("cli", "b")
    assert (await first.status())["state"] == "free"


@pytest.mark.asyncio
async def test_background_shells_are_private_to_principal(paths: HostPaths, tmp_path: Path):
    host = CapabilityHost(paths=paths, cwd=tmp_path)
    await host.start()
    await host.set_consent(True)
    account_a = {
        "kind": "desktop",
        "client_instance_id": "a",
        "device_label": "test",
        "account_id": "network-a",
    }
    account_b = {
        "kind": "cli",
        "client_instance_id": "b",
        "device_label": "test",
        "account_id": "network-b",
    }
    command = _shell_command([sys.executable, "-c", "import time; time.sleep(10)"])
    try:
        started = await host.call(
            "shell",
            "shell_exec",
            {"command": command, "run_in_background": True},
            principal=account_a,
            call_id="shell-a",
        )
        shell_id = started.structured_content["shell_id"]
        visible_a = await host.call("shell", "shell_list", {}, principal=account_a)
        visible_b = await host.call("shell", "shell_list", {}, principal=account_b)
        assert [item["shell_id"] for item in visible_a.structured_content["shells"]] == [shell_id]
        assert visible_b.structured_content == {"shells": []}
        denied = await host.call(
            "shell", "shell_output", {"shell_id": shell_id}, principal=account_b
        )
        assert denied.is_error is True
        assert denied.meta["openagent/error"]["code"] == "resource_owner_mismatch"
    finally:
        await host.release_principal(account_a)
        await host.release_principal(account_b)
        await host.close()


def test_shared_toml_plugin_config_roundtrip(paths: HostPaths):
    store = PluginConfigStore(paths)
    specs = [
        PluginSpec(
            "demo",
            (sys.executable, "-m", "demo_mcp"),
            env={"DEMO": "yes"},
            cwd="/tmp",
        )
    ]
    store.save(specs)
    assert paths.plugins.name == "client-mcps.toml"
    assert store.load() == specs
    assert "[[mcp]]" in paths.plugins.read_text()


@pytest.mark.asyncio
async def test_idempotency_replay_preserves_extension_envelope(paths: HostPaths):
    ledger = IdempotencyLedger(paths.state_db)
    claim = await ledger.claim(
        "principal",
        "envelope",
        server="computer-control",
        tool="computer",
        args={"action": "get_screenshot"},
    )
    assert claim.state == "new"
    original = ToolResult.from_wire(
        {
            "content": [{"type": "text", "text": "done"}],
            "structuredContent": {"ok": True},
            "isError": False,
            "_meta": {"source": "sidecar"},
            "images": [{"type": "image", "data": "c21hbGw="}],
            "child_session_id": "child-1",
            "files": [{"path": "/client/report.pdf"}],
        }
    )
    await ledger.complete("principal", "envelope", original)
    replay = await ledger.claim(
        "principal",
        "envelope",
        server="computer-control",
        tool="computer",
        args={"action": "get_screenshot"},
    )
    assert replay.state == "replay"
    assert replay.result is not None
    assert replay.result.to_wire() == original.to_wire()


@pytest.mark.asyncio
async def test_safe_call_new_broker_owner_takes_over_live_lease_immediately(
    tmp_path: Path,
):
    path = tmp_path / "idempotency.sqlite3"
    old_broker = IdempotencyLedger(path, inflight_lease_seconds=30)
    new_broker = IdempotencyLedger(path, inflight_lease_seconds=30)
    args = {"path": "/client/read-only"}
    first = await old_broker.claim(
        "principal",
        "safe-call",
        server="filesystem",
        tool="read_text_file",
        args=args,
        retry_stale=True,
    )
    assert first.state == "new"

    started = time.monotonic()
    takeover = await new_broker.claim(
        "principal",
        "safe-call",
        server="filesystem",
        tool="read_text_file",
        args=args,
        retry_stale=True,
    )
    assert takeover.state == "new"
    assert time.monotonic() - started < 1

    mutation_path = tmp_path / "mutation.sqlite3"
    mutation_old = IdempotencyLedger(mutation_path, inflight_lease_seconds=30)
    mutation_new = IdempotencyLedger(mutation_path, inflight_lease_seconds=30)
    mutation_args = {"path": "/client/value", "content": "effect"}
    await mutation_old.claim(
        "principal",
        "mutation",
        server="filesystem",
        tool="write_file",
        args=mutation_args,
        retry_stale=False,
    )
    with pytest.raises(HostError) as in_flight:
        await mutation_new.claim(
            "principal",
            "mutation",
            server="filesystem",
            tool="write_file",
            args=mutation_args,
            retry_stale=False,
        )
    assert in_flight.value.code == "idempotency_in_flight"


@pytest.mark.asyncio
async def test_thread_backed_mutation_drains_before_indeterminate_timeout(
    paths: HostPaths, tmp_path: Path
):
    class SlowFilesystem(FilesystemServer):
        def _tool_write_file(self, args):
            time.sleep(0.1)
            return super()._tool_write_file(args)

    host = CapabilityHost(paths=paths, cwd=tmp_path)
    slow = SlowFilesystem(tmp_path)
    host._servers["filesystem"] = slow
    await host.start()
    await host.set_consent(True)
    target = tmp_path / "definitive.txt"
    started = time.monotonic()
    try:
        with pytest.raises(HostError) as exc:
            await host.call(
                "filesystem",
                "write_file",
                {"path": str(target), "content": "finished"},
                principal="test",
                call_id="slow-timeout",
                deadline_ms=10,
            )
        assert exc.value.code == "idempotency_indeterminate"
        assert time.monotonic() - started >= 0.09
        assert target.read_text() == "finished"
        assert (await host.lease.status())["state"] == "free"
        replay = await host.call(
            "filesystem",
            "write_file",
            {"path": str(target), "content": "finished"},
            principal="test",
            call_id="slow-timeout",
        )
        assert replay.meta["openagent/replayed"] is True
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_thread_backed_mutation_drains_before_cancel_returns(
    paths: HostPaths, tmp_path: Path
):
    started = threading.Event()
    release = threading.Event()

    class SlowFilesystem(FilesystemServer):
        def _tool_write_file(self, args):
            started.set()
            if not release.wait(timeout=5):
                raise RuntimeError("test did not release the blocked mutation")
            return super()._tool_write_file(args)

    host = CapabilityHost(paths=paths, cwd=tmp_path)
    host._servers["filesystem"] = SlowFilesystem(tmp_path)
    await host.start()
    await host.set_consent(True)
    target = tmp_path / "cancelled-but-drained.txt"
    call = asyncio.create_task(
        host.call(
            "filesystem",
            "write_file",
            {"path": str(target), "content": "finished"},
            principal="test",
            call_id="slow-cancel",
        )
    )
    try:
        assert await asyncio.to_thread(started.wait, 2)
        assert await host.cancel("slow-cancel") is True
        call.cancel()

        # Cancellation of the orchestration task must not complete while the
        # already-dispatched filesystem thread can still produce an effect.
        await asyncio.sleep(0.02)
        assert call.done() is False
        assert target.exists() is False

        release.set()
        with pytest.raises(HostError) as exc:
            await call
        assert exc.value.code == "idempotency_indeterminate"
        assert target.read_text() == "finished"
        assert (await host.lease.status())["state"] == "free"
    finally:
        release.set()
        await host.close()


@pytest.mark.asyncio
async def test_release_principal_never_unlocks_an_active_mutation(
    paths: HostPaths, tmp_path: Path
):
    started = threading.Event()
    counter_lock = threading.Lock()
    concurrent = 0
    maximum = 0

    class SlowFilesystem(FilesystemServer):
        def _tool_write_file(self, args):
            nonlocal concurrent, maximum
            with counter_lock:
                concurrent += 1
                maximum = max(maximum, concurrent)
            started.set()
            try:
                time.sleep(0.15)
                return super()._tool_write_file(args)
            finally:
                with counter_lock:
                    concurrent -= 1

    host = CapabilityHost(paths=paths, cwd=tmp_path)
    host._servers["filesystem"] = SlowFilesystem(tmp_path)
    await host.start()
    await host.set_consent(True)
    first = asyncio.create_task(
        host.call(
            "filesystem",
            "write_file",
            {"path": str(tmp_path / "first.txt"), "content": "first"},
            principal="account-a",
            call_id="release-active-a",
        )
    )
    try:
        assert await asyncio.to_thread(started.wait, 1)
        releasing = asyncio.create_task(host.release_principal("account-a"))
        await asyncio.sleep(0.02)
        with pytest.raises(HostError) as held:
            await host.call(
                "filesystem",
                "write_file",
                {"path": str(tmp_path / "second.txt"), "content": "second"},
                principal="account-b",
                call_id="release-active-b-held",
            )
        assert held.value.code == "lease_held"
        await first
        await releasing
        await host.call(
            "filesystem",
            "write_file",
            {"path": str(tmp_path / "second.txt"), "content": "second"},
            principal="account-b",
            call_id="release-active-b-after",
        )
        assert maximum == 1
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_external_mutation_disconnect_is_durable_indeterminate(
    paths: HostPaths, tmp_path: Path
):
    class Disconnecting:
        manifest = ServerManifest(
            "disconnecting",
            "1",
            "test",
            (
                ToolManifest(
                    "mutate",
                    "mutate then disconnect",
                    {"type": "object"},
                    ToolClassification.MUTATING,
                ),
            ),
        )

        async def call(self, tool, args):
            del tool, args
            raise HostError("plugin_disconnected", "transport vanished")

        async def close(self):
            return None

    host = CapabilityHost(paths=paths, cwd=tmp_path)
    host._servers["disconnecting"] = Disconnecting()
    host._inventory["disconnecting"] = Disconnecting.manifest
    host._health["disconnecting"] = {"available": True, "source": "plugin"}
    host._external_names.add("disconnecting")
    await host.start()
    await host.set_consent(True)
    try:
        with pytest.raises(HostError) as first:
            await host.call(
                "disconnecting",
                "mutate",
                {},
                principal="test",
                call_id="disconnect-call",
            )
        assert first.value.code == "idempotency_indeterminate"
        assert (await host.lease.status())["state"] == "held"
        with pytest.raises(HostError) as retry:
            await host.call(
                "disconnecting",
                "mutate",
                {},
                principal="test",
                call_id="disconnect-call",
            )
        assert retry.value.code == "idempotency_indeterminate"
    finally:
        await host.close()

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import shlex
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from openagent_host_tools import CapabilityBridge, CapabilityHost, HostError, HostPaths
from openagent_host_tools.bridge import _prepare_result_artifacts
from openagent_host_tools.idempotency import IdempotencyLedger
from openagent_host_tools.types import ToolResult


@pytest.mark.asyncio
async def test_gateway_bridge_uses_exact_generation_and_preserves_result(tmp_path: Path):
    host = CapabilityHost(paths=HostPaths.discover(tmp_path / "user"), cwd=tmp_path)
    await host.start()
    await host.set_consent(True)
    sent: list[dict] = []

    async def send(frame: dict) -> None:
        sent.append(frame)

    bridge = CapabilityBridge(
        host,
        client_instance_id="cli-instance",
        generation=7,
        device_label="test-device",
        trusted_account_id="account-1",
        trusted_device_id="device-1",
        send_json=send,
    )
    try:
        hello = await bridge.hello()
        assert hello["type"] == "capability_hello"
        assert hello["protocol"] == "client-capabilities/1"
        assert hello["client_instance_id"] == "cli-instance"
        assert {item["name"] for item in hello["servers"]} >= {
            "filesystem",
            "editor",
            "shell",
        }
        await bridge.handle(
            {
                "type": "client_tool_call",
                "call_id": "call-1",
                "generation": 7,
                "server": "filesystem",
                "tool": "list_directory",
                "args": {"path": str(tmp_path)},
                "arguments_sha256": hashlib.sha256(
                    json.dumps(
                        {"path": str(tmp_path)},
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode("utf-8")
                ).hexdigest(),
                "deadline_ms": 5000,
                "session_id": "session-1",
                "idempotency_key": "list-1",
                "account_id": "account-1",
            }
        )
        for _ in range(100):
            if any(item.get("call_id") == "call-1" for item in sent):
                break
            await asyncio.sleep(0.01)
        result = next(item for item in sent if item.get("call_id") == "call-1")
        assert result["type"] == "client_tool_result"
        assert result["generation"] == 7
        assert result["result"]["content"]
        assert result["result"]["_meta"]["openagent/location"] == "client"

        await bridge.handle(
            {
                "type": "client_tool_call",
                "call_id": "stale",
                "generation": 6,
                "server": "filesystem",
                "tool": "list_directory",
                "args": {"path": str(tmp_path)},
                "account_id": "account-1",
            }
        )
        stale = next(item for item in sent if item.get("call_id") == "stale")
        assert stale["error"]["code"] == "stale_generation"

        await bridge.handle(
            {
                "type": "client_tool_call",
                "call_id": "bad-hash",
                "generation": 7,
                "server": "filesystem",
                "tool": "list_directory",
                "args": {"path": str(tmp_path)},
                "arguments_sha256": "0" * 64,
                "account_id": "account-1",
            }
        )
        for _ in range(100):
            if any(item.get("call_id") == "bad-hash" for item in sent):
                break
            await asyncio.sleep(0.01)
        mismatch = next(item for item in sent if item.get("call_id") == "bad-hash")
        assert mismatch["error"]["code"] == "arguments_hash_mismatch"

        await bridge.handle(
            {
                "type": "client_tool_call",
                "call_id": "wrong-account",
                "generation": 7,
                "server": "filesystem",
                "tool": "list_directory",
                "args": {"path": str(tmp_path)},
                "account_id": "account-2",
            }
        )
        for _ in range(100):
            if any(item.get("call_id") == "wrong-account" for item in sent):
                break
            await asyncio.sleep(0.01)
        account_error = next(
            item for item in sent if item.get("call_id") == "wrong-account"
        )
        assert account_error["error"]["code"] == "account_mismatch"

        await bridge.handle(
            {
                "type": "client_tool_cancel",
                "call_id": "stale-cancel",
                "generation": 6,
            }
        )
        stale_cancel = next(
            item for item in sent if item.get("call_id") == "stale-cancel"
        )
        assert stale_cancel["error"]["code"] == "stale_generation"
        assert bridge.principal["generation"] == 7
        assert bridge.principal["device_id"] == "device-1"
    finally:
        await bridge.close()
        await host.close()


def test_cross_language_argument_hash_vectors():
    assert IdempotencyLedger.arguments_sha256({"x": 1.0}) == IdempotencyLedger.arguments_sha256(
        {"x": 1}
    )
    assert IdempotencyLedger.arguments_sha256(
        {"nested": [-0.0, {"v": 2.0}]}
    ) == IdempotencyLedger.arguments_sha256({"nested": [0, {"v": 2}]})
    with pytest.raises(Exception, match="NaN"):
        IdempotencyLedger.arguments_sha256({"bad": float("nan")})
    assert IdempotencyLedger.arguments_sha256(
        {"x": 1.0, "nested": [-0.0, {"v": 2.0}], "text": "è"}
    ) == "4dc3a30bb9d5c2c92d2135735330fb49b793376890a07e80b55536ad8d8d5009"


@pytest.mark.asyncio
async def test_gateway_bridge_chunks_large_artifacts():
    class ArtifactHost:
        def __init__(self):
            self.sinks = set()

        async def start(self):
            return None

        async def catalog(self):
            return []

        def subscribe_events(self, sink):
            self.sinks.add(sink)

        def unsubscribe_events(self, sink):
            self.sinks.discard(sink)

        async def call(self, *args, **kwargs):
            del args, kwargs
            return ToolResult(
                content=[
                    {
                        "type": "image",
                        "mimeType": "image/png",
                        "data": base64.b64encode(b"x" * (600 * 1024)).decode(),
                    }
                ],
                extra={
                    "videos": [
                        {
                            "type": "video",
                            "mimeType": "video/mp4",
                            "data": base64.b64encode(b"v" * (300 * 1024)).decode(),
                        }
                    ],
                    "resources": [
                        {
                            "type": "resource",
                            "resource": {
                                "uri": "file:///client/report.bin",
                                "mimeType": "application/octet-stream",
                                "blob": base64.b64encode(b"r" * (280 * 1024)).decode(),
                            },
                        }
                    ],
                },
            )

        async def cancel(self, call_id):
            del call_id
            return False

        async def release_principal(self, principal):
            del principal

    sent: list[dict] = []

    async def send(frame):
        sent.append(frame)

    bridge = CapabilityBridge(
        ArtifactHost(),
        client_instance_id="cli-artifacts",
        generation=9,
        trusted_account_id="account-1",
        send_json=send,
    )
    await bridge.handle(
        {
            "type": "client_tool_call",
            "call_id": "artifact-call",
            "generation": 9,
            "server": "computer-control",
            "tool": "computer",
            "args": {},
            "account_id": "account-1",
        }
    )
    for _ in range(100):
        if any(frame.get("type") == "client_tool_result" for frame in sent):
            break
        await asyncio.sleep(0.01)
    chunks = [frame for frame in sent if frame.get("type") == "client_artifact_chunk"]
    result = next(frame for frame in sent if frame.get("type") == "client_tool_result")
    assert len(chunks) == 4
    assert chunks[-1]["eof"] is True
    content_ref = result["result"]["content"][0]["transfer_id"]
    assert result["result"]["content"] == [
        {
            "type": "artifact_ref",
            "transfer_id": content_ref,
            "artifact_template": {"type": "image", "mimeType": "image/png"},
            "artifact_insert_path": ["data"],
        }
    ]
    video_ref = result["result"]["videos"][0]
    assert video_ref["type"] == "artifact_ref"
    assert video_ref["artifact_template"] == {
        "type": "video",
        "mimeType": "video/mp4",
    }
    assert video_ref["artifact_insert_path"] == ["data"]
    resource_ref = result["result"]["resources"][0]
    assert resource_ref["artifact_template"] == {
        "type": "resource",
        "resource": {
            "uri": "file:///client/report.bin",
            "mimeType": "application/octet-stream",
        },
    }
    assert resource_ref["artifact_insert_path"] == ["resource", "blob"]
    image_chunks = [
        frame for frame in chunks if frame["transfer_id"] == content_ref
    ]
    assert image_chunks[0]["size"] == 600 * 1024
    assert b"".join(base64.b64decode(frame["data"]) for frame in image_chunks) == b"x" * (
        600 * 1024
    )
    await bridge.close()


def test_artifact_transfer_count_is_bounded_before_gateway_dispatch():
    encoded = base64.b64encode(b"x" * (256 * 1024)).decode()
    result = {
        "content": [
            {"type": "image", "mimeType": "image/png", "data": encoded}
            for _ in range(65)
        ],
        "isError": False,
    }
    with pytest.raises(HostError) as error:
        _prepare_result_artifacts(result, "too-many")
    assert error.value.code == "too_many_artifacts"


@pytest.mark.asyncio
async def test_shell_event_retries_until_ack_and_replays_after_reconnect():
    class EventHost:
        def __init__(self):
            self.sinks = set()
            self.event = None
            self.acks = []

        async def start(self):
            return None

        async def catalog(self):
            return []

        def subscribe_events(self, sink):
            self.sinks.add(sink)
            if self.event is not None:
                asyncio.create_task(sink(dict(self.event)))

        def unsubscribe_events(self, sink):
            self.sinks.discard(sink)

        async def ack_event(self, principal, shell_id):
            self.acks.append((principal, shell_id))
            self.event = None
            return True

        async def release_principal(self, principal):
            del principal

        async def cancel(self, call_id):
            del call_id
            return False

        async def emit(self, event):
            self.event = dict(event)
            for sink in list(self.sinks):
                await sink(dict(event))

    host = EventHost()
    sent: list[dict] = []

    async def send(frame):
        sent.append(frame)

    bridge = CapabilityBridge(
        host,
        client_instance_id="cli-events",
        generation=4,
        trusted_account_id="account-1",
        trusted_device_id="device-1",
        send_json=send,
    )
    await bridge.hello()
    bridge.activate_events()
    principal = bridge.principal
    event = {
        "type": "shell_completed",
        "server": "shell",
        "shell_id": "sh_event",
        "status": "completed",
        "principal": json.dumps(
            principal, sort_keys=True, separators=(",", ":")
        ),
    }
    await host.emit(event)
    assert len([item for item in sent if item.get("type") == "client_tool_event"]) == 1
    await bridge.heartbeat()
    assert len([item for item in sent if item.get("type") == "client_tool_event"]) == 2

    # A transport reconnect keeps the principal and broker event alive. The
    # new bridge receives the cached event again before any ACK was observed.
    await bridge.close(release_principals=False)
    reconnected_sent: list[dict] = []

    async def send_reconnected(frame):
        reconnected_sent.append(frame)

    second = CapabilityBridge(
        host,
        client_instance_id="cli-events",
        generation=4,
        trusted_account_id="account-1",
        trusted_device_id="device-1",
        send_json=send_reconnected,
    )
    await second.hello()
    second.activate_events()
    for _ in range(20):
        if any(item.get("type") == "client_tool_event" for item in reconnected_sent):
            break
        await asyncio.sleep(0)
    assert any(item.get("type") == "client_tool_event" for item in reconnected_sent)
    assert await second.handle(
        {
            "type": "client_tool_event_ack",
            "generation": 4,
            "shell_id": "sh_event",
            "accepted": True,
        }
    )
    assert host.acks[-1][1] == "sh_event"
    before = len(reconnected_sent)
    await second.heartbeat()
    assert not any(
        item.get("type") == "client_tool_event"
        for item in reconnected_sent[before:]
    )
    await second.close()


@pytest.mark.asyncio
async def test_bridge_maps_possible_mutation_effect_to_gateway_error():
    class IndeterminateHost:
        async def call(self, *args, **kwargs):
            del args, kwargs
            raise HostError(
                "idempotency_indeterminate",
                "previous mutation may have completed",
                {"manual_reconciliation_required": True},
            )

        async def cancel(self, call_id):
            del call_id
            return False

        async def release_principal(self, principal):
            del principal

    sent: list[dict] = []

    async def send(frame):
        sent.append(frame)

    bridge = CapabilityBridge(
        IndeterminateHost(),
        client_instance_id="cli-idem",
        generation=3,
        trusted_account_id="account-1",
        send_json=send,
    )
    await bridge.handle(
        {
            "type": "client_tool_call",
            "call_id": "idem-call",
            "generation": 3,
            "server": "filesystem",
            "tool": "write_file",
            "args": {},
            "account_id": "account-1",
        }
    )
    for _ in range(100):
        if sent:
            break
        await asyncio.sleep(0.01)
    assert sent[0]["error"]["code"] == "CLIENT_RESULT_INDETERMINATE"
    assert sent[0]["error"]["data"]["local_code"] == "idempotency_indeterminate"
    await bridge.close()


@pytest.mark.asyncio
async def test_bridge_close_cancels_read_claim_before_exact_reconnect_retry(
    tmp_path: Path,
):
    target = tmp_path / "slow-read.txt"
    target.write_text("retry-ok")
    host = CapabilityHost(paths=HostPaths.discover(tmp_path / "user"), cwd=tmp_path)
    await host.start()
    await host.set_consent(True)
    filesystem = host._servers["filesystem"]
    original = filesystem._tool_read_text_file
    entered = threading.Event()
    release = threading.Event()

    def slow_read(args):
        entered.set()
        release.wait(timeout=3)
        return original(args)

    filesystem._tool_read_text_file = slow_read
    args = {"path": str(target)}
    digest = IdempotencyLedger.arguments_sha256(args)
    first_sent: list[dict] = []
    first = CapabilityBridge(
        host,
        client_instance_id="cli-retry",
        generation=12,
        trusted_account_id="account-1",
        trusted_device_id="device-1",
        send_json=first_sent.append,
    )
    await first.handle(
        {
            "type": "client_tool_call",
            "call_id": "retry-read",
            "idempotency_key": "retry-read",
            "generation": 12,
            "account_id": "account-1",
            "server": "filesystem",
            "tool": "read_text_file",
            "args": args,
            "arguments_sha256": digest,
        }
    )
    assert await asyncio.to_thread(entered.wait, 2)
    await first.close(release_principals=False)
    release.set()

    filesystem._tool_read_text_file = original
    retry_sent: list[dict] = []
    retry = CapabilityBridge(
        host,
        client_instance_id="cli-retry",
        generation=12,
        trusted_account_id="account-1",
        trusted_device_id="device-1",
        send_json=retry_sent.append,
    )
    try:
        await retry.handle(
            {
                "type": "client_tool_call",
                "call_id": "retry-read",
                "idempotency_key": "retry-read",
                "generation": 12,
                "account_id": "account-1",
                "server": "filesystem",
                "tool": "read_text_file",
                "args": args,
                "arguments_sha256": digest,
            }
        )
        for _ in range(100):
            if any(item.get("type") == "client_tool_result" for item in retry_sent):
                break
            await asyncio.sleep(0.01)
        result = next(
            item for item in retry_sent if item.get("type") == "client_tool_result"
        )
        assert result["result"]["content"][0]["text"] == "retry-ok"
    finally:
        await retry.close()
        await host.close()


@pytest.mark.asyncio
async def test_bridge_transport_reconnect_preserves_returned_background_shell(
    tmp_path: Path,
):
    host = CapabilityHost(paths=HostPaths.discover(tmp_path / "user"), cwd=tmp_path)
    await host.start()
    await host.set_consent(True)
    executable = [sys.executable, "-c", "import time; time.sleep(30)"]
    command = (
        subprocess.list2cmdline(executable)
        if os.name == "nt"
        else shlex.join(executable)
    )

    async def call_and_result(bridge, sent, call_id, tool, args):
        await bridge.handle(
            {
                "type": "client_tool_call",
                "call_id": call_id,
                "generation": 14,
                "account_id": "account-1",
                "server": "shell",
                "tool": tool,
                "args": args,
            }
        )
        for _ in range(200):
            match = next(
                (item for item in sent if item.get("call_id") == call_id), None
            )
            if match is not None:
                return match
            await asyncio.sleep(0.01)
        raise AssertionError(f"no result for {call_id}")

    first_sent: list[dict] = []
    first = CapabilityBridge(
        host,
        client_instance_id="cli-background",
        generation=14,
        trusted_account_id="account-1",
        trusted_device_id="device-1",
        send_json=first_sent.append,
    )
    started = await call_and_result(
        first,
        first_sent,
        "background-start",
        "shell_exec",
        {"command": command, "run_in_background": True},
    )
    shell_id = started["result"]["structuredContent"]["shell_id"]
    await first.close(release_principals=False)

    second_sent: list[dict] = []
    second = CapabilityBridge(
        host,
        client_instance_id="cli-background",
        generation=14,
        trusted_account_id="account-1",
        trusted_device_id="device-1",
        send_json=second_sent.append,
    )
    try:
        listed = await call_and_result(
            second, second_sent, "background-list", "shell_list", {}
        )
        assert [
            item["shell_id"]
            for item in listed["result"]["structuredContent"]["shells"]
        ] == [shell_id]
    finally:
        await second.close()
        await host.close()

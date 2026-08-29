from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

from openagent_host_tools import CapabilityBridge, CapabilityHost, HostError, HostPaths
from openagent_host_tools import mcp_stdio as mcp_stdio_module
from openagent_host_tools.config import PluginSpec
from openagent_host_tools.context import current_principal
from openagent_host_tools.mcp_stdio import MCPStdioServer, PerPrincipalMCPPool
from openagent_host_tools.sidecars import AGENT_IN_CHROME_MANIFEST


_SUPERVISED_MCP = r'''import json, os, pathlib, sys, threading

state = pathlib.Path(os.environ["TEST_STATE_PATH"])
generation = int(state.read_text()) + 1 if state.exists() else 1
state.write_text(str(generation))
if generation == 1 and os.environ.get("TEST_FAIL_FIRST_START") == "1":
    sys.exit(29)

def reply(request_id, result):
    print(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}), flush=True)

for line in sys.stdin:
    value = json.loads(line)
    request_id = value.get("id")
    if request_id is None:
        continue
    method = value.get("method")
    if method == "initialize":
        reply(request_id, {
            "protocolVersion": "2024-11-05",
            "serverInfo": {"name": os.environ["TEST_SERVER_NAME"], "version": "1.0"},
        })
    elif method == "tools/list":
        reply(request_id, {"tools": [
            {
                "name": "mutate",
                "description": "record an effect",
                "inputSchema": {"type": "object"},
            },
            {
                "name": "inspect",
                "description": "show generation",
                "inputSchema": {"type": "object"},
                "annotations": {"readOnlyHint": True},
            },
            {
                "name": "large_image",
                "description": "return an image larger than asyncio's default line limit",
                "inputSchema": {"type": "object"},
                "annotations": {"readOnlyHint": True},
            },
        ]})
        if os.environ.get("TEST_EXIT_AFTER_LIST") == "1":
            threading.Timer(0.03, lambda: os._exit(23)).start()
    elif method == "tools/call":
        arguments = value.get("params", {}).get("arguments", {})
        if arguments.get("crash"):
            effect = os.environ.get("TEST_EFFECT_PATH")
            if effect:
                pathlib.Path(effect).write_text(f"effect-{generation}")
            os._exit(17)
        if value.get("params", {}).get("name") == "large_image":
            payload = "A" * (256 * 1024)
            reply(request_id, {
                "content": [{"type": "image", "mimeType": "image/png", "data": payload}],
                "structuredContent": {"encoded_size": len(payload)},
                "isError": False,
            })
            continue
        reply(request_id, {
            "content": [{"type": "text", "text": f"generation-{generation}"}],
            "structuredContent": {"generation": generation},
            "isError": False,
        })
'''


def _write_mcp(tmp_path: Path) -> Path:
    script = tmp_path / "supervised_mcp.py"
    script.write_text(_SUPERVISED_MCP, encoding="utf-8")
    return script


async def _wait_until(predicate, *, timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition did not become true")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_stdio_preserves_results_larger_than_asyncio_default_line_limit(
    tmp_path: Path,
):
    script = _write_mcp(tmp_path)
    state = tmp_path / "large-result-starts.txt"
    adapter = MCPStdioServer(
        PluginSpec(
            "large-result",
            (sys.executable, str(script)),
            env={
                "TEST_STATE_PATH": str(state),
                "TEST_SERVER_NAME": "large-result",
            },
        )
    )
    try:
        await adapter.start()
        result = await adapter.call("large_image", {})
        assert result.is_error is False
        assert result.structured_content == {"encoded_size": 256 * 1024}
        assert result.content == [
            {
                "type": "image",
                "mimeType": "image/png",
                "data": "A" * (256 * 1024),
            }
        ]
        assert adapter.manifest.available is True
        assert adapter.process is not None
        assert adapter.process.returncode is None
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_stdio_oversized_frame_fails_closed_and_recovers_via_supervisor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    script = _write_mcp(tmp_path)
    state = tmp_path / "oversized-frame-starts.txt"
    monkeypatch.setattr(
        mcp_stdio_module,
        "_MAX_MCP_STDIO_LINE_BYTES",
        128 * 1024,
    )
    adapter = MCPStdioServer(
        PluginSpec(
            "oversized-frame",
            (sys.executable, str(script)),
            env={
                "TEST_STATE_PATH": str(state),
                "TEST_SERVER_NAME": "oversized-frame",
            },
        ),
        restart_limit=1,
        restart_initial_delay=0.01,
        restart_max_delay=0.01,
    )
    try:
        await adapter.start()
        with pytest.raises(HostError, match="stdout frame exceeded"):
            await adapter.call("large_image", {})
        await _wait_until(
            lambda: state.read_text(encoding="utf-8") == "2"
            and adapter.manifest.available is True
        )
        recovered = await adapter.call("inspect", {})
        assert recovered.structured_content == {"generation": 2}
        assert adapter.process is not None
        assert adapter.process.returncode is None
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_stdio_runtime_restart_is_bounded_and_shutdown_does_not_restart(
    tmp_path: Path,
):
    script = _write_mcp(tmp_path)
    state = tmp_path / "starts.txt"
    changes: list[bool] = []
    adapter = MCPStdioServer(
        PluginSpec(
            "flapping",
            (sys.executable, str(script)),
            env={
                "TEST_STATE_PATH": str(state),
                "TEST_SERVER_NAME": "flapping",
                "TEST_EXIT_AFTER_LIST": "1",
            },
        ),
        on_state_change=lambda server: changes.append(server.manifest.available),
        restart_limit=2,
        restart_initial_delay=0.01,
        restart_max_delay=0.02,
    )
    try:
        await adapter.start()
        await _wait_until(
            lambda: state.exists()
            and state.read_text(encoding="utf-8") == "3"
            and not adapter.manifest.available
            and adapter._restart_task is None
        )
        assert adapter._restart_attempts == 2
        assert changes.count(True) == 2
        assert changes.count(False) == 3
        await asyncio.sleep(0.08)
        assert state.read_text(encoding="utf-8") == "3"
    finally:
        await adapter.close()
    await asyncio.sleep(0.05)
    assert state.read_text(encoding="utf-8") == "3"


@pytest.mark.asyncio
async def test_stdio_shutdown_cancels_a_pending_backoff_restart(tmp_path: Path):
    script = _write_mcp(tmp_path)
    state = tmp_path / "shutdown-starts.txt"
    adapter = MCPStdioServer(
        PluginSpec(
            "shutdown-test",
            (sys.executable, str(script)),
            env={
                "TEST_STATE_PATH": str(state),
                "TEST_SERVER_NAME": "shutdown-test",
            },
        ),
        restart_limit=1,
        restart_initial_delay=0.2,
        restart_max_delay=0.2,
    )
    await adapter.start()
    with pytest.raises(HostError, match="disconnected"):
        await adapter.call("mutate", {"crash": True})
    await _wait_until(
        lambda: not adapter.manifest.available
        and adapter._restart_task is not None
    )
    await adapter.close()
    await asyncio.sleep(0.25)
    assert state.read_text(encoding="utf-8") == "1"


@pytest.mark.asyncio
async def test_per_principal_sidecar_death_updates_health_and_restarts(tmp_path: Path):
    script = _write_mcp(tmp_path)
    state = tmp_path / "chrome-starts.txt"
    pool = PerPrincipalMCPPool(
        PluginSpec(
            "agent-in-chrome",
            (sys.executable, str(script)),
            env={
                "TEST_STATE_PATH": str(state),
                "TEST_SERVER_NAME": "agent-in-chrome",
                "TEST_EFFECT_PATH": str(tmp_path / "chrome-effect.txt"),
            },
        ),
        placeholder=AGENT_IN_CHROME_MANIFEST,
        data_root=tmp_path / "chrome",
        restart_limit=1,
        restart_initial_delay=0.2,
        restart_max_delay=0.2,
    )
    await pool.start()
    principal_a = json.dumps(
        {
            "kind": "interactive-client",
            "client_instance_id": "desktop-a",
            "account_id": "account-a",
            "network_id": "network-a",
        }
    )
    principal_b = json.dumps(
        {
            "kind": "interactive-client",
            "client_instance_id": "desktop-b",
            "account_id": "account-b",
            "network_id": "network-b",
        }
    )
    try:
        token_b = current_principal.set(principal_b)
        healthy_b = await pool.call("inspect", {})
        current_principal.reset(token_b)

        token_a = current_principal.set(principal_a)
        with pytest.raises(HostError, match="disconnected"):
            await pool.call("mutate", {"crash": True})
        current_principal.reset(token_a)
        await _wait_until(
            lambda: pool.availability_for_principal(principal_a)[0] is False
        )

        # One failed network/account must not remove Chrome from the machine's
        # catalog or reject a different account whose sidecar is still alive.
        assert pool.manifest.available is True
        assert pool.availability_for_principal(principal_b) == (True, None)
        token_b = current_principal.set(principal_b)
        still_healthy_b = await pool.call("inspect", {})
        current_principal.reset(token_b)
        assert still_healthy_b.structured_content == healthy_b.structured_content

        await _wait_until(
            lambda: pool.availability_for_principal(principal_a)[0] is True
        )
        token_a = current_principal.set(principal_a)
        recovered = await pool.call("inspect", {})
        current_principal.reset(token_a)
        assert recovered.structured_content["generation"] == 4
    finally:
        current_principal.set(None)
        await pool.close()
    starts_after_close = state.read_text(encoding="utf-8")
    await asyncio.sleep(0.05)
    assert state.read_text(encoding="utf-8") == starts_after_close


@pytest.mark.asyncio
async def test_host_supervises_a_transient_failure_on_the_first_start(tmp_path: Path):
    script = _write_mcp(tmp_path)
    paths = HostPaths.discover(tmp_path / "first-start-user")
    state = tmp_path / "first-starts.txt"
    host = CapabilityHost(
        paths=paths,
        cwd=tmp_path,
        external_restart_limit=2,
        external_restart_initial_delay=0.01,
        external_restart_max_delay=0.01,
    )
    host.plugin_store.save(
        [
            PluginSpec(
                "first-start",
                (sys.executable, str(script)),
                env={
                    "TEST_STATE_PATH": str(state),
                    "TEST_SERVER_NAME": "first-start",
                    "TEST_FAIL_FIRST_START": "1",
                },
            )
        ]
    )
    try:
        await host.set_consent(True)
        await _wait_until(
            lambda: state.exists()
            and state.read_text(encoding="utf-8") == "2"
            and host._health["first-start"]["available"] is True
        )
        assert "first-start" in {
            server["name"] for server in await host.catalog()
        }
        result = await host.call(
            "first-start",
            "inspect",
            {},
            principal="first-start-principal",
            call_id="first-start-call",
        )
        assert result.structured_content == {"generation": 2}
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_host_supervises_transient_chrome_catalog_probe_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    script = _write_mcp(tmp_path)
    paths = HostPaths.discover(tmp_path / "chrome-probe-user")
    state = tmp_path / "chrome-probe-starts.txt"
    monkeypatch.setenv(
        "OPENAGENT_AGENT_IN_CHROME_COMMAND",
        json.dumps([sys.executable, str(script)]),
    )
    monkeypatch.setenv("TEST_STATE_PATH", str(state))
    monkeypatch.setenv("TEST_SERVER_NAME", "agent-in-chrome")
    monkeypatch.setenv("TEST_FAIL_FIRST_START", "1")
    host = CapabilityHost(
        paths=paths,
        cwd=tmp_path,
        external_restart_limit=2,
        external_restart_initial_delay=0.01,
        external_restart_max_delay=0.01,
    )
    try:
        await host.set_consent(True)
        await _wait_until(
            lambda: state.exists()
            and state.read_text(encoding="utf-8") == "2"
            and host._health["agent-in-chrome"]["available"] is True
        )
        chrome = next(
            server
            for server in await host.catalog()
            if server["name"] == "agent-in-chrome"
        )
        assert {tool["name"] for tool in chrome["tools"]} == {
            "mutate",
            "inspect",
            "large_image",
        }
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_bridge_automatically_publishes_death_and_restart_catalogs(
    tmp_path: Path,
):
    script = _write_mcp(tmp_path)
    paths = HostPaths.discover(tmp_path / "user")
    state = tmp_path / "plugin-starts.txt"
    effect = tmp_path / "effect.txt"
    host = CapabilityHost(
        paths=paths,
        cwd=tmp_path,
        external_restart_limit=1,
        external_restart_initial_delay=0.01,
        external_restart_max_delay=0.01,
    )
    host.plugin_store.save(
        [
            PluginSpec(
                "supervised",
                (sys.executable, str(script)),
                env={
                    "TEST_STATE_PATH": str(state),
                    "TEST_SERVER_NAME": "supervised",
                    "TEST_EFFECT_PATH": str(effect),
                },
            )
        ]
    )
    sent: list[dict] = []
    bridge = CapabilityBridge(
        host,
        client_instance_id="desktop",
        generation=41,
        trusted_account_id="account-a",
        trusted_network_id="network-a",
        trusted_device_id="device-a",
        send_json=lambda frame: sent.append(frame),
    )
    try:
        await host.start()
        await host.set_consent(True)
        hello = await bridge.hello()
        assert "supervised" in {server["name"] for server in hello["servers"]}
        bridge.activate_events()
        await bridge.handle(
            {
                "type": "client_tool_call",
                "generation": 41,
                "call_id": "mutating-crash",
                "server": "supervised",
                "tool": "mutate",
                "args": {"crash": True},
                "account_id": "account-a",
                "network_id": "network-a",
                "idempotency_key": "mutating-crash",
            }
        )
        await _wait_until(
            lambda: any(
                frame.get("call_id") == "mutating-crash" for frame in sent
            )
            and len(
                [
                    frame
                    for frame in sent
                    if frame.get("type") == "capability_catalog_update"
                ]
            )
            >= 2
        )
        result = next(
            frame for frame in sent if frame.get("call_id") == "mutating-crash"
        )
        assert result["error"]["code"] == "CLIENT_RESULT_INDETERMINATE"
        assert effect.read_text(encoding="utf-8") == "effect-1"
        catalogs = [
            {server["name"] for server in frame["servers"]}
            for frame in sent
            if frame.get("type") == "capability_catalog_update"
        ]
        assert "supervised" not in catalogs[-2]
        assert "supervised" in catalogs[-1]
        assert state.read_text(encoding="utf-8") == "2"

        await host.set_consent(False)
        starts_after_revoke = state.read_text(encoding="utf-8")
        await asyncio.sleep(0.05)
        assert state.read_text(encoding="utf-8") == starts_after_revoke
        assert sent[-1]["type"] == "capability_catalog_update"
        assert sent[-1]["servers"] == []
    finally:
        await bridge.close()
        await host.close()

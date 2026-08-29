from __future__ import annotations

import json
import http.server
import shutil
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from openagent_host_tools.config import PluginSpec
from openagent_host_tools.context import current_principal
from openagent_host_tools.mcp_stdio import PerPrincipalMCPPool
from openagent_host_tools.sidecars import AGENT_IN_CHROME_MANIFEST


_FAKE_MCP = r'''import json, os, sys
for line in sys.stdin:
    value = json.loads(line)
    request_id = value.get("id")
    if request_id is None:
        continue
    method = value.get("method")
    if method == "initialize":
        result = {"protocolVersion": "2024-11-05", "serverInfo": {"name": "agent-in-chrome", "version": "2.0.0"}}
    elif method == "tools/list":
        result = {"tools": [{"name": "tabs_context_mcp", "description": "probe", "inputSchema": {"type": "object"}, "annotations": {"readOnlyHint": True}}]}
    elif method == "tools/call":
        result = {"content": [{"type": "text", "text": "ok"}], "structuredContent": {"profile": os.environ["OPENAGENT_CHROME_PROFILE_DIR"], "extensions": os.environ["OPENAGENT_CHROME_EXTENSIONS_DIR"], "port": os.environ["OPENAGENT_CHROME_CDP_PORT"], "pid": os.getpid()}}
    else:
        result = {}
    print(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}), flush=True)
'''


def _principal(instance: str, account: str) -> str:
    return json.dumps(
        {
            "kind": "interactive-client",
            "client_instance_id": instance,
            "device_label": "test",
            "account_id": account,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def test_browser_reuse_requires_exact_profile_ownership_marker(tmp_path: Path):
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is required to exercise the Agent in Chrome sidecar")

    endpoint_path = "/devtools/browser/openagent-owned"

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib callback name
            assert self.path == "/json/version"
            payload = json.dumps(
                {
                    "Browser": "Chrome/1.2.3.4",
                    "webSocketDebuggerUrl": (
                        f"ws://127.0.0.1:{self.server.server_port}{endpoint_path}"
                    ),
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        owned = tmp_path / "owned"
        unrelated = tmp_path / "unrelated"
        wrong_port = tmp_path / "wrong-port"
        for profile in (owned, unrelated, wrong_port):
            profile.mkdir()
        port = server.server_port
        (owned / "DevToolsActivePort").write_text(f"{port}\n{endpoint_path}\n")
        (unrelated / "DevToolsActivePort").write_text(
            f"{port}\n/devtools/browser/someone-else\n"
        )
        (wrong_port / "DevToolsActivePort").write_text(
            f"{port + 1}\n{endpoint_path}\n"
        )
        browser_js = (
            Path(__file__).parents[1]
            / "sidecars"
            / "agent-in-chrome"
            / "host"
            / "browser.js"
        )
        script = r'''
import { pathToFileURL } from "node:url";
const browser = await import(pathToFileURL(process.argv[1]).href);
const port = Number(process.argv[5]);
const result = {
  owned: await browser.getProfileWsEndpoint(process.argv[2], port),
  unrelated: await browser.getProfileWsEndpoint(process.argv[3], port),
  wrongPort: await browser.getProfileWsEndpoint(process.argv[4], port),
};
process.stdout.write(JSON.stringify(result));
'''
        completed = subprocess.run(
            [
                node,
                "--input-type=module",
                "--eval",
                script,
                str(browser_js),
                str(owned),
                str(unrelated),
                str(wrong_port),
                str(port),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        result = json.loads(completed.stdout)
        assert result == {
            "owned": f"ws://127.0.0.1:{port}{endpoint_path}",
            "unrelated": None,
            "wrongPort": None,
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_chromium_snapshot_fallback_never_uses_nonexistent_arm_archives():
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is required to inspect the browser runtime")
    browser_js = (
        Path(__file__).parents[1]
        / "sidecars"
        / "agent-in-chrome"
        / "host"
        / "browser.js"
    )
    script = r'''
import { pathToFileURL } from "node:url";
const browser = await import(pathToFileURL(process.argv[1]).href);
const result = {
  linuxX64: browser.chromiumSnapshotSpec("linux", "x64"),
  winX64: browser.chromiumSnapshotSpec("win32", "x64"),
  errors: [],
};
for (const [system, arch] of [["linux", "arm64"], ["win32", "arm64"]]) {
  try { browser.chromiumSnapshotSpec(system, arch); }
  catch (error) { result.errors.push(String(error.message)); }
}
process.stdout.write(JSON.stringify(result));
'''
    completed = subprocess.run(
        [node, "--input-type=module", "--eval", script, str(browser_js)],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    result = json.loads(completed.stdout)
    assert result["linuxX64"] == {
        "archPath": "Linux_x64",
        "archiveName": "chrome-linux.zip",
    }
    assert result["winX64"] == {
        "archPath": "Win_x64",
        "archiveName": "chrome-win.zip",
    }
    assert len(result["errors"]) == 2
    assert all("OPENAGENT_CHROME_BINARY" in message for message in result["errors"])
    assert all("Win_Arm" not in message for message in result["errors"])


@pytest.mark.asyncio
async def test_chrome_pool_is_per_account_and_reference_counts_instances(tmp_path: Path):
    script = tmp_path / "fake_chrome_mcp.py"
    script.write_text(_FAKE_MCP)
    pool = PerPrincipalMCPPool(
        PluginSpec("agent-in-chrome", (sys.executable, str(script))),
        placeholder=AGENT_IN_CHROME_MANIFEST,
        data_root=tmp_path / "chrome",
    )
    await pool.start()
    first = _principal("desktop", "network-a")
    second = _principal("cli", "network-a")
    other = _principal("desktop", "network-b")
    try:
        token = current_principal.set(first)
        one = await pool.call("tabs_context_mcp", {})
        current_principal.reset(token)
        token = current_principal.set(second)
        two = await pool.call("tabs_context_mcp", {})
        current_principal.reset(token)
        token = current_principal.set(other)
        three = await pool.call("tabs_context_mcp", {})
        current_principal.reset(token)

        assert one.structured_content["pid"] == two.structured_content["pid"]
        assert one.structured_content["profile"] == two.structured_content["profile"]
        assert one.structured_content["port"] == two.structured_content["port"]
        assert one.structured_content["pid"] != three.structured_content["pid"]
        assert one.structured_content["profile"] != three.structured_content["profile"]
        assert one.structured_content["port"] != three.structured_content["port"]

        await pool.release_principal(first)
        assert "network-a" in pool._instances
        await pool.release_principal(second)
        assert "network-a" not in pool._instances
        assert "network-b" in pool._instances
    finally:
        await pool.close()

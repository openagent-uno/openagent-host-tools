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


def test_browser_runtime_has_no_mutable_snapshot_download_path():
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
    source = browser_js.read_text()
    forbidden = (
        "LAST_CHANGE",
        "chromium-browser-snapshots",
        "downloadChromium",
        "cachedChromiumBinary",
        "CHROMIUM_DIR",
        "No working browser found — downloading Chromium",
    )
    for marker in forbidden:
        assert marker not in source, f"mutable browser download path remains: {marker}"

    script = r'''
import { pathToFileURL } from "node:url";
const browser = await import(pathToFileURL(process.argv[1]).href);
const result = {
  snapshotSpecExported: typeof browser.chromiumSnapshotSpec !== "undefined",
  downloadExported: typeof browser.downloadChromium !== "undefined",
};
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
    assert result == {"snapshotSpecExported": False, "downloadExported": False}


def test_crx3_verification_accepts_valid_signature_and_rejects_tampering():
    """Build and authenticate a real CRX3 container entirely in Node.

    The fixture is generated from a fresh RSA key, not a mocked verifier: its
    extension id is derived from the SPKI, and its signature covers the exact
    CRX3 context string, signed header and ZIP payload.  Flipping one payload
    byte or requesting another id must fail closed.
    """

    node = shutil.which("node")
    if node is None:
        pytest.skip("node is required to exercise CRX3 signature verification")
    browser_js = (
        Path(__file__).parents[1]
        / "sidecars"
        / "agent-in-chrome"
        / "host"
        / "browser.js"
    )
    script = r'''
import crypto from "node:crypto";
import { pathToFileURL } from "node:url";

const browser = await import(pathToFileURL(process.argv[1]).href);

function varint(input) {
  let value = BigInt(input);
  const out = [];
  do {
    let byte = Number(value & 0x7fn);
    value >>= 7n;
    if (value) byte |= 0x80;
    out.push(byte);
  } while (value);
  return Buffer.from(out);
}

function bytesField(number, value) {
  const data = Buffer.from(value);
  return Buffer.concat([varint(number * 8 + 2), varint(data.length), data]);
}

function extensionId(publicKey) {
  const digest = crypto.createHash("sha256").update(publicKey).digest().subarray(0, 16);
  let id = "";
  for (const byte of digest) id += String.fromCharCode(97 + (byte >> 4), 97 + (byte & 15));
  return id;
}

const { publicKey, privateKey } = crypto.generateKeyPairSync("rsa", {
  modulusLength: 2048,
  publicExponent: 0x10001,
});
const spki = publicKey.export({ type: "spki", format: "der" });
const id = extensionId(spki);
const rawId = crypto.createHash("sha256").update(spki).digest().subarray(0, 16);
const signedHeader = bytesField(1, rawId);
// It only needs to be a deterministic ZIP-shaped byte sequence for this
// verifier test; extraction happens strictly after authentication.
const zip = Buffer.from("504b03041400000000000000000000000000000000000000000000", "hex");
const signedHeaderSize = Buffer.alloc(4);
signedHeaderSize.writeUInt32LE(signedHeader.length);
const signedPayload = Buffer.concat([
  Buffer.from("CRX3 SignedData\0", "ascii"),
  signedHeaderSize,
  signedHeader,
  zip,
]);
const signature = crypto.sign("sha256", signedPayload, {
  key: privateKey,
  padding: crypto.constants.RSA_PKCS1_PADDING,
});
const proof = Buffer.concat([bytesField(1, spki), bytesField(2, signature)]);
const header = Buffer.concat([bytesField(2, proof), bytesField(10000, signedHeader)]);
const prefix = Buffer.alloc(12);
prefix.write("Cr24", 0, "latin1");
prefix.writeUInt32LE(3, 4);
prefix.writeUInt32LE(header.length, 8);
const crx = Buffer.concat([prefix, header, zip]);

// A mathematically valid RSA signature mislabeled as ECDSA must not be
// accepted merely because Node can infer the algorithm from its SPKI.
const wrongAlgorithmHeader = Buffer.concat([
  bytesField(3, proof),
  bytesField(10000, signedHeader),
]);
const wrongAlgorithmPrefix = Buffer.alloc(12);
wrongAlgorithmPrefix.write("Cr24", 0, "latin1");
wrongAlgorithmPrefix.writeUInt32LE(3, 4);
wrongAlgorithmPrefix.writeUInt32LE(wrongAlgorithmHeader.length, 8);
const wrongAlgorithmCrx = Buffer.concat([wrongAlgorithmPrefix, wrongAlgorithmHeader, zip]);

const validOffset = browser.verifyCrx3Package(crx, id);
const tampered = Buffer.from(crx);
tampered[tampered.length - 1] ^= 0x01;
const wrongId = id.slice(0, -1) + (id.endsWith("a") ? "b" : "a");

function rejected(fn) {
  try {
    fn();
    return null;
  } catch (error) {
    return String(error.message);
  }
}

process.stdout.write(JSON.stringify({
  validOffset,
  expectedOffset: crx.length - zip.length,
  tamperError: rejected(() => browser.verifyCrx3Package(tampered, id)),
  wrongIdError: rejected(() => browser.verifyCrx3Package(crx, wrongId)),
  wrongAlgorithmError: rejected(() => browser.verifyCrx3Package(wrongAlgorithmCrx, id)),
  invalidIdError: rejected(() => browser.verifyCrx3Package(crx, "not-an-extension-id")),
}));
'''
    completed = subprocess.run(
        [node, "--input-type=module", "--eval", script, str(browser_js)],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    result = json.loads(completed.stdout)
    assert result["validOffset"] == result["expectedOffset"]
    assert "signature verification failed" in result["tamperError"]
    assert "signed extension id does not match" in result["wrongIdError"]
    assert "signature verification failed" in result["wrongAlgorithmError"]
    assert "invalid Chrome Web Store extension id" in result["invalidIdError"]


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

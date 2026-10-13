#!/usr/bin/env python3
"""Exercise UTF-8 NDJSON against the actual frozen host executable."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
from pathlib import Path


async def smoke(executable: Path) -> None:
    # Non-ASCII request IDs test input and output. The home path also appears
    # in real initialize/status results, rather than in a mocked echo server.
    marker = "caffè—東京🙂"
    with tempfile.TemporaryDirectory(prefix="openagent-utf8-") as temporary:
        home = Path(temporary) / marker
        env = dict(os.environ, OPENAGENT_HOST_TOOLS_HOME=str(home),
                   PYTHONUTF8="0", PYTHONIOENCODING="cp1252:strict")
        process = await asyncio.create_subprocess_exec(
            str(executable.resolve()), "--direct", cwd=executable.parent,
            env=env, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        assert process.stdin and process.stdout and process.stderr
        try:
            for kind in ("initialize", "status", "shutdown"):
                request_id = f"{kind}-{marker}"
                frame = {"id": request_id, "type": kind}
                process.stdin.write((json.dumps(frame, ensure_ascii=False) + "\n").encode("utf-8"))
                await process.stdin.drain()
                raw = await asyncio.wait_for(process.stdout.readline(), timeout=20)
                assert raw.endswith(b"\n"), f"missing {kind} NDJSON response"
                reply = json.loads(raw.decode("utf-8", errors="strict"))
                assert reply["id"] == request_id and reply["ok"] is True, reply
                if kind != "shutdown":
                    assert marker in reply["result"]["config_path"], reply
                else:
                    assert reply["result"]["shutting_down"] is True, reply
            process.stdin.close()
            await asyncio.wait_for(process.wait(), timeout=10)
            stderr = await process.stderr.read()
            assert process.returncode == 0, (process.returncode, stderr)
            assert b"Traceback" not in stderr, stderr
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("executable", type=Path)
    args = parser.parse_args()
    asyncio.run(smoke(args.executable))
    print("Frozen NDJSON UTF-8 initialize/status/shutdown: PASS")


if __name__ == "__main__":
    main()

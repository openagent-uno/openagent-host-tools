#!/usr/bin/env python3
"""Refreeze only the Python host; preserve verified 0.1.2 native sidecars."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

from build_standalone import ROOT, platform_key, write_bundle_manifest
from verify_bundle_archive import verify_and_extract

BASE = "https://github.com/openagent-uno/openagent-host-tools/releases/download/v0.1.2/"
INDEX_SHA = "88efc4b74b89796f1862839f8d8f3ec51f463cc15799f9de40a4502ae2421f08"
BASE_SOURCE = "af6ad6871d4d1208874bf79735710d089f59b959"


def fetch(name: str, destination: Path, max_bytes: int) -> bytes:
    if Path(name).name != name:
        raise RuntimeError("invalid release asset name")
    with urllib.request.urlopen(BASE + name, timeout=60) as response:
        content = response.read(max_bytes + 1)
    if len(content) > max_bytes:
        raise RuntimeError("oversized release asset")
    destination.write_bytes(content)
    return content


def main() -> None:
    key = platform_key()
    inputs = ROOT / "build" / "compat-input"
    inputs.mkdir(parents=True, exist_ok=False)
    raw = fetch("release-index.json", inputs / "release-index.json", 100_000)
    if hashlib.sha256(raw).hexdigest() != INDEX_SHA:
        raise RuntimeError("baseline release index checksum mismatch")
    index = json.loads(raw)
    if index["source_commit"] != BASE_SOURCE or index["version"] != "0.1.2":
        raise RuntimeError("baseline release source mismatch")
    pinned = index["platforms"][key]
    archive = inputs / pinned["asset"]
    content = fetch(archive.name, archive, 200_000_000)
    if hashlib.sha256(content).hexdigest() != pinned["archive_sha256"]:
        raise RuntimeError("baseline archive checksum mismatch")
    fetch(archive.name + ".sha256", Path(str(archive) + ".sha256"), 1_000)
    original = verify_and_extract(archive, inputs / "verified",
                                  expected_platform=key, expected_version="0.1.2")
    original_manifest = (original / "bundle-manifest.json").read_bytes()
    if hashlib.sha256(original_manifest).hexdigest() != pinned["bundle_manifest_sha256"]:
        raise RuntimeError("baseline manifest checksum mismatch")
    subprocess.run([sys.executable, str(ROOT / "scripts/build_standalone.py"),
                    "--no-sidecars", "--expect-platform", key], check=True)
    bundle = ROOT / "dist" / key
    host_name = "openagent-host-tools.exe" if key.startswith("win32-") else "openagent-host-tools"
    for item in original.iterdir():
        if item.name in {host_name, "bundle-manifest.json", "THIRD_PARTY_NOTICES.md"}:
            continue
        if item.is_dir():
            shutil.copytree(item, bundle / item.name)
        else:
            shutil.copy2(item, bundle / item.name)
    write_bundle_manifest(bundle, key)
    current = json.loads((bundle / "bundle-manifest.json").read_text())
    prior = json.loads(original_manifest)
    reused = {name: entry for name, entry in prior["files"].items()
              if name not in {host_name, "THIRD_PARTY_NOTICES.md"}}
    if any(current["files"].get(name) != entry for name, entry in reused.items()):
        raise RuntimeError("native sidecar bytes changed during compatibility refreeze")
    receipt = {"baselineVersion": "0.1.2", "baselineSource": BASE_SOURCE,
               "baselineIndexSha256": INDEX_SHA, "baselineArchiveSha256": pinned["archive_sha256"],
               "platform": key, "reusedFiles": reused, "host": current["files"][host_name]}
    (ROOT / "dist" / f"compatibility-{key}.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"Refrozen {key}: {len(reused)} verified native/runtime files unchanged")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Archive one native host-tools bundle and emit a detached SHA-256 file."""

from __future__ import annotations

import argparse
import hashlib
import json
import tarfile
from pathlib import Path

from project_metadata import PROJECT_VERSION


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--version", default=PROJECT_VERSION)
    args = parser.parse_args()
    bundle = args.bundle.resolve()
    if not bundle.is_dir() or bundle.parent.name != "dist":
        raise SystemExit(f"expected dist/<platform> bundle, got {bundle}")
    manifest_path = bundle / "bundle-manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read bundle manifest: {exc}") from exc
    if (
        manifest.get("manifest_version") != 1
        or manifest.get("version") != args.version
        or manifest.get("platform") != bundle.name
    ):
        raise SystemExit("bundle manifest identity does not match package target")
    archive = bundle.parent / f"openagent-host-tools-{bundle.name}.tar.gz"
    checksum = Path(str(archive) + ".sha256")
    # Packaging is frequently rerun in a developer checkout. Remove only old
    # generated archives for this exact platform so a legacy/versioned asset
    # cannot be picked up by a later wildcard upload or release-index build.
    generated_patterns = (
        f"openagent-host-tools-*-{bundle.name}.tar.gz",
        f"openagent-host-tools-*-{bundle.name}.tar.gz.sha256",
        f"openagent-host-tools-*-{bundle.name}.zip",
        f"openagent-host-tools-*-{bundle.name}.zip.sha256",
    )
    generated = {archive, checksum}
    for pattern in generated_patterns:
        generated.update(bundle.parent.glob(pattern))
    for candidate in generated:
        if candidate.is_file():
            candidate.unlink()
    links = [path.relative_to(bundle) for path in bundle.rglob("*") if path.is_symlink()]
    if links:
        raise SystemExit(
            "bundle contains unsupported symbolic links: "
            + ", ".join(path.as_posix() for path in links)
        )
    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        raise SystemExit("bundle manifest has no checksum map")
    actual_files = {
        path.relative_to(bundle).as_posix()
        for path in bundle.rglob("*")
        if path.is_file() and path != manifest_path
    }
    if actual_files != set(files):
        raise SystemExit("bundle file set does not match bundle manifest")
    for relative, expected in files.items():
        path = bundle / relative
        if (
            not isinstance(expected, dict)
            or path.stat().st_size != expected.get("size")
            or sha256(path) != expected.get("sha256")
        ):
            raise SystemExit(f"bundle file integrity mismatch: {relative}")
    # One archive format and one stable asset name across every consumer keeps
    # Desktop, CLI and server release locks mechanically identical.
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(bundle, arcname=bundle.name)
    checksum.write_text(f"{sha256(archive)}  {archive.name}\n", encoding="utf-8")
    print(archive)
    print(checksum)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Build the immutable consumer lock for one merged release artifact set."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tarfile
from pathlib import Path

from project_metadata import PROJECT_VERSION
from verify_bundle_archive import verify_archive


SUPPORTED_PLATFORMS = (
    "darwin-x64",
    "darwin-arm64",
    "linux-x64",
    "linux-arm64",
    "win32-x64",
    "win32-arm64",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--version", default=PROJECT_VERSION)
    parser.add_argument("--source-commit", default=os.environ.get("GITHUB_SHA", ""))
    parser.add_argument(
        "--source-repository",
        default=os.environ.get("GITHUB_REPOSITORY", "openagent-uno/openagent-host-tools"),
    )
    args = parser.parse_args()
    source_commit = args.source_commit.strip().lower()
    if len(source_commit) != 40 or any(value not in "0123456789abcdef" for value in source_commit):
        raise SystemExit("--source-commit must be the exact 40-character release commit")
    root = args.directory.resolve()
    assets: dict[str, dict[str, str]] = {}
    prefix = "openagent-host-tools-"
    expected_archives = {
        f"{prefix}{platform}.tar.gz": platform for platform in SUPPORTED_PLATFORMS
    }
    discovered_archives = {
        path.name: path for path in root.glob(f"{prefix}*.tar.gz")
    }
    unsupported_archives = sorted(set(discovered_archives) - set(expected_archives))
    missing_archives = sorted(set(expected_archives) - set(discovered_archives))
    legacy_zip_assets = sorted(path.name for path in root.glob(f"{prefix}*.zip"))
    if unsupported_archives or missing_archives or legacy_zip_assets:
        details = []
        if unsupported_archives:
            details.append(f"unsupported archives: {unsupported_archives}")
        if missing_archives:
            details.append(f"missing archives: {missing_archives}")
        if legacy_zip_assets:
            details.append(f"unsupported zip assets: {legacy_zip_assets}")
        raise SystemExit(
            "release artifact set must contain exactly six native bundles; "
            + "; ".join(details)
        )
    for archive_name, platform in expected_archives.items():
        archive = discovered_archives[archive_name]
        try:
            verified = verify_archive(
                archive,
                expected_platform=platform,
                expected_version=args.version,
            )
        except (OSError, RuntimeError, tarfile.TarError) as exc:
            raise SystemExit(f"invalid release archive {archive.name}: {exc}") from exc
        manifest_bytes = verified.manifest_bytes
        assets[platform] = {
            "asset": archive.name,
            "archive_sha256": sha256(archive),
            "bundle_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        }
    wheels = sorted(root.glob("openagent_host_tools-*.whl"))
    if set(assets) != set(SUPPORTED_PLATFORMS) or len(wheels) != 1:
        raise SystemExit("expected exactly six native bundles and exactly one wheel")
    index = {
        "schema": 1,
        "manifest_version": 1,
        "version": args.version,
        "host_tools_version": args.version,
        "bundle_manifest_version": 1,
        "source_repository": args.source_repository,
        "source_ref": f"v{args.version}",
        "source_commit": source_commit,
        "platforms": assets,
        "python_wheel": {"asset": wheels[0].name, "sha256": sha256(wheels[0])},
    }
    (root / "release-index.json").write_text(
        json.dumps(index, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Build one native host-tools bundle and stage its real optional sidecars."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import plistlib
import shutil
import subprocess
import sys
from pathlib import Path

from project_metadata import PROJECT_VERSION

ROOT = Path(__file__).resolve().parents[1]


def platform_key() -> str:
    os_name = {"darwin": "darwin", "linux": "linux", "win32": "win32"}.get(
        sys.platform
    )
    machine = platform.machine().lower()
    arch = "arm64" if machine in {"arm64", "aarch64"} else "x64" if machine in {
        "x86_64",
        "amd64",
    } else None
    if os_name is None or arch is None:
        raise SystemExit(f"unsupported build target: {sys.platform}/{machine}")
    return f"{os_name}-{arch}"


def checked_clean(path: Path, parent: Path) -> None:
    resolved = path.resolve()
    if resolved.parent != parent.resolve():
        raise SystemExit(f"refusing unsafe build cleanup: {resolved}")
    if resolved.exists():
        shutil.rmtree(resolved)


def _build_computer_control(source: Path) -> None:
    cargo = shutil.which("cargo")
    if cargo is None:
        raise SystemExit("Rust/cargo is required to build computer-control")
    subprocess.run(
        [cargo, "build", "--release", "--locked", "--manifest-path", str(source / "Cargo.toml")],
        cwd=ROOT,
        check=True,
    )


def stage_sidecars(bundle: Path, key: str, *, required: bool) -> None:
    """Build and stage the host-owned optional MCP sidecars.

    Their source lives in this repository.  A release therefore cannot
    silently pick up a different openagent-server branch or commit.
    """

    suffix = ".exe" if key.startswith("win32-") else ""
    computer_name = f"openagent-computer-control{suffix}"
    computer_root = ROOT / "sidecars" / "computer-control"
    candidates = [
        computer_root / "bin" / key / computer_name,
        computer_root / "target" / "release" / computer_name,
    ]
    computer = next((path for path in candidates if path.is_file()), None)
    if computer is None and not args_no_build_sidecars():
        _build_computer_control(computer_root)
        computer = computer_root / "target" / "release" / computer_name
        if not computer.is_file():
            computer = None
    if computer is not None:
        if key.startswith("darwin-"):
            app = bundle / "openagent-computer-control.app"
            executable_dir = app / "Contents" / "MacOS"
            executable_dir.mkdir(parents=True)
            target = executable_dir / computer_name
            shutil.copy2(computer, target)
            target.chmod(0o755)
            resources = app / "Contents" / "Resources"
            resources.mkdir()
            with (app / "Contents" / "Info.plist").open("wb") as handle:
                plistlib.dump(
                    {
                        "CFBundleDevelopmentRegion": "en",
                        "CFBundleExecutable": computer_name,
                        "CFBundleIdentifier": "com.openagent.computer-control",
                        "CFBundleInfoDictionaryVersion": "6.0",
                        "CFBundleName": "OpenAgent Computer Control",
                        "CFBundlePackageType": "APPL",
                        "CFBundleShortVersionString": PROJECT_VERSION,
                        "CFBundleVersion": "1",
                        "LSBackgroundOnly": True,
                    },
                    handle,
                )
        else:
            shutil.copy2(computer, bundle / computer_name)
            (bundle / computer_name).chmod(0o755)
    elif required:
        raise SystemExit(f"computer-control binary missing for {key}; checked {candidates}")

    chrome_source = ROOT / "sidecars" / "agent-in-chrome"
    chrome_target = bundle / "agent-in-chrome"
    if chrome_source.is_dir():
        shutil.copytree(
            chrome_source,
            chrome_target,
            ignore=shutil.ignore_patterns("node_modules", ".git", "*.log"),
        )
        npm = shutil.which("npm")
        node = shutil.which("node")
        if npm and node:
            subprocess.run(
                [npm, "ci", "--omit=dev", "--ignore-scripts"],
                cwd=chrome_target / "host",
                check=True,
            )
            # npm's command shims are not used by the sidecar and are symlinks
            # on Unix. Removing them keeps the signed release archive composed
            # only of regular files/directories for fail-closed extraction in
            # Desktop and CLI.
            shutil.rmtree(chrome_target / "host" / "node_modules" / ".bin", ignore_errors=True)
            node_name = "node.exe" if key.startswith("win32-") else "node"
            shutil.copy2(node, bundle / node_name)
            (bundle / node_name).chmod(0o755)
        elif required:
            raise SystemExit("Node.js/npm are required to stage agent-in-chrome")
    elif required:
        raise SystemExit(f"agent-in-chrome source missing at {chrome_source}")


def args_no_build_sidecars() -> bool:
    """Test seam for source bundles that intentionally pre-stage binaries."""

    return os.environ.get("OPENAGENT_HOST_TOOLS_SKIP_SIDECAR_BUILD") == "1"


def write_bundle_manifest(bundle: Path, key: str) -> None:
    files = {}
    for path in sorted(item for item in bundle.rglob("*") if item.is_file()):
        relative = path.relative_to(bundle).as_posix()
        if relative == "bundle-manifest.json":
            continue
        files[relative] = {
            "size": path.stat().st_size,
            "sha256": file_sha256(path),
        }
    (bundle / "bundle-manifest.json").write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "version": PROJECT_VERSION,
                "platform": key,
                "files": files,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-sidecars", action="store_true")
    parser.add_argument("--require-sidecars", action="store_true")
    parser.add_argument("--expect-platform")
    parser.add_argument("--manifest-only", action="store_true")
    args = parser.parse_args()

    key = platform_key()
    if args.expect_platform and args.expect_platform != key:
        raise SystemExit(
            f"runner architecture mismatch: expected {args.expect_platform}, got {key}"
        )
    if args.manifest_only:
        bundle = ROOT / "dist" / key
        if not bundle.is_dir():
            raise SystemExit(f"bundle does not exist: {bundle}")
        write_bundle_manifest(bundle, key)
        print(bundle)
        return
    build_parent = ROOT / "build"
    build_parent.mkdir(exist_ok=True)
    build = build_parent / f"standalone-{key}"
    checked_clean(build, build_parent)
    build.mkdir()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "PyInstaller",
            "--noconfirm",
            "--clean",
            "--distpath",
            str(build / "dist"),
            "--workpath",
            str(build / "work"),
            str(ROOT / "openagent-host-tools.spec"),
        ],
        cwd=ROOT,
        check=True,
    )
    output_parent = ROOT / "dist"
    output_parent.mkdir(exist_ok=True)
    bundle = output_parent / key
    checked_clean(bundle, output_parent)
    bundle.mkdir()
    suffix = ".exe" if key.startswith("win32-") else ""
    executable = build / "dist" / f"openagent-host-tools{suffix}"
    shutil.copy2(executable, bundle / executable.name)
    (bundle / executable.name).chmod(0o755)

    if not args.no_sidecars:
        stage_sidecars(bundle, key, required=args.require_sidecars)
    shutil.copy2(ROOT / "THIRD_PARTY_NOTICES.md", bundle / "THIRD_PARTY_NOTICES.md")
    write_bundle_manifest(bundle, key)
    print(bundle)


if __name__ == "__main__":
    main()

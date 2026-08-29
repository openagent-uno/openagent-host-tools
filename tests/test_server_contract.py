from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

from openagent_host_tools import CapabilityHost, HostPaths, __version__
from openagent_host_tools.sidecars import (
    COMPUTER_CONTROL_MANIFEST,
    _VERIFIED_BUNDLES,
    _discover,
)


@pytest.mark.asyncio
async def test_manifest_lock_covers_all_builtins(tmp_path: Path):
    host = CapabilityHost(paths=HostPaths.discover(tmp_path / "user"), cwd=tmp_path)
    try:
        status = await host.status()
        lock = status["manifest_lock"]
        assert lock["lock_version"] == 1
        assert set(lock["servers"]) >= {
            "filesystem",
            "editor",
            "shell",
            "computer-control",
            "agent-in-chrome",
        }
        for value in lock["servers"].values():
            assert value["version"]
            assert re.fullmatch(r"[0-9a-f]{64}", value["sha256"])
        packaged = json.loads(
            (Path(__file__).resolve().parents[1] / "src/openagent_host_tools/builtins-lock.json").read_text()
        )
        assert lock == packaged
    finally:
        await host.close()


def test_existing_server_core_tool_names_match_host_contract():
    """Guard the current server wrappers until they consume this package directly."""
    repo = Path(__file__).resolve().parents[2]
    server = repo / "openagent-server"
    if not server.exists():
        pytest.skip("openagent-server sibling checkout unavailable")

    editor_source = (server / "src/mcp/servers/editor/src/index.ts").read_text()
    editor_names = set(re.findall(r"server\.registerTool\(\s*['\"]([^'\"]+)", editor_source))
    assert editor_names == {"edit", "grep", "glob"}

    shell_source = (server / "src/mcp/servers/shell/adapters.py").read_text()
    shell_names = set(re.findall(r"async def (shell_[a-z_]+)\(", shell_source))
    assert shell_names >= {
        "shell_exec",
        "shell_output",
        "shell_input",
        "shell_kill",
        "shell_list",
        "shell_which",
    }

    builtins_source = (server / "src/mcp/builtins.py").read_text()
    for filesystem_name in ("read_text_file", "write_file", "list_directory"):
        assert filesystem_name in builtins_source


def test_frozen_darwin_nested_app_uses_bundle_root_for_integrity(
    tmp_path: Path, monkeypatch
):
    executable = "openagent-computer-control"
    helper = (
        tmp_path
        / f"{executable}.app"
        / "Contents"
        / "MacOS"
        / executable
    )
    helper.parent.mkdir(parents=True)
    helper.write_bytes(b"signed-helper")
    helper.chmod(0o755)
    relative = helper.relative_to(tmp_path).as_posix()
    (tmp_path / "bundle-manifest.json").write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "version": __version__,
                "platform": "darwin-arm64",
                "files": {
                    relative: {
                        "size": helper.stat().st_size,
                        "sha256": hashlib.sha256(helper.read_bytes()).hexdigest(),
                    }
                },
            }
        )
    )
    monkeypatch.setenv("OPENAGENT_HOST_TOOLS_SIDECAR_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    _VERIFIED_BUNDLES.clear()

    found = _discover(
        "computer-control",
        "OPENAGENT_COMPUTER_CONTROL_COMMAND",
        executable,
        COMPUTER_CONTROL_MANIFEST,
    )
    assert found.command == (str(helper),)

    (tmp_path / "unexpected").write_text("not declared", encoding="utf-8")
    rejected_extra = _discover(
        "computer-control",
        "OPENAGENT_COMPUTER_CONTROL_COMMAND",
        executable,
        COMPUTER_CONTROL_MANIFEST,
    )
    assert rejected_extra.command is None
    assert "file set mismatch" in (rejected_extra.reason or "")
    (tmp_path / "unexpected").unlink()

    helper.write_bytes(b"tampered")
    helper.chmod(0o755)
    rejected = _discover(
        "computer-control",
        "OPENAGENT_COMPUTER_CONTROL_COMMAND",
        executable,
        COMPUTER_CONTROL_MANIFEST,
    )
    assert rejected.command is None
    assert "integrity check failed" in (rejected.reason or "")


def test_source_artifacts_exclude_generated_sidecar_trees(tmp_path: Path):
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is required for packaging regression")
    root = Path(__file__).resolve().parents[1]
    subprocess.run(
        [uv, "build", "--wheel", "--sdist", "--out-dir", str(tmp_path)],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    archives = list(tmp_path.glob("openagent_host_tools-*"))
    assert len(archives) == 2
    for archive in archives:
        if archive.suffix == ".whl":
            with zipfile.ZipFile(archive) as value:
                members = value.namelist()
            assert "openagent_host_tools/THIRD_PARTY_NOTICES.md" in members
        else:
            with tarfile.open(archive, "r:*") as value:
                members = value.getnames()
        forbidden = [
            member
            for member in members
            if any(
                marker in member
                for marker in ("node_modules/", "/target/", "/bin/", "/dist/")
            )
        ]
        assert forbidden == []


def test_release_archive_contains_only_regular_files_and_directories(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bundle = tmp_path / "dist" / "linux-x64"
    bundle.mkdir(parents=True)
    executable = bundle / "openagent-host-tools"
    executable.write_bytes(b"host")
    executable.chmod(0o755)
    digest = hashlib.sha256(executable.read_bytes()).hexdigest()
    (bundle / "bundle-manifest.json").write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "version": __version__,
                "platform": "linux-x64",
                "files": {
                    "openagent-host-tools": {
                        "size": executable.stat().st_size,
                        "sha256": digest,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    subprocess.run(
        [sys.executable, str(root / "scripts/package_bundle.py"), str(bundle)],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    archive = bundle.parent / "openagent-host-tools-linux-x64.tar.gz"
    stale_archive = bundle.parent / f"openagent-host-tools-{__version__}-linux-x64.tar.gz"
    stale_checksum = Path(str(stale_archive) + ".sha256")
    stale_archive.write_bytes(b"stale")
    stale_checksum.write_text("stale\n", encoding="utf-8")
    subprocess.run(
        [sys.executable, str(root / "scripts/package_bundle.py"), str(bundle)],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    assert not stale_archive.exists()
    assert not stale_checksum.exists()
    with tarfile.open(archive, "r:gz") as packaged:
        members = packaged.getmembers()
    assert members
    assert all(member.isfile() or member.isdir() for member in members)
    for platform in (
        "darwin-x64",
        "darwin-arm64",
        "linux-arm64",
        "win32-x64",
        "win32-arm64",
    ):
        platform_bundle = bundle.parent / platform
        platform_bundle.mkdir()
        platform_file = platform_bundle / "openagent-host-tools"
        platform_file.write_bytes(platform.encode("utf-8"))
        platform_manifest = {
            "manifest_version": 1,
            "version": __version__,
            "platform": platform,
            "files": {
                platform_file.name: {
                    "size": platform_file.stat().st_size,
                    "sha256": hashlib.sha256(platform_file.read_bytes()).hexdigest(),
                }
            },
        }
        (platform_bundle / "bundle-manifest.json").write_text(
            json.dumps(platform_manifest), encoding="utf-8"
        )
        subprocess.run(
            [
                sys.executable,
                str(root / "scripts/package_bundle.py"),
                str(platform_bundle),
            ],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    wheel = bundle.parent / f"openagent_host_tools-{__version__}-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    subprocess.run(
        [
            sys.executable,
            str(root / "scripts/release_index.py"),
            str(bundle.parent),
            "--source-commit",
            "a" * 40,
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    lock = json.loads((bundle.parent / "release-index.json").read_text(encoding="utf-8"))
    assert lock["schema"] == 1
    assert lock["version"] == __version__
    assert lock["source_ref"] == f"v{__version__}"
    assert lock["source_commit"] == "a" * 40
    assert lock["platforms"]["linux-x64"]["asset"] == archive.name
    assert len(lock["platforms"]["linux-x64"]["archive_sha256"]) == 64
    assert lock["python_wheel"]["asset"] == wheel.name

    stale_archive.write_bytes(b"stale")
    rejected_stale_set = subprocess.run(
        [
            sys.executable,
            str(root / "scripts/release_index.py"),
            str(bundle.parent),
            "--source-commit",
            "a" * 40,
        ],
        cwd=root,
        capture_output=True,
        text=True,
    )
    assert rejected_stale_set.returncode != 0
    assert "unsupported archives" in rejected_stale_set.stderr
    stale_archive.unlink()

    link = bundle / "unsupported-link"
    try:
        link.symlink_to(executable.name)
    except OSError:
        pytest.skip("this runner cannot create symbolic links")
    rejected = subprocess.run(
        [sys.executable, str(root / "scripts/package_bundle.py"), str(bundle)],
        cwd=root,
        capture_output=True,
        text=True,
    )
    assert rejected.returncode != 0
    assert "unsupported symbolic links" in rejected.stderr

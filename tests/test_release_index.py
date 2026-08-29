from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from openagent_host_tools import __version__


ROOT = Path(__file__).resolve().parents[1]
RELEASE_INDEX = ROOT / "scripts" / "release_index.py"
PLATFORMS = (
    "darwin-x64",
    "darwin-arm64",
    "linux-x64",
    "linux-arm64",
    "win32-x64",
    "win32-arm64",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_archive(root: Path, platform: str, defect: str | None = None) -> Path:
    build = root / "build" / platform
    bundle = build / platform
    bundle.mkdir(parents=True)
    executable_name = (
        "openagent-host-tools.exe"
        if platform.startswith("win32-")
        else "openagent-host-tools"
    )
    executable = bundle / executable_name
    content = f"host:{platform}".encode()
    executable.write_bytes(content)
    executable.chmod(0o755)
    declared_content = b"x" * len(content) if defect == "content-mismatch" else content
    manifest = {
        "manifest_version": 1,
        "version": __version__,
        "platform": platform,
        "files": {
            executable_name: {
                "size": len(content),
                "sha256": hashlib.sha256(declared_content).hexdigest(),
            }
        },
    }
    manifest_bytes = json.dumps(manifest, sort_keys=True).encode()
    if defect == "noncanonical-manifest":
        manifest_path = bundle / "nested" / "bundle-manifest.json"
        manifest_path.parent.mkdir()
    else:
        manifest_path = bundle / "bundle-manifest.json"
    manifest_path.write_bytes(manifest_bytes)
    if defect == "duplicate-manifest":
        duplicate = bundle / "nested" / "bundle-manifest.json"
        duplicate.parent.mkdir()
        duplicate.write_bytes(manifest_bytes)

    archive = root / f"openagent-host-tools-{platform}.tar.gz"
    with tarfile.open(archive, "w:gz") as package:
        package.add(bundle, arcname=platform)
    Path(f"{archive}.sha256").write_text(
        f"{_sha256(archive)}  {archive.name}\n", encoding="utf-8"
    )
    return archive


def _release_set(root: Path, *, defect: str | None = None) -> None:
    for platform in PLATFORMS:
        _write_archive(
            root,
            platform,
            defect if platform == "linux-x64" else None,
        )
    (root / f"openagent_host_tools-{__version__}-py3-none-any.whl").write_bytes(
        b"wheel"
    )


def _run_release_index(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(RELEASE_INDEX),
            str(root),
            "--source-commit",
            "a" * 40,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_release_index_verifies_complete_archives_without_extracting(tmp_path: Path):
    _release_set(tmp_path)
    before = {path.relative_to(tmp_path) for path in tmp_path.rglob("*")}

    result = _run_release_index(tmp_path)

    assert result.returncode == 0, result.stderr
    after = {
        path.relative_to(tmp_path)
        for path in tmp_path.rglob("*")
        if path.name != "release-index.json"
    }
    assert after == before
    index = json.loads((tmp_path / "release-index.json").read_text(encoding="utf-8"))
    assert set(index["platforms"]) == set(PLATFORMS)
    for platform, locked in index["platforms"].items():
        archive = tmp_path / locked["asset"]
        assert locked["archive_sha256"] == _sha256(archive)
        assert len(locked["bundle_manifest_sha256"]) == 64


@pytest.mark.parametrize(
    ("defect", "message"),
    (
        ("noncanonical-manifest", "exactly one regular bundle manifest"),
        ("duplicate-manifest", "exactly one regular bundle manifest"),
        ("content-mismatch", "archive content does not match manifest"),
    ),
)
def test_release_index_rejects_untrusted_archive_shapes(
    tmp_path: Path, defect: str, message: str
):
    _release_set(tmp_path, defect=defect)

    result = _run_release_index(tmp_path)

    assert result.returncode != 0
    assert message in result.stderr
    assert not (tmp_path / "release-index.json").exists()

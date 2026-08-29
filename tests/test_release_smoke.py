from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import sys
import tarfile
from pathlib import Path

import pytest
from mcp import types as mcp_types

from openagent_host_tools import __version__

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


smoke_bundle = _load_script("smoke_bundle")
verify_bundle_archive = _load_script("verify_bundle_archive")


def test_computer_control_smoke_validates_cursor_png_and_tcc_errors(tmp_path: Path):
    cursor = mcp_types.CallToolResult(
        content=[mcp_types.TextContent(type="text", text='{"x":12,"y":34}')]
    )
    assert smoke_bundle._validate_cursor_result(cursor) == (12, 34)
    smoke_bundle._require_cursor_near((63, 65), (64, 64))
    with pytest.raises(RuntimeError, match="mouse_move was not reflected"):
        smoke_bundle._require_cursor_near((60, 64), (64, 64))

    png = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
        "+A8AAQUBAScY42YAAAAASUVORK5CYII="
    )
    screenshot = mcp_types.CallToolResult(
        content=[
            mcp_types.ImageContent(
                type="image",
                data=base64.b64encode(png).decode(),
                mimeType="image/png",
            )
        ]
    )
    smoke_bundle._validate_screenshot_result(screenshot)

    accessibility = mcp_types.CallToolResult(
        isError=True,
        content=[
            mcp_types.TextContent(
                type="text", text="macOS Accessibility permission required."
            )
        ],
    )
    screen_recording = mcp_types.CallToolResult(
        isError=True,
        content=[
            mcp_types.TextContent(
                type="text", text="macOS Screen Recording permission required."
            )
        ],
    )
    smoke_bundle._require_permission_error(accessibility, "Accessibility")
    smoke_bundle._require_permission_error(screen_recording, "Screen Recording")

    invalid_image = mcp_types.CallToolResult(
        content=[
            mcp_types.ImageContent(
                type="image",
                data=base64.b64encode(b"not-png").decode(),
                mimeType="image/png",
            )
        ]
    )
    with pytest.raises(RuntimeError, match="invalid PNG"):
        smoke_bundle._validate_screenshot_result(invalid_image)
    with pytest.raises(SystemExit):
        smoke_bundle._parse_args([str(tmp_path)])
    assert (
        smoke_bundle._parse_args(
            [str(tmp_path), "--computer-control", "expect-granted"]
        ).computer_control
        == "expect-granted"
    )


def _release_archive(tmp_path: Path, *, extra_file: bool = False) -> Path:
    platform = "darwin-arm64"
    bundle = tmp_path / platform
    helper = (
        bundle
        / "openagent-computer-control.app"
        / "Contents"
        / "MacOS"
        / "openagent-computer-control"
    )
    helper.parent.mkdir(parents=True)
    (bundle / "openagent-computer-control.app" / "Contents" / "Resources").mkdir()
    files = {
        "openagent-host-tools": b"host",
        "node": b"node",
        helper.relative_to(bundle).as_posix(): b"helper",
    }
    for relative, content in files.items():
        path = bundle / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        path.chmod(0o755)
    manifest = {
        "manifest_version": 1,
        "version": __version__,
        "platform": platform,
        "files": {
            relative: {
                "size": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
            for relative, content in files.items()
        },
    }
    (bundle / "bundle-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    if extra_file:
        (bundle / "undeclared").write_text("extra", encoding="utf-8")

    archive = tmp_path / f"openagent-host-tools-{platform}.tar.gz"
    with tarfile.open(archive, "w:gz") as package:
        package.add(bundle, arcname=platform)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    Path(f"{archive}.sha256").write_text(f"{digest}  {archive.name}\n", encoding="utf-8")
    return archive


def test_release_archive_is_verified_before_and_after_safe_extraction(tmp_path: Path):
    archive = _release_archive(tmp_path)
    destination = tmp_path / "extracted"
    bundle = verify_bundle_archive.verify_and_extract(
        archive, destination, expected_platform="darwin-arm64"
    )
    assert (bundle / "openagent-host-tools").read_bytes() == b"host"
    assert (
        bundle / "openagent-computer-control.app" / "Contents" / "Resources"
    ).is_dir()


def test_release_archive_rejects_files_absent_from_manifest(tmp_path: Path):
    archive = _release_archive(tmp_path, extra_file=True)
    with pytest.raises(RuntimeError, match="file set does not match manifest"):
        verify_bundle_archive.verify_and_extract(archive, tmp_path / "extracted")

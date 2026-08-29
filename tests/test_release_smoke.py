from __future__ import annotations

import base64
import asyncio
import hashlib
import importlib.util
import inspect
import json
import os
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


def test_linux_xvfb_smoke_prepares_a_real_input_connection():
    script = SCRIPTS / "smoke_linux_xvfb.sh"
    text = script.read_text(encoding="utf-8")
    assert os.access(script, os.X_OK)
    assert "xdpyinfo" in text
    assert "xmodmap -e 'keycode 255 ='" in text
    assert "--computer-control expect-granted" in text
    assert "--core-only" not in text
    assert "env=dict(os.environ)" in inspect.getsource(smoke_bundle._computer_control)


def test_linux_and_windows_release_smokes_exercise_real_chrome_and_desktop():
    chrome_source = inspect.getsource(smoke_bundle._chrome)
    for required in (
        "session.list_tools()",
        '"tabs_context_mcp"',
        '"navigate"',
        '"javascript_tool"',
        '"get_page_text"',
        "CHROME_SMOKE_MUTATED_MARKER",
    ):
        assert required in chrome_source

    workflows = {
        "test-build.yml": (
            "python scripts/smoke_bundle.py dist/* --computer-control expect-granted"
        ),
        "release.yml": (
            "python scripts/smoke_bundle.py dist/${{ matrix.platform }} "
            "--computer-control expect-granted"
        ),
    }
    for name, windows_command in workflows.items():
        workflow = (ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")
        assert "computer-control, and Chrome under Xvfb" in workflow
        assert "computer-control, and Chrome on Windows desktop" in workflow
        assert windows_command in workflow
        assert "--core-only --computer-control skip" not in workflow


def test_frozen_core_smoke_consumes_shell_completion_notifications():
    source = inspect.getsource(smoke_bundle._core)
    assert "logging_callback=logging_callback" in source
    assert '"run_in_background": True' in source
    assert "SHELL_COMPLETION_CAPABILITY" in source
    assert 'event.get("type") != "shell_completed"' in source


def test_chrome_smoke_tab_context_requires_a_real_integer_tab_id():
    valid = mcp_types.CallToolResult(
        content=[
            mcp_types.TextContent(
                type="text",
                text=(
                    '{"availableTabs":[{"tabId":7,"title":"Smoke",'
                    '"url":"about:blank"}]}\n\nTab Context:'
                ),
            )
        ]
    )
    assert smoke_bundle._tab_id_from_context(valid) == 7
    invalid = mcp_types.CallToolResult(
        content=[mcp_types.TextContent(type="text", text='{"availableTabs":[{"tabId":"7"}]}')]
    )
    with pytest.raises(RuntimeError, match="non-integer tab id"):
        smoke_bundle._tab_id_from_context(invalid)


def test_macos_launchservices_stdio_uses_real_fifo_paths():
    relay = SCRIPTS / "launch_macos_app_stdio.sh"
    text = relay.read_text(encoding="utf-8")
    assert os.access(relay, os.X_OK)
    assert 'mkfifo "$to_app" "$from_app"' in text
    assert '--stdin "$to_app"' in text
    assert '--stdout "$from_app"' in text
    assert "/dev/stdin" not in text
    assert "/dev/stdout" not in text
    source = inspect.getsource(smoke_bundle._computer_control)
    assert 'with_name("launch_macos_app_stdio.sh")' in source


def test_macos_capture_preflights_screen_recording_and_fails_closed():
    source = (ROOT / "sidecars" / "computer-control" / "src" / "capture.rs").read_text(
        encoding="utf-8"
    )
    assert "CGPreflightScreenCaptureAccess" in source
    assert "CGRequestScreenCaptureAccess" in source
    assert source.count("require_screen_recording_permission()?") == 2
    assert "Err(anyhow!(MAC_SCREEN_RECORDING_HINT))" in source


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
            mcp_types.TextContent(type="text", text="macOS Accessibility permission required.")
        ],
    )
    screen_recording = mcp_types.CallToolResult(
        isError=True,
        content=[
            mcp_types.TextContent(type="text", text="macOS Screen Recording permission required.")
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
    launchservices = smoke_bundle._parse_args(
        [
            str(tmp_path),
            "--computer-control",
            "expect-denied",
            "--macos-launchservices",
        ]
    )
    assert launchservices.macos_launchservices is True
    source = inspect.getsource(smoke_bundle._computer_control)
    assert source.index('"start_screen_recording"') < source.index('"get_cursor_position"')
    assert 'with_name("launch_macos_app_stdio.sh")' in source
    if sys.platform != "darwin":
        with pytest.raises(RuntimeError, match="requires macOS and expect-denied"):
            asyncio.run(
                smoke_bundle._computer_control(
                    tmp_path,
                    "skip",
                    macos_launchservices=True,
                )
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
    assert (bundle / "openagent-computer-control.app" / "Contents" / "Resources").is_dir()


def test_release_archive_rejects_files_absent_from_manifest(tmp_path: Path):
    archive = _release_archive(tmp_path, extra_file=True)
    with pytest.raises(RuntimeError, match="file set does not match manifest"):
        verify_bundle_archive.verify_and_extract(archive, tmp_path / "extracted")

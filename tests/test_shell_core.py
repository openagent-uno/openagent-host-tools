from __future__ import annotations

import pytest

from openagent_host_tools import shell_core
from openagent_host_tools.shell_core import BackgroundShell


@pytest.mark.asyncio
async def test_windows_shell_command_is_not_requoted_as_an_argv_element(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = '\"C:\\Program Files\\Python\\python.exe\" -c \"print(\'quoted value\')\"'
    sentinel = object()
    captured: dict[str, object] = {}

    async def fake_create_subprocess_shell(value: str, **kwargs):
        captured["command"] = value
        captured.update(kwargs)
        return sentinel

    async def reject_create_subprocess_exec(*_args, **_kwargs):
        raise AssertionError("Windows commands must not pass through argv quoting")

    monkeypatch.setattr(shell_core.platform, "system", lambda: "Windows")
    monkeypatch.setattr(
        shell_core.asyncio,
        "create_subprocess_shell",
        fake_create_subprocess_shell,
    )
    monkeypatch.setattr(
        shell_core.asyncio,
        "create_subprocess_exec",
        reject_create_subprocess_exec,
    )

    shell = BackgroundShell(
        shell_id="windows-quoted",
        command=command,
        cwd="C:\\work tree",
        env={"OPENAGENT_TEST": "1"},
    )
    process = await shell._spawn_process()

    assert process is sentinel
    assert captured["command"] == command
    assert "executable" not in captured
    assert captured["cwd"] == "C:\\work tree"
    assert captured["env"]["OPENAGENT_TEST"] == "1"
    assert "start_new_session" not in captured


@pytest.mark.asyncio
async def test_posix_shell_keeps_exec_argv_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = "printf '%s\\n' 'quoted value'"
    sentinel = object()
    captured: dict[str, object] = {}

    async def fake_create_subprocess_exec(*argv, **kwargs):
        captured["argv"] = argv
        captured.update(kwargs)
        return sentinel

    async def reject_create_subprocess_shell(*_args, **_kwargs):
        raise AssertionError("POSIX keeps the explicit shell argv contract")

    monkeypatch.setattr(shell_core.platform, "system", lambda: "Linux")
    monkeypatch.setenv("SHELL", "/bin/test-shell")
    monkeypatch.setattr(
        shell_core.asyncio,
        "create_subprocess_exec",
        fake_create_subprocess_exec,
    )
    monkeypatch.setattr(
        shell_core.asyncio,
        "create_subprocess_shell",
        reject_create_subprocess_shell,
    )

    shell = BackgroundShell(
        shell_id="posix-quoted",
        command=command,
        cwd="/work tree",
        env=None,
    )
    process = await shell._spawn_process()

    assert process is sentinel
    assert captured["argv"] == ("/bin/test-shell", "-c", command)
    assert captured["cwd"] == "/work tree"
    assert captured["start_new_session"] is True

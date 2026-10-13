from __future__ import annotations

import io
import json
import sys

import pytest

from openagent_host_tools.stdio import _write_stdout


@pytest.mark.parametrize("encoding", ["cp1252", "ascii", "utf-8"])
def test_ndjson_output_is_utf8_bytes_independent_of_text_wrapper(monkeypatch, encoding):
    output = io.BytesIO()
    wrapper = io.TextIOWrapper(output, encoding=encoding, errors="strict", newline="\r\n")
    value = {"id": "caffè—東京🙂", "result": {"path": "dati/日本語.txt"}}
    data = json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"
    try:
        monkeypatch.setattr(sys, "stdout", wrapper)
        _write_stdout(data)
        wire = output.getvalue()
        assert wire == data.encode("utf-8")
        assert wire.endswith(b"\n") and not wire.endswith(b"\r\n")
        assert json.loads(wire.decode("utf-8", errors="strict")) == value
    finally:
        wrapper.detach()

"""Paths to the versioned sidecar sources owned by this package."""

from __future__ import annotations

from pathlib import Path


def sidecar_source(name: str) -> Path:
    packaged = Path(__file__).resolve().parent / "sidecars" / name
    if packaged.is_dir():
        return packaged
    # Editable checkout: ``src/openagent_host_tools`` and repo ``sidecars``.
    checkout = Path(__file__).resolve().parents[2] / "sidecars" / name
    if checkout.is_dir():
        return checkout
    raise FileNotFoundError(f"host-tools sidecar source is unavailable: {name}")

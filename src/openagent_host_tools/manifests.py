"""Stable checksums for server/client built-in manifest parity."""

from __future__ import annotations

import hashlib
import json
from typing import Iterable

from .types import ServerManifest

MANIFEST_LOCK_VERSION = 1


def contract_payload(manifest: ServerManifest) -> dict:
    """Return the location-independent portion covered by the manifest lock."""
    payload = manifest.to_wire()
    payload.pop("available", None)
    payload.pop("unavailable_reason", None)
    payload.pop("data_directory", None)
    return payload


def manifest_sha256(manifest: ServerManifest) -> str:
    canonical = json.dumps(
        contract_payload(manifest),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def build_manifest_lock(manifests: Iterable[ServerManifest]) -> dict:
    return {
        "lock_version": MANIFEST_LOCK_VERSION,
        "servers": {
            manifest.name: {
                "version": manifest.version,
                "sha256": manifest_sha256(manifest),
            }
            for manifest in sorted(manifests, key=lambda item: item.name)
        },
    }

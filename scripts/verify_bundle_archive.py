#!/usr/bin/env python3
"""Fail-closed extraction and manifest verification for a release bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import tarfile
from pathlib import Path, PurePosixPath
from typing import Any, NamedTuple

from project_metadata import PROJECT_VERSION

ARCHIVE_RE = re.compile(
    r"^openagent-host-tools-((?:darwin|linux|win32)-(?:arm64|x64))\.tar\.gz$"
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class VerifiedBundleArchive(NamedTuple):
    """Identity and manifest bytes from a completely verified release archive."""

    platform: str
    manifest_bytes: bytes
    manifest: dict[str, Any]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_stream(handle: object) -> str:
    digest = hashlib.sha256()
    while chunk := handle.read(1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def _safe_relative(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or "\\" in value
        or path.is_absolute()
        or value != path.as_posix()
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise RuntimeError(f"unsafe archive path: {value!r}")
    return path


def _verify_detached_checksum(archive: Path) -> None:
    checksum = Path(f"{archive}.sha256")
    try:
        fields = checksum.read_text(encoding="utf-8").strip().split()
    except OSError as exc:
        raise RuntimeError(f"cannot read detached checksum {checksum}: {exc}") from exc
    if len(fields) != 2 or not SHA256_RE.fullmatch(fields[0]):
        raise RuntimeError(f"invalid detached checksum format: {checksum}")
    if fields[1] != archive.name:
        raise RuntimeError(
            f"detached checksum names {fields[1]!r}, expected {archive.name!r}"
        )
    actual = _sha256_file(archive)
    if actual != fields[0]:
        raise RuntimeError(
            f"archive checksum mismatch: expected {fields[0]}, got {actual}"
        )


def _prepare_archive(
    archive: Path, expected_platform: str | None
) -> tuple[Path, str]:
    archive = archive.resolve()
    match = ARCHIVE_RE.fullmatch(archive.name)
    if match is None:
        raise RuntimeError(f"not a canonical host-tools asset: {archive.name}")
    platform = match.group(1)
    if expected_platform is not None and expected_platform != platform:
        raise RuntimeError(
            f"archive platform is {platform}, expected {expected_platform}"
        )
    if not archive.is_file():
        raise RuntimeError(f"archive does not exist: {archive}")
    _verify_detached_checksum(archive)
    return archive, platform


def _verify_open_archive(
    package: tarfile.TarFile,
    *,
    platform: str,
    expected_version: str,
) -> tuple[VerifiedBundleArchive, list[tarfile.TarInfo]]:
    """Verify every member and byte while ``package`` remains open."""

    members = package.getmembers()
    if not members:
        raise RuntimeError("release archive is empty")

    by_name: dict[str, tarfile.TarInfo] = {}
    manifest_members: list[tarfile.TarInfo] = []
    for member in members:
        relative = _safe_relative(member.name)
        if relative.parts[0] != platform:
            raise RuntimeError(
                f"archive member is outside the {platform} bundle: {member.name}"
            )
        if not (member.isfile() or member.isdir()):
            raise RuntimeError(
                f"archive contains a link or special entry: {member.name}"
            )
        if member.name in by_name:
            raise RuntimeError(f"archive contains duplicate entry: {member.name}")
        by_name[member.name] = member
        if relative.name == "bundle-manifest.json":
            manifest_members.append(member)

    manifest_name = f"{platform}/bundle-manifest.json"
    if (
        len(manifest_members) != 1
        or manifest_members[0].name != manifest_name
        or not manifest_members[0].isfile()
    ):
        found = sorted(member.name for member in manifest_members)
        raise RuntimeError(
            "release archive must contain exactly one regular bundle manifest "
            f"at {manifest_name}; found={found}"
        )
    manifest_member = manifest_members[0]
    manifest_handle = package.extractfile(manifest_member)
    if manifest_handle is None:
        raise RuntimeError("cannot read bundle-manifest.json from release archive")
    manifest_bytes = manifest_handle.read()
    try:
        manifest = json.loads(manifest_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid bundle-manifest.json: {exc}") from exc
    if (
        not isinstance(manifest, dict)
        or manifest.get("manifest_version") != 1
        or manifest.get("version") != expected_version
        or manifest.get("platform") != platform
    ):
        raise RuntimeError("bundle manifest identity does not match the release asset")
    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        raise RuntimeError("bundle manifest has no checksum map")

    expected_archive_files = {manifest_name}
    for relative_value, metadata in files.items():
        if not isinstance(relative_value, str):
            raise RuntimeError("bundle manifest contains a non-string file path")
        relative = _safe_relative(relative_value)
        if relative.name == "bundle-manifest.json":
            raise RuntimeError(
                "bundle manifest file-map may not contain another bundle manifest"
            )
        member_name = f"{platform}/{relative.as_posix()}"
        expected_archive_files.add(member_name)
        member = by_name.get(member_name)
        if member is None or not member.isfile():
            raise RuntimeError(f"manifest file is absent from archive: {relative}")
        if (
            not isinstance(metadata, dict)
            or isinstance(metadata.get("size"), bool)
            or not isinstance(metadata.get("size"), int)
            or metadata["size"] < 0
            or not isinstance(metadata.get("sha256"), str)
            or not SHA256_RE.fullmatch(metadata["sha256"])
            or member.size != metadata["size"]
        ):
            raise RuntimeError(f"invalid manifest metadata for {relative}")
        source = package.extractfile(member)
        if source is None or _sha256_stream(source) != metadata["sha256"]:
            raise RuntimeError(f"archive content does not match manifest: {relative}")

    actual_archive_files = {member.name for member in members if member.isfile()}
    if actual_archive_files != expected_archive_files:
        unexpected = sorted(actual_archive_files - expected_archive_files)
        missing = sorted(expected_archive_files - actual_archive_files)
        raise RuntimeError(
            "archive file set does not match manifest; "
            f"extra={unexpected}, missing={missing}"
        )
    return (
        VerifiedBundleArchive(
            platform=platform,
            manifest_bytes=manifest_bytes,
            manifest=manifest,
        ),
        members,
    )


def verify_archive(
    archive: Path,
    *,
    expected_platform: str | None = None,
    expected_version: str = PROJECT_VERSION,
) -> VerifiedBundleArchive:
    """Verify a release archive completely without extracting any member."""

    archive, platform = _prepare_archive(archive, expected_platform)
    with tarfile.open(archive, "r:gz") as package:
        verified, _members = _verify_open_archive(
            package,
            platform=platform,
            expected_version=expected_version,
        )
    return verified


def verify_and_extract(
    archive: Path,
    destination: Path,
    *,
    expected_platform: str | None = None,
    expected_version: str = PROJECT_VERSION,
) -> Path:
    archive, platform = _prepare_archive(archive, expected_platform)

    destination = destination.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        raise RuntimeError(f"extraction destination is not empty: {destination}")

    with tarfile.open(archive, "r:gz") as package:
        verified, members = _verify_open_archive(
            package,
            platform=platform,
            expected_version=expected_version,
        )
        # Extract only entries already proven to be regular files/directories.
        for member in members:
            target = destination.joinpath(*PurePosixPath(member.name).parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                target.chmod(member.mode & 0o777)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            source = package.extractfile(member)
            if source is None:
                raise RuntimeError(f"cannot extract regular file: {member.name}")
            with target.open("wb") as output:
                shutil.copyfileobj(source, output)
            target.chmod(member.mode & 0o777)

    bundle = destination / platform
    files = verified.manifest["files"]
    actual_files = {
        path.relative_to(bundle).as_posix()
        for path in bundle.rglob("*")
        if path.is_file() and path != bundle / "bundle-manifest.json"
    }
    if actual_files != set(files):
        raise RuntimeError("extracted bundle file set does not match its manifest")
    for relative, metadata in files.items():
        path = bundle.joinpath(*PurePosixPath(relative).parts)
        if (
            path.stat().st_size != metadata["size"]
            or _sha256_file(path) != metadata["sha256"]
        ):
            raise RuntimeError(f"extracted file does not match manifest: {relative}")
    return bundle


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--expect-platform")
    args = parser.parse_args()
    bundle = verify_and_extract(
        args.archive, args.destination, expected_platform=args.expect_platform
    )
    print(bundle)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Assemble an immutable 0.1.3 draft; this script never publishes a release."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import zipfile
from pathlib import Path

from verify_bundle_archive import verify_archive

REPO = "openagent-uno/openagent-host-tools"
SOURCE = "615351f9ea11ac513f6ec29d6ee632e5cb064dc3"
RUN = 38431981858
TAG = "v0.1.3"
WHEEL = "openagent_host_tools-0.1.3-py3-none-any.whl"
INPUTS = {
    "linux-x64": (11831781287, "bbbee63b627a1511ed78627dee46e991951880223e0ef8ff113c3ea0d73cf978"),
    "linux-arm64": (11830972715, "38deb83a75696c4bdeb478cb40e1b823538ea8bfeac3ca60f4a513c464c80d9d"),
    "win32-x64": (11830754035, "dd6d1e33d53a917c1abd0347aa05e536679d7e793fa4a259f17d6440047a805e"),
    "win32-arm64": (11832385725, "7fd355f3489b90d026d9bdf18f8b4d7987b87f58ceea2aa263861318f8377346"),
}
MAC = {
    "darwin-arm64": "b46a1f6f5b76599e8109410c7f048f9fca89e3ad333063e5242f7fbf8f41a573",
    "darwin-x64": "0426a631d13688f54b87fbd9433132c752f7cff8452663097d0efef8848f6fae",
}


def gh(*args: str) -> str:
    return subprocess.check_output(["gh", *args], text=True)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(data)
    return value.hexdigest()


def main() -> None:
    run = json.loads(gh("api", f"repos/{REPO}/actions/runs/{RUN}"))
    assert run["head_sha"] == SOURCE and run["conclusion"] == "success"
    assert run["head_repository"]["full_name"] == REPO
    reference = json.loads(gh("api", f"repos/{REPO}/git/ref/tags/{TAG}"))["object"]
    visited = set()
    while reference["type"] == "tag":
        assert reference["sha"] not in visited and len(visited) < 5
        visited.add(reference["sha"])
        reference = json.loads(gh("api", f"repos/{REPO}/git/tags/{reference['sha']}"))["object"]
    assert reference["type"] == "commit" and reference["sha"] == SOURCE
    jobs = json.loads(gh("api", f"repos/{REPO}/actions/runs/{RUN}/jobs?per_page=100"))["jobs"]
    assert len(jobs) == 6 and all(job["conclusion"] == "success" for job in jobs)
    subprocess.run(["git", "merge-base", "--is-ancestor", SOURCE, "HEAD"], check=True)
    # Operational publisher source differs; payload files must retain exact 615 bytes.
    subprocess.run(["git", "diff", "--exit-code", SOURCE, "HEAD", "--", "src", "pyproject.toml",
                    "uv.lock", "openagent-host-tools.spec", "sidecars"], check=True)
    release = json.loads(gh("release", "view", TAG, "--repo", REPO,
                            "--json", "isDraft,targetCommitish,assets"))
    assert release["isDraft"] is True and release["targetCommitish"] == SOURCE
    output = Path("draft-artifacts")
    output.mkdir(exist_ok=False)
    proofs = []
    for key, (artifact_id, expected_sha) in INPUTS.items():
        artifact = json.loads(gh("api", f"repos/{REPO}/actions/artifacts/{artifact_id}"))
        assert artifact["workflow_run"]["id"] == RUN and artifact["workflow_run"]["head_sha"] == SOURCE
        assert artifact["name"] == f"utf8-qualified-{key}" and not artifact["expired"]
        assert artifact["digest"] == f"sha256:{expected_sha}"
        archive_zip = output / f"input-{key}.zip"
        with archive_zip.open("wb") as stream:
            subprocess.run(["gh", "api", f"repos/{REPO}/actions/artifacts/{artifact_id}/zip"], stdout=stream, check=True)
        assert digest(archive_zip) == expected_sha
        allowed = {f"openagent-host-tools-{key}.tar.gz", f"openagent-host-tools-{key}.tar.gz.sha256",
                   f"compatibility-{key}.json", f"source-{key}.txt", f"dependencies-{key}.txt", WHEEL}
        with zipfile.ZipFile(archive_zip) as package:
            assert set(package.namelist()) == allowed and len(package.namelist()) == len(allowed)
            assert package.read(f"source-{key}.txt").decode().strip() == SOURCE
            for name in allowed:
                if name == WHEEL and key != "linux-x64":
                    continue
                (output / name).write_bytes(package.read(name))
        archive_zip.unlink()
        verified = verify_archive(output / f"openagent-host-tools-{key}.tar.gz", expected_platform=key, expected_version="0.1.3")
        baseline = json.loads((output / f"compatibility-{key}.json").read_text())
        assert all(verified.manifest["files"][name] == entry for name, entry in baseline["reusedFiles"].items())
        proofs.append({"platform": key, "artifactId": artifact_id, "artifactDigest": expected_sha})
    for key, expected_sha in MAC.items():
        name = f"openagent-host-tools-{key}.tar.gz"
        for asset in (name, name + ".sha256"):
            subprocess.run(["gh", "release", "download", TAG, "--repo", REPO, "--pattern", asset, "--dir", str(output)], check=True)
        assert digest(output / name) == expected_sha
        verify_archive(output / name, expected_platform=key, expected_version="0.1.3")
    subprocess.run(["gh", "release", "download", TAG, "--repo", REPO,
                    "--pattern", "mac-signing-qualification.json", "--dir", str(output)], check=True)
    mac_proof = json.loads((output / "mac-signing-qualification.json").read_text())
    assert mac_proof["sourceCommit"] == SOURCE and mac_proof["qualifiedRun"] == RUN
    assert mac_proof["notarization"]["status"] == "Accepted"
    assert {row["platform"]: row["sha256"] for row in mac_proof["macArchives"]} == MAC
    assert all(row["appleTicketMatches"] and row["codesignStrictNotarizedRequirement"] == "pass" for row in mac_proof["macArchives"])
    subprocess.run(["python", "scripts/release_index.py", str(output), "--source-commit", SOURCE,
                    "--source-repository", REPO], check=True)
    proof = {"payloadSource": SOURCE, "publisherSource": os.environ["GITHUB_SHA"],
             "qualificationRun": RUN, "qualifiedPlatforms": 6, "artifacts": proofs,
             "macNotarization": mac_proof["notarization"], "draftOnly": True}
    (output / "source-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
    files = sorted(path for path in output.iterdir() if path.is_file())
    (output / "SHA256SUMS").write_text("".join(f"{digest(path)}  {path.name}\n" for path in files))
    existing = {asset["name"]: asset for asset in release["assets"]}
    for path in sorted(output.iterdir()):
        if path.name in existing:
            assert existing[path.name]["digest"] == f"sha256:{digest(path)}", path.name
        else:
            subprocess.run(["gh", "release", "upload", TAG, "--repo", REPO, str(path)], check=True)
    print("Complete checksum/source-bound draft assembled; publishing remains a separate reviewed action.")


if __name__ == "__main__":
    main()

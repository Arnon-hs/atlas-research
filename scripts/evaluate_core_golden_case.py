# SPDX-License-Identifier: MIT
"""Evaluate one pinned public AtlasRepo Core decision pack offline."""

from __future__ import annotations

import argparse
import hashlib
import os
import stat
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from atlas_research.canonical import canonical_json_bytes, canonical_sha256, strict_json_loads
from atlas_research.errors import AtlasResearchError

FEATURE_FLAG = "ATLAS_RESEARCH_CORE_EVAL_ENABLED"
MAX_FIXTURE_BYTES = 64 * 1024
EXPECTED_CASE_SCHEMA = "atlasrepo.research/core-golden-eval-case/v0.1"
EXPECTED_CASE_ID = "dify-core-v0.2.1-conditional"
EXPECTED_CORE_RELEASE: Mapping[str, object] = {
    "repository": "https://github.com/Arnon-hs/atlasrepo-core",
    "version": "0.2.1",
    "tag": "v0.2.1",
    "commit": "6bffb144add56d13de0c0bf9be9c39931ec0c9bb",
    "release_uri": "https://github.com/Arnon-hs/atlasrepo-core/releases/tag/v0.2.1",
    "package_uri": (
        "https://github.com/Arnon-hs/atlasrepo-core/releases/download/v0.2.1/"
        "atlasrepo-core-0.2.1.tgz"
    ),
    "package_size_bytes": 356223,
    "package_sha256": "sha256:445a986cba38a87edcbdd50c787e7468e057d42bd3792aab2824e0a78fc2d81b",
    "license": "Apache-2.0",
}
EXPECTED_ARTIFACT: Mapping[str, object] = {
    "path": "decision-pack.v0.1.json",
    "source_path": "examples/dify/decision-pack.v0.1.json",
    "resource_ref": (
        "github:Arnon-hs/atlasrepo-core@6bffb144add56d13de0c0bf9be9c39931ec0c9bb:"
        "examples/dify/decision-pack.v0.1.json"
    ),
    "public_uri": (
        "https://github.com/Arnon-hs/atlasrepo-core/blob/"
        "6bffb144add56d13de0c0bf9be9c39931ec0c9bb/examples/dify/decision-pack.v0.1.json"
    ),
    "access_hint": "public",
    "size_bytes": 7141,
    "raw_sha256": "sha256:e16639137039b5d3237f20b8447bef7dd4735aed33e55fd6be3357924b308261",
    "canonical_sha256": ("sha256:0f4863d6b583f6ae00eef4a4cde9cdebc6be575c5e99ec827ccb0436dc4282f5"),
}
EXPECTED_REDISTRIBUTION: Mapping[str, object] = {
    "license": {
        "path": "LICENSE.atlasrepo-core",
        "size_bytes": 11357,
        "raw_sha256": ("sha256:c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4"),
    },
    "notice": {
        "path": "NOTICE.atlasrepo-core",
        "size_bytes": 123,
        "raw_sha256": ("sha256:247a5b7ccd7fc0335c030e1d00e060b1ebcb0a8897e8e3cf8754d9fa54da225a"),
    },
}
EXPECTED_EXPECTATION: Mapping[str, object] = {
    "schema_version": "atlasrepo.core/decision-pack/v0.1",
    "decision_status": "conditional",
    "citation_count": 9,
    "unresolved_gates": [
        "license-review",
        "local-runtime-pilot",
        "operations-review",
        "security-review",
    ],
}
EXPECTED_CASE_FIELDS = {
    "schema_version",
    "case_id",
    "feature_flag",
    "core_release",
    "artifact",
    "redistribution",
    "expectation",
}


class GoldenEvalError(ValueError):
    """The golden case is not safe or does not match its immutable pins."""


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise GoldenEvalError(f"{field} must be an object")
    return cast(Mapping[str, object], value)


def _string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise GoldenEvalError(f"{field} must be a non-empty string")
    return value


def _safe_read(path: Path, *, maximum: int = MAX_FIXTURE_BYTES) -> bytes:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise GoldenEvalError("fixture file could not be opened safely") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise GoldenEvalError("fixture input must be one regular file")
        if before.st_size > maximum:
            raise GoldenEvalError("fixture file exceeds the byte limit")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(descriptor)
        if len(data) > maximum:
            raise GoldenEvalError("fixture file exceeds the byte limit")
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise GoldenEvalError("fixture file changed while being read")
        return data
    finally:
        os.close(descriptor)


def _load_mapping(path: Path, field: str) -> tuple[Mapping[str, object], bytes]:
    raw = _safe_read(path)
    return _mapping(strict_json_loads(raw), field), raw


def _verify_pinned_file(case_root: Path, reference: Mapping[str, object], field: str) -> None:
    name = _string(reference.get("path"), f"{field}.path")
    if Path(name).name != name:
        raise GoldenEvalError(f"{field}.path must be a local filename")
    raw = _safe_read(case_root / name)
    digest = f"sha256:{hashlib.sha256(raw).hexdigest()}"
    if len(raw) != reference.get("size_bytes") or digest != reference.get("raw_sha256"):
        raise GoldenEvalError(f"{field} bytes do not match their pin")


def _require_public_citations(decision_pack: Mapping[str, object]) -> list[Mapping[str, object]]:
    raw_citations = decision_pack.get("citations")
    if not isinstance(raw_citations, list) or not raw_citations:
        raise GoldenEvalError("decision_pack.citations must be a non-empty array")
    citations = [
        _mapping(citation, f"decision_pack.citations[{index}]")
        for index, citation in enumerate(raw_citations)
    ]
    for index, citation in enumerate(citations):
        if citation.get("accessHint") != "public":
            raise GoldenEvalError(f"decision_pack.citations[{index}] is not public")
        public_uri = _string(
            citation.get("publicUri"), f"decision_pack.citations[{index}].publicUri"
        )
        if not public_uri.startswith("https://github.com/"):
            raise GoldenEvalError(
                f"decision_pack.citations[{index}].publicUri is outside the public allowlist"
            )
    return citations


def evaluate(case_root: Path) -> Mapping[str, object]:
    case, _ = _load_mapping(case_root / "case.json", "case")
    if set(case) != EXPECTED_CASE_FIELDS or case.get("schema_version") != EXPECTED_CASE_SCHEMA:
        raise GoldenEvalError("case schema does not match the exact supported contract")
    if case.get("case_id") != EXPECTED_CASE_ID:
        raise GoldenEvalError("case.case_id does not match the exact supported case")
    feature_flag = _mapping(case.get("feature_flag"), "case.feature_flag")
    if feature_flag != {"name": FEATURE_FLAG, "default": "false"}:
        raise GoldenEvalError("case.feature_flag must preserve the disabled default")

    release = _mapping(case.get("core_release"), "case.core_release")
    artifact = _mapping(case.get("artifact"), "case.artifact")
    redistribution = _mapping(case.get("redistribution"), "case.redistribution")
    expectation = _mapping(case.get("expectation"), "case.expectation")
    if release != EXPECTED_CORE_RELEASE:
        raise GoldenEvalError("case.core_release does not match the allowed Core v0.2.1 release")
    if artifact.get("access_hint") != "public":
        raise GoldenEvalError("case.artifact is not public")
    if artifact != EXPECTED_ARTIFACT:
        raise GoldenEvalError("case.artifact does not match the allowed Core v0.2.1 artifact")
    if redistribution != EXPECTED_REDISTRIBUTION:
        raise GoldenEvalError(
            "case.redistribution does not match the allowed Core license material"
        )
    if expectation != EXPECTED_EXPECTATION:
        raise GoldenEvalError("case.expectation does not match the exact supported contract")
    _verify_pinned_file(
        case_root,
        _mapping(redistribution.get("license"), "case.redistribution.license"),
        "case.redistribution.license",
    )
    _verify_pinned_file(
        case_root,
        _mapping(redistribution.get("notice"), "case.redistribution.notice"),
        "case.redistribution.notice",
    )

    artifact_name = _string(artifact.get("path"), "case.artifact.path")
    if Path(artifact_name).name != artifact_name:
        raise GoldenEvalError("case.artifact.path must be a local filename")
    decision_pack, raw_pack = _load_mapping(case_root / artifact_name, "decision_pack")
    citations = _require_public_citations(decision_pack)

    raw_sha256 = f"sha256:{hashlib.sha256(raw_pack).hexdigest()}"
    if len(raw_pack) != artifact.get("size_bytes") or raw_sha256 != artifact.get("raw_sha256"):
        raise GoldenEvalError("decision pack raw bytes do not match the pinned artifact")
    canonical_digest = f"sha256:{canonical_sha256(decision_pack)}"
    if canonical_digest != artifact.get("canonical_sha256"):
        raise GoldenEvalError("decision pack canonical digest does not match the pinned artifact")

    unresolved_gates = decision_pack.get("unresolvedGates")
    if not isinstance(unresolved_gates, list) or any(
        not isinstance(gate, str) for gate in unresolved_gates
    ):
        raise GoldenEvalError("decision_pack.unresolvedGates must be an array of strings")
    if (
        decision_pack.get("schemaVersion") != expectation.get("schema_version")
        or decision_pack.get("status") != expectation.get("decision_status")
        or len(citations) != expectation.get("citation_count")
        or unresolved_gates != expectation.get("unresolved_gates")
    ):
        raise GoldenEvalError("decision pack does not match the golden expectation")

    return {
        "schema_version": "atlasrepo.research/core-golden-eval-result/v0.1",
        "case_id": EXPECTED_CASE_ID,
        "status": "passed",
        "core": {
            "repository": _string(release.get("repository"), "case.core_release.repository"),
            "version": _string(release.get("version"), "case.core_release.version"),
            "tag": _string(release.get("tag"), "case.core_release.tag"),
            "commit": _string(release.get("commit"), "case.core_release.commit"),
            "artifact_resource_ref": _string(
                artifact.get("resource_ref"), "case.artifact.resource_ref"
            ),
            "artifact_raw_sha256": raw_sha256,
            "artifact_canonical_sha256": canonical_digest,
        },
        "decision": {
            "status": decision_pack["status"],
            "citation_count": len(citations),
            "unresolved_gates": unresolved_gates,
        },
        "checks": [
            {"id": "feature-disabled-by-default", "passed": True},
            {"id": "core-artifact-digest-pinned", "passed": True},
            {"id": "core-license-materials-present", "passed": True},
            {"id": "public-evidence-only", "passed": True},
            {"id": "conditional-gates-preserved", "passed": True},
        ],
        "limitations": [
            "One golden case is not a benchmark; use 50-100 reviewed cases before "
            "benchmark claims.",
            "This result does not execute Dify, call an LLM, access a network, or "
            "authorize deployment.",
            "License compatibility and all runtime, operations, and security gates "
            "remain unresolved.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case-root", type=Path, required=True)
    args = parser.parse_args()

    enabled = os.environ.get(FEATURE_FLAG, "false")
    if enabled == "false":
        print(f"{FEATURE_FLAG} is false; Core golden evaluation is disabled", file=sys.stderr)
        return 78
    if enabled != "true":
        print(f"{FEATURE_FLAG} must be exactly true or false", file=sys.stderr)
        return 64
    try:
        result = evaluate(args.case_root)
    except GoldenEvalError as error:
        print(f"Core golden evaluation failed: {error}", file=sys.stderr)
        return 2
    except AtlasResearchError:
        print("Core golden evaluation failed: fixture JSON is invalid", file=sys.stderr)
        return 2
    except OSError:
        print("Core golden evaluation failed: fixture bytes are unavailable", file=sys.stderr)
        return 2
    sys.stdout.buffer.write(canonical_json_bytes(result) + b"\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

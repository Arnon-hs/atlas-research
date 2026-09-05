# SPDX-License-Identifier: MIT
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).parents[1]
CASE_ROOT = ROOT / "examples" / "core-v0.2.1-dify"
SCRIPT = ROOT / "scripts" / "evaluate_core_golden_case.py"
FEATURE_FLAG = "ATLAS_RESEARCH_CORE_EVAL_ENABLED"
CASE_FILES = (
    "case.json",
    "decision-pack.v0.1.json",
    "LICENSE.atlasrepo-core",
    "NOTICE.atlasrepo-core",
)


def _run(case_root: Path, *, enabled: str | None) -> subprocess.CompletedProcess[bytes]:
    environment = dict(os.environ)
    if enabled is None:
        environment.pop(FEATURE_FLAG, None)
    else:
        environment[FEATURE_FLAG] = enabled
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--case-root", str(case_root)],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        check=False,
    )


def _copy_case(tmp_path: Path) -> Path:
    copied_root = tmp_path / "case"
    copied_root.mkdir()
    for name in CASE_FILES:
        (copied_root / name).write_bytes((CASE_ROOT / name).read_bytes())
    return copied_root


def test_core_golden_eval_is_disabled_by_default() -> None:
    result = _run(CASE_ROOT, enabled=None)
    assert result.returncode == 78
    assert result.stdout == b""
    assert result.stderr == (
        b"ATLAS_RESEARCH_CORE_EVAL_ENABLED is false; Core golden evaluation is disabled\n"
    )


def test_dify_core_golden_eval_is_exact_and_deterministic() -> None:
    case = json.loads((CASE_ROOT / "case.json").read_bytes())
    assert case["feature_flag"] == {"name": FEATURE_FLAG, "default": "false"}
    assert case["core_release"] == {
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
        "package_sha256": (
            "sha256:445a986cba38a87edcbdd50c787e7468e057d42bd3792aab2824e0a78fc2d81b"
        ),
        "license": "Apache-2.0",
    }
    assert case["artifact"]["raw_sha256"] == (
        "sha256:e16639137039b5d3237f20b8447bef7dd4735aed33e55fd6be3357924b308261"
    )
    assert case["artifact"]["canonical_sha256"] == (
        "sha256:0f4863d6b583f6ae00eef4a4cde9cdebc6be575c5e99ec827ccb0436dc4282f5"
    )
    assert case["redistribution"] == {
        "license": {
            "path": "LICENSE.atlasrepo-core",
            "size_bytes": 11357,
            "raw_sha256": (
                "sha256:c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4"
            ),
        },
        "notice": {
            "path": "NOTICE.atlasrepo-core",
            "size_bytes": 123,
            "raw_sha256": (
                "sha256:247a5b7ccd7fc0335c030e1d00e060b1ebcb0a8897e8e3cf8754d9fa54da225a"
            ),
        },
    }

    first = _run(CASE_ROOT, enabled="true")
    second = _run(CASE_ROOT, enabled="true")
    assert first.returncode == second.returncode == 0
    assert first.stderr == second.stderr == b""
    assert first.stdout == second.stdout == (CASE_ROOT / "expected-result.json").read_bytes()


@pytest.mark.parametrize("access_hint", ["restricted", "private"])
def test_core_golden_eval_rejects_nonpublic_evidence(tmp_path: Path, access_hint: str) -> None:
    copied_root = _copy_case(tmp_path)
    decision_pack: dict[str, Any] = json.loads(
        (copied_root / "decision-pack.v0.1.json").read_bytes()
    )
    decision_pack["citations"][0]["accessHint"] = access_hint
    (copied_root / "decision-pack.v0.1.json").write_text(
        json.dumps(decision_pack), encoding="utf-8"
    )

    result = _run(copied_root, enabled="true")
    assert result.returncode == 2
    assert result.stdout == b""
    assert b"is not public" in result.stderr


def test_core_golden_eval_rejects_nonpublic_case_artifact(tmp_path: Path) -> None:
    copied_root = _copy_case(tmp_path)
    case: dict[str, Any] = json.loads((copied_root / "case.json").read_bytes())
    case["artifact"]["access_hint"] = "private"
    (copied_root / "case.json").write_text(json.dumps(case), encoding="utf-8")

    result = _run(copied_root, enabled="true")
    assert result.returncode == 2
    assert result.stdout == b""
    assert b"case.artifact is not public" in result.stderr


def test_core_golden_eval_fails_closed_without_echoing_malformed_fixture_path(
    tmp_path: Path,
) -> None:
    case_root = tmp_path / "private-looking-case-name"
    case_root.mkdir()
    (case_root / "case.json").write_bytes(b'{"duplicate":1,"duplicate":2}')

    result = _run(case_root, enabled="true")
    assert result.returncode == 2
    assert result.stdout == b""
    assert result.stderr == b"Core golden evaluation failed: fixture JSON is invalid\n"
    assert str(case_root).encode() not in result.stderr


@pytest.mark.parametrize(
    ("field", "fake_value"),
    [
        ("repository", "https://github.com/example/fake-core"),
        ("version", "0.2.0"),
        ("tag", "v0.2.0"),
        ("commit", "0" * 40),
        ("release_uri", "https://github.com/example/fake-core/releases/tag/v0.2.1"),
        ("package_uri", "https://github.com/example/fake-core/core.tgz"),
        ("package_size_bytes", 1),
        ("package_sha256", f"sha256:{'0' * 64}"),
        ("license", "MIT"),
    ],
)
def test_core_golden_eval_rejects_fake_release_metadata(
    tmp_path: Path, field: str, fake_value: object
) -> None:
    copied_root = _copy_case(tmp_path)
    case: dict[str, Any] = json.loads((copied_root / "case.json").read_bytes())
    case["core_release"][field] = fake_value
    (copied_root / "case.json").write_text(json.dumps(case), encoding="utf-8")

    result = _run(copied_root, enabled="true")
    assert result.returncode == 2
    assert result.stdout == b""
    assert b"does not match the allowed Core v0.2.1 release" in result.stderr


@pytest.mark.parametrize(
    ("field", "fake_value"),
    [
        ("resource_ref", "github:Arnon-hs/private-core@0000000:private/decision-pack.json"),
        ("public_uri", "https://github.com/Arnon-hs/private-core/private/decision-pack.json"),
    ],
)
def test_core_golden_eval_rejects_private_artifact_locators(
    tmp_path: Path, field: str, fake_value: str
) -> None:
    copied_root = _copy_case(tmp_path)
    case: dict[str, Any] = json.loads((copied_root / "case.json").read_bytes())
    case["artifact"][field] = fake_value
    (copied_root / "case.json").write_text(json.dumps(case), encoding="utf-8")

    result = _run(copied_root, enabled="true")
    assert result.returncode == 2
    assert result.stdout == b""
    assert b"does not match the allowed Core v0.2.1 artifact" in result.stderr


def test_core_golden_eval_rejects_wrong_case_schema_version(tmp_path: Path) -> None:
    copied_root = _copy_case(tmp_path)
    case: dict[str, Any] = json.loads((copied_root / "case.json").read_bytes())
    case["schema_version"] = "atlasrepo.research/core-golden-eval-case/v9"
    (copied_root / "case.json").write_text(json.dumps(case), encoding="utf-8")

    result = _run(copied_root, enabled="true")
    assert result.returncode == 2
    assert result.stdout == b""
    assert b"case schema does not match the exact supported contract" in result.stderr


def test_core_golden_eval_rejects_mutated_expectation(tmp_path: Path) -> None:
    copied_root = _copy_case(tmp_path)
    case: dict[str, Any] = json.loads((copied_root / "case.json").read_bytes())
    case["expectation"]["decision_status"] = "recommended"
    (copied_root / "case.json").write_text(json.dumps(case), encoding="utf-8")

    result = _run(copied_root, enabled="true")
    assert result.returncode == 2
    assert result.stdout == b""
    assert b"case.expectation does not match the exact supported contract" in result.stderr


@pytest.mark.parametrize("name", CASE_FILES)
def test_core_golden_eval_rejects_symlinked_fixture_files(tmp_path: Path, name: str) -> None:
    copied_root = _copy_case(tmp_path)
    target = tmp_path / f"private-{name}"
    target.write_bytes((copied_root / name).read_bytes())
    (copied_root / name).unlink()
    (copied_root / name).symlink_to(target)

    result = _run(copied_root, enabled="true")
    assert result.returncode == 2
    assert result.stdout == b""
    assert b"fixture file could not be opened safely" in result.stderr
    assert str(target).encode() not in result.stderr


@pytest.mark.parametrize("name", ["case.json", "decision-pack.v0.1.json"])
def test_core_golden_eval_rejects_oversized_fixture_files(tmp_path: Path, name: str) -> None:
    copied_root = _copy_case(tmp_path)
    (copied_root / name).write_bytes(b" " * ((64 * 1024) + 1))

    result = _run(copied_root, enabled="true")
    assert result.returncode == 2
    assert result.stdout == b""
    assert result.stderr == b"Core golden evaluation failed: fixture file exceeds the byte limit\n"
    assert str(copied_root).encode() not in result.stderr

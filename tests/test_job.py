# SPDX-License-Identifier: MIT
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from atlas_research.canonical import canonical_json_bytes
from atlas_research.errors import ValidationError
from atlas_research.job import load_job, parse_job, parse_timestamp


def _ref(role: str, media_type: str, schema_id: str) -> dict[str, object]:
    return {
        "uri": "inputs/value.json",
        "role": role,
        "media_type": media_type,
        "sha256": "0" * 64,
        "size_bytes": 1,
        "producer": {"name": "atlas-research", "version": "0.1.0"},
        "external_schema": {"id": schema_id, "version": "1.0.0"},
    }


def valid_job() -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "task": "research.experiment",
        "job_id": "job-1",
        "attempt": 1,
        "idempotency_key": "experiment-key-0001",
        "created_at": "2026-08-30T00:00:00Z",
        "deadline": "2026-08-30T01:00:00Z",
        "dataset_manifest": _ref(
            "dataset_manifest",
            "application/vnd.atlas-research.dataset-manifest+json",
            "urn:atlasrepo:atlas-research:schema:v1:dataset-manifest",
        ),
        "benchmark_manifest": _ref(
            "benchmark_manifest",
            "application/vnd.atlas-research.benchmark-manifest+json",
            "urn:atlasrepo:atlas-research:schema:v1:benchmark-manifest",
        ),
        "baseline_evaluation_payload": _ref(
            "evaluation_payload", "application/json", "urn:atlasrepo:test:evaluator:v1"
        ),
        "candidate": _ref(
            "candidate",
            "application/vnd.atlas-research.candidate+json",
            "urn:atlasrepo:atlas-research:schema:v1:candidate-artifact",
        ),
        "evaluation_split": "validation",
        "limits": {
            "wall_seconds": 60,
            "max_records": 100,
            "max_input_bytes": 1_000,
            "max_output_bytes": 1_000,
            "max_workspace_bytes": 10_000,
            "max_peak_rss_bytes": 1_048_576,
            "max_open_files": 16,
            "max_json_depth": 8,
            "max_string_bytes": 128,
        },
    }


def test_parse_job_and_deadline() -> None:
    job = parse_job(valid_job())
    assert job.job_id == "job-1"
    with pytest.raises(ValidationError, match="future"):
        job.ensure_not_expired(now=datetime(2026, 8, 29, 23, 59, 59, tzinfo=UTC))
    with pytest.raises(ValidationError, match="deadline"):
        job.ensure_not_expired(now=datetime(2026, 8, 30, 1, 0, 1, tzinfo=UTC))


def test_test_split_requires_review() -> None:
    value = valid_job()
    value["evaluation_split"] = "test"
    with pytest.raises(ValidationError, match="requires review"):
        parse_job(value)


def test_test_review_must_be_effective_at_evaluation_admission() -> None:
    value = valid_job()
    value["evaluation_split"] = "test"
    value["review_authorization"] = {
        "reviewer": "reviewer",
        "approved_at": "2026-08-30T00:30:00Z",
        "reason": "approved test evaluation",
    }
    job = parse_job(value)

    before_approval = datetime(2026, 8, 30, 0, 29, 59, tzinfo=UTC)
    with pytest.raises(ValidationError, match="not effective"):
        job.ensure_review_effective(now=before_approval)
    with pytest.raises(ValidationError, match="not effective"):
        job.ensure_not_expired(now=before_approval)

    job.ensure_review_effective(now=datetime(2026, 8, 30, 0, 30, tzinfo=UTC))


@pytest.mark.parametrize(
    "value",
    [
        "2026-08-30 00:00:00Z",
        "20260830T000000Z",
        "2026-W35-7T00:00:00Z",
        "2026-08-30T00:00Z",
        "2026-08-30T00:00:00.Z",
        "2026-08-30T00:00:00.1234567Z",
        "2026-08-30T24:00:00Z",
        "2026-02-29T00:00:00Z",
        "2026-08-30T00:00:00+00:00",
    ],
)
def test_parse_timestamp_rejects_noncanonical_utc_forms(value: str) -> None:
    with pytest.raises(ValidationError, match="RFC 3339 UTC"):
        parse_timestamp(value, field="created_at")


@pytest.mark.parametrize(
    "value",
    [
        "2026-08-30T00:00:00Z",
        "2026-08-30T00:00:00.1Z",
        "2026-08-30T00:00:00.123456Z",
    ],
)
def test_parse_timestamp_accepts_canonical_utc_with_bounded_fraction(value: str) -> None:
    assert parse_timestamp(value, field="created_at").tzinfo is UTC


def test_validation_rejects_review() -> None:
    value = valid_job()
    value["review_authorization"] = {
        "reviewer": "reviewer",
        "approved_at": "2026-08-30T00:00:00Z",
        "reason": "test",
    }
    with pytest.raises(ValidationError, match="cannot carry"):
        parse_job(value)


def test_load_job_rejects_symlink(tmp_path: Path) -> None:
    source = tmp_path / "job.json"
    source.write_bytes(canonical_json_bytes(valid_job()))
    linked = tmp_path / "linked.json"
    linked.symlink_to(source)
    with pytest.raises(ValidationError, match="could not be opened"):
        load_job(linked)


def test_load_job_reads_stable_regular_file(tmp_path: Path) -> None:
    path = tmp_path / "job.json"
    path.write_bytes(canonical_json_bytes(valid_job()))
    assert load_job(path).spec_sha256

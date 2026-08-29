# SPDX-License-Identifier: MIT
"""Strict admission for one offline research experiment job."""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from atlas_research.artifacts import ArtifactRef
from atlas_research.canonical import canonical_sha256, strict_json_loads
from atlas_research.constants import MAX_JOB_BYTES, SCHEMA_VERSION
from atlas_research.errors import ValidationError
from atlas_research.limits import EffectiveLimits

_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_IDEMPOTENCY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}$")
_RFC3339_UTC = re.compile(
    r"^[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])"
    r"T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:[.][0-9]{1,6})?Z$",
    re.ASCII,
)
_REQUIRED = {
    "schema_version",
    "task",
    "job_id",
    "attempt",
    "idempotency_key",
    "created_at",
    "deadline",
    "dataset_manifest",
    "benchmark_manifest",
    "baseline_evaluation_payload",
    "candidate",
    "evaluation_split",
    "limits",
}


@dataclass(frozen=True)
class ReviewAuthorization:
    reviewer: str
    approved_at: datetime
    reason: str


@dataclass(frozen=True)
class ResearchJob:
    raw: Mapping[str, Any]
    job_id: str
    attempt: int
    idempotency_key: str
    created_at: datetime
    deadline: datetime
    dataset_manifest: ArtifactRef
    benchmark_manifest: ArtifactRef
    baseline_evaluation_payload: ArtifactRef
    candidate: ArtifactRef
    evaluation_split: str
    review_authorization: ReviewAuthorization | None
    limits: EffectiveLimits
    spec_sha256: str

    def ensure_not_expired(self, *, now: datetime | None = None) -> None:
        instant = now or datetime.now(UTC)
        if instant < self.created_at:
            raise ValidationError("JOB_NOT_YET_VALID", "job creation time is in the future")
        if instant > self.deadline:
            raise ValidationError("JOB_EXPIRED", "job deadline has expired")
        self.ensure_review_effective(now=instant)

    def ensure_review_effective(self, *, now: datetime | None = None) -> None:
        """Require test review approval to exist no later than admission."""

        if self.evaluation_split != "test":
            return
        instant = now or datetime.now(UTC)
        review = self.review_authorization
        if review is None or review.approved_at > instant:
            raise ValidationError(
                "TEST_NOT_AUTHORIZED", "test review is not effective at evaluation admission"
            )


def parse_timestamp(value: object, *, field: str) -> datetime:
    if not isinstance(value, str) or _RFC3339_UTC.fullmatch(value) is None:
        raise ValidationError("INVALID_TIMESTAMP", f"{field} must be an RFC 3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError as error:
        raise ValidationError(
            "INVALID_TIMESTAMP", f"{field} must be an RFC 3339 UTC timestamp"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValidationError("INVALID_TIMESTAMP", f"{field} must be an RFC 3339 UTC timestamp")
    return parsed.astimezone(UTC)


def _expect_mapping(value: object, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValidationError("INVALID_JOB", f"{field} must be an object")
    return value


def _parse_review(value: object) -> ReviewAuthorization:
    mapping = _expect_mapping(value, field="review_authorization")
    if set(mapping) != {"reviewer", "approved_at", "reason"}:
        raise ValidationError("INVALID_JOB", "review authorization fields are invalid")
    reviewer = mapping["reviewer"]
    reason = mapping["reason"]
    if not isinstance(reviewer, str) or not 1 <= len(reviewer) <= 128:
        raise ValidationError("INVALID_JOB", "reviewer is invalid")
    if not isinstance(reason, str) or not 1 <= len(reason) <= 1_000:
        raise ValidationError("INVALID_JOB", "review reason is invalid")
    return ReviewAuthorization(
        reviewer=reviewer,
        approved_at=parse_timestamp(mapping["approved_at"], field="approved_at"),
        reason=reason,
    )


def parse_job(value: object) -> ResearchJob:
    mapping = _expect_mapping(value, field="job")
    allowed = _REQUIRED | {"review_authorization"}
    if set(mapping) != _REQUIRED and set(mapping) != allowed:
        raise ValidationError("INVALID_JOB", "job fields are invalid")
    if mapping["schema_version"] != SCHEMA_VERSION or mapping["task"] != "research.experiment":
        raise ValidationError("INVALID_JOB", "job contract identity is invalid")

    job_id = mapping["job_id"]
    key = mapping["idempotency_key"]
    attempt = mapping["attempt"]
    split = mapping["evaluation_split"]
    if not isinstance(job_id, str) or not _IDENTIFIER.fullmatch(job_id):
        raise ValidationError("INVALID_JOB", "job id is invalid")
    if not isinstance(key, str) or not _IDEMPOTENCY.fullmatch(key):
        raise ValidationError("INVALID_JOB", "idempotency key is invalid")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or not 1 <= attempt <= 1_000:
        raise ValidationError("INVALID_JOB", "attempt is invalid")
    if split not in {"validation", "test"}:
        raise ValidationError("INVALID_JOB", "evaluation split is invalid")

    created_at = parse_timestamp(mapping["created_at"], field="created_at")
    deadline = parse_timestamp(mapping["deadline"], field="deadline")
    if created_at > deadline:
        raise ValidationError("INVALID_JOB", "job deadline precedes creation")

    review = None
    if split == "test":
        if "review_authorization" not in mapping:
            raise ValidationError("TEST_NOT_AUTHORIZED", "test evaluation requires review")
        review = _parse_review(mapping["review_authorization"])
        if review.approved_at < created_at or review.approved_at > deadline:
            raise ValidationError("TEST_NOT_AUTHORIZED", "test review is outside the job window")
    elif "review_authorization" in mapping:
        raise ValidationError("INVALID_JOB", "validation jobs cannot carry test authorization")

    raw = dict(mapping)
    return ResearchJob(
        raw=raw,
        job_id=job_id,
        attempt=attempt,
        idempotency_key=key,
        created_at=created_at,
        deadline=deadline,
        dataset_manifest=ArtifactRef.from_mapping(
            _expect_mapping(mapping["dataset_manifest"], field="dataset_manifest")
        ),
        benchmark_manifest=ArtifactRef.from_mapping(
            _expect_mapping(mapping["benchmark_manifest"], field="benchmark_manifest")
        ),
        baseline_evaluation_payload=ArtifactRef.from_mapping(
            _expect_mapping(
                mapping["baseline_evaluation_payload"], field="baseline_evaluation_payload"
            )
        ),
        candidate=ArtifactRef.from_mapping(
            _expect_mapping(mapping["candidate"], field="candidate")
        ),
        evaluation_split=split,
        review_authorization=review,
        limits=EffectiveLimits.from_mapping(_expect_mapping(mapping["limits"], field="limits")),
        spec_sha256=canonical_sha256(raw),
    )


def load_job(path: Path) -> ResearchJob:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ValidationError("INVALID_JOB_FILE", "job file could not be opened") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise ValidationError("INVALID_JOB_FILE", "job file must be one regular file")
        if before.st_size > MAX_JOB_BYTES:
            raise ValidationError("JOB_TOO_LARGE", "job file exceeded the byte limit")
        chunks: list[bytes] = []
        remaining = MAX_JOB_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(descriptor)
        stable = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) == (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        if not stable or len(data) != before.st_size:
            raise ValidationError("INVALID_JOB_FILE", "job file changed while reading")
        if len(data) > MAX_JOB_BYTES:
            raise ValidationError("JOB_TOO_LARGE", "job file exceeded the byte limit")
    finally:
        os.close(descriptor)
    return parse_job(strict_json_loads(data))

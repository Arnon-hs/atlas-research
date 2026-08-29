# SPDX-License-Identifier: MIT
"""Bounded offline experiment worker with a parent watchdog."""

from __future__ import annotations

import fcntl
import hashlib
import os
import platform
import re
import resource
import signal
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, cast

from . import __version__
from .artifacts import (
    ArtifactExpectation,
    ArtifactRef,
    ArtifactResolver,
    build_artifact_ref,
    ensure_private_directory,
    read_private_bytes,
    write_canonical_json_private,
)
from .candidate import verify_candidate_change
from .canonical import canonical_json_bytes, canonical_sha256, strict_json_loads
from .constants import (
    BENCHMARK_MANIFEST_SCHEMA,
    CANDIDATE_SCHEMA,
    DATASET_MANIFEST_SCHEMA,
    LINEAR_EVALUATOR_SCHEMA,
    MAX_ARTIFACTS,
    MAX_JOB_BYTES,
    PRODUCER_NAME,
    RECEIPT_SCHEMA,
    SCHEMA_VERSION,
)
from .dataset import SplitName, parse_scoring_jsonl, verify_dataset_manifest
from .errors import AtlasResearchError, ConflictError, ResourceLimitError, ValidationError
from .evaluation import CanonicalResult, evaluate_experiment, parse_metric_specs
from .job import ResearchJob, load_job, parse_job, parse_timestamp
from .limits import EffectiveLimits, effective_limits, reduce_limits
from .receipts import (
    ReceiptCommit,
    ReceiptLog,
    canonical_result_sha256,
    validate_receipt_document,
)

_IDENTIFIER: Final = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$", re.ASCII)
_COMMIT: Final = re.compile(r"^[0-9a-f]{40}$", re.ASCII)
_SHA256: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_ERROR_CODE: Final = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$", re.ASCII)
_INSTALLED_PROVENANCE: Final = Path("/usr/local/share/atlas-research/source-provenance")
_SAFE_RELATIVE: Final = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}(?:/[A-Za-z0-9][A-Za-z0-9._-]{0,127}){0,15}$",
    re.ASCII,
)
_BENCHMARK_FIELDS: Final = {
    "schema_version",
    "benchmark_id",
    "created_at",
    "dataset_manifest",
    "baseline_evaluation_payload",
    "evaluation_split",
    "metrics",
    "minimum_records",
    "limits",
}
_CANDIDATE_FIELDS: Final = {
    "schema_version",
    "candidate_id",
    "created_at",
    "status",
    "parent_evaluation_payload",
    "research_level",
    "hypothesis",
    "changed_variable",
    "evaluation_payload",
    "target_contract",
    "generator",
}


@dataclass(frozen=True, slots=True)
class WorkerIdentity:
    """Non-secret worker provenance written into results and receipts."""

    worker_id: str = "local-worker"
    session_id: str = ""

    def normalized(self) -> WorkerIdentity:
        worker_id = self.worker_id
        session_id = self.session_id or f"session-{uuid.uuid4().hex}"
        if not _IDENTIFIER.fullmatch(worker_id) or not _IDENTIFIER.fullmatch(session_id):
            raise ValidationError("WORKER_IDENTITY_INVALID", "Worker identity is invalid")
        return WorkerIdentity(worker_id=worker_id, session_id=session_id)


@dataclass(frozen=True, slots=True)
class WorkerOutcome:
    """Terminal result bytes and their immutable output path."""

    result: Mapping[str, object]
    path: Path
    replayed: bool


@dataclass(frozen=True, slots=True)
class SourceProvenance:
    """Exact checkout or wheel provenance used by an experiment receipt."""

    git_commit: str
    revision_kind: str
    source_artifact_sha256: str | None = None

    def normalized(self) -> SourceProvenance:
        if _COMMIT.fullmatch(self.git_commit) is None:
            raise ValidationError("PROVENANCE_UNAVAILABLE", "Git commit provenance is unavailable")
        if self.revision_kind not in {"verified_checkout", "declared_wheel_revision"}:
            raise ValidationError("PROVENANCE_UNAVAILABLE", "Source provenance kind is invalid")
        if self.revision_kind == "verified_checkout":
            if self.source_artifact_sha256 is not None:
                raise ValidationError("PROVENANCE_UNAVAILABLE", "Checkout provenance is invalid")
        elif (
            self.source_artifact_sha256 is None
            or _SHA256.fullmatch(self.source_artifact_sha256) is None
        ):
            raise ValidationError("PROVENANCE_UNAVAILABLE", "Wheel provenance is invalid")
        return self


@dataclass(frozen=True, slots=True)
class _EvaluationPacket:
    started_at: str
    wall_milliseconds: int
    records_evaluated: int
    peak_rss_bytes: int
    canonical_result: CanonicalResult

    def to_mapping(self) -> dict[str, object]:
        return {
            "ok": True,
            "started_at": self.started_at,
            "wall_milliseconds": self.wall_milliseconds,
            "records_evaluated": self.records_evaluated,
            "peak_rss_bytes": self.peak_rss_bytes,
            "canonical_result": self.canonical_result,
        }


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _as_mapping(value: object, *, code: str, message: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ValidationError(code, message)
    return cast(Mapping[str, object], value)


def _artifact_ref(value: object, *, code: str, message: str) -> ArtifactRef:
    return ArtifactRef.from_mapping(_as_mapping(value, code=code, message=message))


def _same_ref(left: ArtifactRef, right: ArtifactRef) -> bool:
    return left.to_mapping() == right.to_mapping()


def _benchmark_document(
    job: ResearchJob,
    resolver: ArtifactResolver,
) -> tuple[Mapping[str, object], EffectiveLimits]:
    resolved = resolver.resolve(
        job.benchmark_manifest,
        ArtifactExpectation(
            role="benchmark_manifest",
            producer_name=PRODUCER_NAME,
            external_schema_id=BENCHMARK_MANIFEST_SCHEMA,
            external_schema_version=SCHEMA_VERSION,
        ),
        parse_json=True,
        max_bytes=job.limits.max_input_bytes,
        max_json_depth=job.limits.max_json_depth,
        max_string_bytes=job.limits.max_string_bytes,
    )
    benchmark = _as_mapping(
        resolved.json_value,
        code="INVALID_BENCHMARK",
        message="Benchmark manifest must be an object",
    )
    if set(benchmark) != _BENCHMARK_FIELDS or benchmark.get("schema_version") != SCHEMA_VERSION:
        raise ValidationError("INVALID_BENCHMARK", "Benchmark manifest fields are invalid")
    benchmark_limits = EffectiveLimits.from_mapping(
        _as_mapping(
            benchmark.get("limits"),
            code="INVALID_BENCHMARK",
            message="Benchmark limits are invalid",
        )
    )
    return benchmark, effective_limits(job.limits, benchmark_limits)


def _validate_benchmark_bindings(
    job: ResearchJob,
    benchmark: Mapping[str, object],
) -> tuple[Mapping[str, object], Mapping[str, object]]:
    benchmark_id = benchmark.get("benchmark_id")
    if not isinstance(benchmark_id, str) or _IDENTIFIER.fullmatch(benchmark_id) is None:
        raise ValidationError("INVALID_BENCHMARK", "Benchmark identity is invalid")
    benchmark_created_at = parse_timestamp(
        benchmark.get("created_at"), field="benchmark.created_at"
    )
    if benchmark_created_at > job.created_at:
        raise ValidationError("INVALID_BENCHMARK", "Benchmark was created after the job")
    dataset_ref = _artifact_ref(
        benchmark.get("dataset_manifest"),
        code="INVALID_BENCHMARK",
        message="Benchmark dataset reference is invalid",
    )
    baseline_ref = _artifact_ref(
        benchmark.get("baseline_evaluation_payload"),
        code="INVALID_BENCHMARK",
        message="Benchmark baseline reference is invalid",
    )
    if not _same_ref(dataset_ref, job.dataset_manifest):
        raise ValidationError("JOB_DATASET_MISMATCH", "Job and benchmark datasets differ")
    if not _same_ref(baseline_ref, job.baseline_evaluation_payload):
        raise ValidationError("JOB_BASELINE_MISMATCH", "Job and benchmark baselines differ")
    if benchmark.get("evaluation_split") != job.evaluation_split:
        raise ValidationError("JOB_SPLIT_MISMATCH", "Job and benchmark splits differ")
    metrics = _as_mapping(
        benchmark.get("metrics"),
        code="INVALID_BENCHMARK",
        message="Benchmark metrics are invalid",
    )
    parse_metric_specs(metrics)
    minimum_records = _as_mapping(
        benchmark.get("minimum_records"),
        code="INVALID_BENCHMARK",
        message="Benchmark minimum record counts are invalid",
    )
    if set(minimum_records) != {"train", "validation", "test"}:
        raise ValidationError("INVALID_BENCHMARK", "Benchmark minimum record counts are invalid")
    for value in minimum_records.values():
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 1_000_000:
            raise ValidationError(
                "INVALID_BENCHMARK", "Benchmark minimum record counts are invalid"
            )
    return metrics, minimum_records


def _candidate_document(
    job: ResearchJob,
    resolver: ArtifactResolver,
    limits: EffectiveLimits,
) -> tuple[Mapping[str, object], ArtifactRef]:
    resolved = resolver.resolve(
        job.candidate,
        ArtifactExpectation(
            role="candidate",
            producer_name=PRODUCER_NAME,
            external_schema_id=CANDIDATE_SCHEMA,
            external_schema_version=SCHEMA_VERSION,
        ),
        parse_json=True,
        max_bytes=limits.max_input_bytes,
        max_json_depth=limits.max_json_depth,
        max_string_bytes=limits.max_string_bytes,
    )
    candidate = _as_mapping(
        resolved.json_value,
        code="INVALID_CANDIDATE",
        message="Candidate artifact must be an object",
    )
    if (
        set(candidate) != _CANDIDATE_FIELDS
        or candidate.get("schema_version") != SCHEMA_VERSION
        or candidate.get("status") != "proposed"
        or candidate.get("research_level") not in {"LEVEL_1", "LEVEL_2"}
    ):
        raise ValidationError("INVALID_CANDIDATE", "Candidate artifact fields are invalid")
    candidate_id = candidate.get("candidate_id")
    hypothesis = candidate.get("hypothesis")
    if not isinstance(candidate_id, str) or _IDENTIFIER.fullmatch(candidate_id) is None:
        raise ValidationError("INVALID_CANDIDATE", "Candidate identity is invalid")
    if not isinstance(hypothesis, str) or not 1 <= len(hypothesis) <= 2_000:
        raise ValidationError("INVALID_CANDIDATE", "Candidate hypothesis is invalid")
    candidate_created_at = parse_timestamp(
        candidate.get("created_at"), field="candidate.created_at"
    )
    if candidate_created_at > job.created_at:
        raise ValidationError("INVALID_CANDIDATE", "Candidate was created after the job")
    generator = _as_mapping(
        candidate.get("generator"),
        code="INVALID_CANDIDATE",
        message="Candidate generator is invalid",
    )
    if generator.get("kind") == "human":
        if set(generator) != {"kind"}:
            raise ValidationError("INVALID_CANDIDATE", "Human generator fields are invalid")
    elif generator.get("kind") == "qwen":
        if set(generator) != {"kind", "model", "model_sha256", "prompt_sha256"}:
            raise ValidationError("INVALID_CANDIDATE", "Qwen generator fields are invalid")
        model = generator.get("model")
        model_digest = generator.get("model_sha256")
        prompt_digest = generator.get("prompt_sha256")
        if (
            not isinstance(model, str)
            or not 1 <= len(model) <= 128
            or not isinstance(model_digest, str)
            or _SHA256.fullmatch(model_digest) is None
            or not isinstance(prompt_digest, str)
            or _SHA256.fullmatch(prompt_digest) is None
        ):
            raise ValidationError("INVALID_CANDIDATE", "Qwen generator provenance is invalid")
    else:
        raise ValidationError("INVALID_CANDIDATE", "Candidate generator kind is invalid")
    parent = _artifact_ref(
        candidate.get("parent_evaluation_payload"),
        code="INVALID_CANDIDATE",
        message="Candidate parent reference is invalid",
    )
    if not _same_ref(parent, job.baseline_evaluation_payload):
        raise ValidationError("CANDIDATE_PARENT_MISMATCH", "Candidate parent differs from the job")
    proposed = _artifact_ref(
        candidate.get("evaluation_payload"),
        code="INVALID_CANDIDATE",
        message="Candidate payload reference is invalid",
    )
    if proposed.role != "evaluation_payload":
        raise ValidationError("CANDIDATE_PAYLOAD_MISMATCH", "Candidate payload role is invalid")
    return candidate, proposed


def _evaluation_payload(
    resolver: ArtifactResolver,
    reference: ArtifactRef,
    limits: EffectiveLimits,
) -> Mapping[str, object]:
    resolved = resolver.resolve(
        reference,
        ArtifactExpectation(
            role="evaluation_payload",
            media_type="application/json",
            external_schema_id=LINEAR_EVALUATOR_SCHEMA,
            external_schema_version=SCHEMA_VERSION,
        ),
        parse_json=True,
        max_bytes=limits.max_input_bytes,
        max_json_depth=limits.max_json_depth,
        max_string_bytes=limits.max_string_bytes,
    )
    return _as_mapping(
        resolved.json_value,
        code="INVALID_LINEAR_EVALUATOR",
        message="Evaluation payload must be an object",
    )


def _validate_input_inventory(
    job: ResearchJob,
    benchmark: Mapping[str, object],
    candidate_payload: ArtifactRef,
    dataset_manifest: Mapping[str, object],
    limits: EffectiveLimits,
) -> tuple[int, int]:
    references: list[ArtifactRef] = [
        job.dataset_manifest,
        job.benchmark_manifest,
        job.baseline_evaluation_payload,
        job.candidate,
        candidate_payload,
    ]
    references.extend(
        _artifact_ref(
            benchmark.get(field),
            code="BENCHMARK_MANIFEST_INVALID",
            message="Benchmark artifact binding is invalid",
        )
        for field in ("dataset_manifest", "baseline_evaluation_payload")
    )
    raw_sources = dataset_manifest.get("source_artifacts")
    if not isinstance(raw_sources, list):
        raise ValidationError("DATASET_MANIFEST_INVALID", "Dataset source artifacts are invalid")
    references.extend(
        _artifact_ref(
            raw_source,
            code="DATASET_MANIFEST_INVALID",
            message="Dataset source artifact is invalid",
        )
        for raw_source in raw_sources
    )
    splits = _as_mapping(
        dataset_manifest.get("splits"),
        code="DATASET_MANIFEST_INVALID",
        message="Dataset split metadata is invalid",
    )
    for split_name in ("train", "validation", "test"):
        split = _as_mapping(
            splits.get(split_name),
            code="DATASET_MANIFEST_INVALID",
            message="Dataset split metadata is invalid",
        )
        references.append(
            _artifact_ref(
                split.get("artifact"),
                code="DATASET_MANIFEST_INVALID",
                message="Dataset split artifact is invalid",
            )
        )
    unique = {(item.uri, item.sha256, item.size_bytes): item.size_bytes for item in references}
    artifact_count = len(unique)
    total_bytes = sum(unique.values())
    if artifact_count > MAX_ARTIFACTS:
        raise ResourceLimitError(
            "ARTIFACT_COUNT_EXCEEDED", "Experiment inputs exceed the artifact count limit"
        )
    if total_bytes > limits.max_input_bytes:
        raise ResourceLimitError("INPUT_BYTES_EXCEEDED", "Experiment inputs exceed the byte limit")
    return total_bytes, artifact_count


def _evaluate_loaded_job(job: ResearchJob, artifact_root: Path) -> _EvaluationPacket:
    started_wall = time.monotonic_ns()
    started_at = _utc_now()
    job.ensure_not_expired()
    if job.evaluation_split == "test":
        raise ValidationError(
            "TEST_CAPABILITY_REQUIRED",
            "Sealed test evaluation requires an external operator capability",
        )
    with ArtifactResolver(artifact_root) as resolver:
        benchmark, limits = _benchmark_document(job, resolver)
        metrics, minimum_records = _validate_benchmark_bindings(job, benchmark)

        dataset_resolved = resolver.resolve(
            job.dataset_manifest,
            ArtifactExpectation(
                role="dataset_manifest",
                producer_name=PRODUCER_NAME,
                external_schema_id=DATASET_MANIFEST_SCHEMA,
                external_schema_version=SCHEMA_VERSION,
            ),
            parse_json=True,
            max_bytes=limits.max_input_bytes,
            max_json_depth=limits.max_json_depth,
            max_string_bytes=limits.max_string_bytes,
        )
        dataset_manifest = _as_mapping(
            dataset_resolved.json_value,
            code="DATASET_MANIFEST_INVALID",
            message="Dataset manifest must be an object",
        )
        frozen = verify_dataset_manifest(
            dataset_manifest,
            resolver,
            max_records=limits.max_records,
            max_input_bytes=limits.max_input_bytes,
            max_json_depth=limits.max_json_depth,
            max_string_bytes=limits.max_string_bytes,
        )
        for split_name in ("train", "validation", "test"):
            required = minimum_records[split_name]
            observed = frozen.splits[split_name].record_count
            if not isinstance(required, int) or observed < required:
                raise ValidationError(
                    "BENCHMARK_MINIMUM_NOT_MET",
                    "Dataset does not satisfy benchmark minimum record counts",
                )

        candidate, proposed_ref = _candidate_document(job, resolver, limits)
        _validate_input_inventory(job, benchmark, proposed_ref, dataset_manifest, limits)
        baseline_payload = _evaluation_payload(resolver, job.baseline_evaluation_payload, limits)
        proposed_payload = _evaluation_payload(resolver, proposed_ref, limits)
        verify_candidate_change(
            candidate,
            baseline_payload,
            proposed_payload,
            evaluation_schema_id=LINEAR_EVALUATOR_SCHEMA,
        )
        selected = frozen.splits[cast(SplitName, job.evaluation_split)]
        records = parse_scoring_jsonl(
            selected.data,
            max_bytes=limits.max_input_bytes,
            max_records=limits.max_records,
            max_json_depth=limits.max_json_depth,
            max_string_bytes=limits.max_string_bytes,
        )
        canonical_result = evaluate_experiment(
            baseline_payload,
            proposed_payload,
            [record.to_mapping() for record in records],
            metrics,
        )
    job.ensure_not_expired()
    wall_milliseconds = max(0, (time.monotonic_ns() - started_wall) // 1_000_000)
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_rss_bytes = int(usage if platform.system() == "Darwin" else usage * 1024)
    return _EvaluationPacket(
        started_at=started_at,
        wall_milliseconds=wall_milliseconds,
        records_evaluated=len(records),
        peak_rss_bytes=max(0, peak_rss_bytes),
        canonical_result=canonical_result,
    )


def evaluate_job(job_path: Path, artifact_root: Path) -> _EvaluationPacket:
    """Evaluate one stable job file in-process; intended for local verification."""

    return _evaluate_loaded_job(load_job(job_path), artifact_root)


def child_main(job_snapshot: bytes, artifact_root: Path) -> int:
    """Emit one bounded child packet and never expose raw input or host paths."""

    try:
        value = strict_json_loads(job_snapshot, max_bytes=MAX_JOB_BYTES)
        packet: Mapping[str, object] = _evaluate_loaded_job(
            parse_job(value), artifact_root
        ).to_mapping()
        exit_code = 0
    except AtlasResearchError as error:
        packet = {"ok": False, "error": error.as_dict()}
        exit_code = 2
    except Exception:
        packet = {
            "ok": False,
            "error": {"code": "INTERNAL_ERROR", "message": "Worker evaluation failed"},
        }
        exit_code = 3
    sys.stdout.buffer.write(canonical_json_bytes(packet) + b"\n")
    sys.stdout.buffer.flush()
    return exit_code


def preflight_main(job_snapshot: bytes, artifact_root: Path) -> int:
    """Resolve only the pinned benchmark and emit reduced child ceilings."""

    try:
        value = strict_json_loads(job_snapshot, max_bytes=MAX_JOB_BYTES)
        job = parse_job(value)
        job.ensure_not_expired()
        if job.evaluation_split == "test":
            raise ValidationError(
                "TEST_CAPABILITY_REQUIRED",
                "Sealed test evaluation requires an external operator capability",
            )
        with ArtifactResolver(artifact_root) as resolver:
            _, limits = _benchmark_document(job, resolver)
        packet: Mapping[str, object] = {"ok": True, "limits": limits.to_mapping()}
        exit_code = 0
    except AtlasResearchError as error:
        packet = {"ok": False, "error": error.as_dict()}
        exit_code = 2
    except Exception:
        packet = {
            "ok": False,
            "error": {"code": "INTERNAL_ERROR", "message": "Worker preflight failed"},
        }
        exit_code = 3
    sys.stdout.buffer.write(canonical_json_bytes(packet) + b"\n")
    sys.stdout.buffer.flush()
    return exit_code


def _preexec_limits(limits: EffectiveLimits) -> None:
    os.umask(0o077)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(
        resource.RLIMIT_CPU,
        (limits.wall_seconds, limits.wall_seconds + 1),
    )
    resource.setrlimit(
        resource.RLIMIT_FSIZE,
        (limits.max_output_bytes, limits.max_output_bytes),
    )
    resource.setrlimit(
        resource.RLIMIT_NOFILE,
        (limits.max_open_files, limits.max_open_files),
    )
    if platform.system() == "Darwin":
        return
    resource.setrlimit(
        resource.RLIMIT_AS,
        (limits.max_peak_rss_bytes, limits.max_peak_rss_bytes),
    )


def _remaining_timeout(
    job: ResearchJob, limits: EffectiveLimits, admitted_monotonic: float
) -> float:
    deadline_seconds = (job.deadline - datetime.now(UTC)).total_seconds()
    if deadline_seconds <= 0:
        raise ValidationError("JOB_EXPIRED", "job deadline has expired")
    wall_seconds = float(limits.wall_seconds) - (time.monotonic() - admitted_monotonic)
    if wall_seconds <= 0:
        raise ResourceLimitError("WORKER_TIMEOUT", "Experiment exceeded the wall-time limit")
    return min(deadline_seconds, wall_seconds)


def _spawn_child(
    mode: str, artifact_root: Path, limits: EffectiveLimits
) -> subprocess.Popen[bytes]:
    command = [
        sys.executable,
        "-I",
        "-m",
        "atlas_research._worker_child",
        mode,
        str(artifact_root),
    ]
    environment = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "PYTHONHASHSEED": "0",
    }
    return subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=environment,
        close_fds=True,
        start_new_session=True,
        preexec_fn=lambda: _preexec_limits(limits),
    )


def _communicate_child(
    process: subprocess.Popen[bytes],
    *,
    job_snapshot: bytes,
    timeout_seconds: float,
) -> bytes:
    try:
        stdout, _ = process.communicate(input=job_snapshot, timeout=timeout_seconds)
    except subprocess.TimeoutExpired as error:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        raise ResourceLimitError(
            "WORKER_TIMEOUT", "Experiment exceeded the wall-time limit"
        ) from error
    return stdout


def _packet_or_error(stdout: bytes, limits: EffectiveLimits) -> Mapping[str, object]:
    if len(stdout) > min(limits.max_output_bytes, 1 << 20):
        raise ResourceLimitError("WORKER_OUTPUT_EXCEEDED", "Worker output exceeded the limit")
    parsed = strict_json_loads(
        stdout,
        max_bytes=min(limits.max_output_bytes, 1 << 20),
        max_depth=limits.max_json_depth,
        max_string_bytes=limits.max_string_bytes,
    )
    packet = _as_mapping(parsed, code="WORKER_PROTOCOL_INVALID", message="Worker packet is invalid")
    if packet.get("ok") is not True:
        error_mapping = _as_mapping(
            packet.get("error"),
            code="WORKER_PROTOCOL_INVALID",
            message="Worker error packet is invalid",
        )
        code = error_mapping.get("code")
        message = error_mapping.get("message")
        if not isinstance(code, str) or not isinstance(message, str):
            raise ValidationError("WORKER_PROTOCOL_INVALID", "Worker error packet is invalid")
        raise AtlasResearchError(code, message)
    return packet


def _run_preflight(
    job: ResearchJob,
    job_snapshot: bytes,
    artifact_root: Path,
    preliminary_limits: EffectiveLimits,
    admitted_monotonic: float,
) -> EffectiveLimits:
    process = _spawn_child("preflight", artifact_root, preliminary_limits)
    stdout = _communicate_child(
        process,
        job_snapshot=job_snapshot,
        timeout_seconds=_remaining_timeout(job, preliminary_limits, admitted_monotonic),
    )
    packet = _packet_or_error(stdout, preliminary_limits)
    if process.returncode != 0 or set(packet) != {"ok", "limits"}:
        raise ValidationError("WORKER_PROTOCOL_INVALID", "Worker preflight packet is invalid")
    limit_mapping = _as_mapping(
        packet.get("limits"),
        code="WORKER_PROTOCOL_INVALID",
        message="Worker preflight limits are invalid",
    )
    try:
        reported = EffectiveLimits.from_mapping(limit_mapping)
    except AtlasResearchError as error:
        raise ValidationError(
            "WORKER_PROTOCOL_INVALID", "Worker preflight limits are invalid"
        ) from error
    return effective_limits(job.limits, reported)


def _run_child(
    job: ResearchJob,
    job_snapshot: bytes,
    artifact_root: Path,
    limits: EffectiveLimits,
    admitted_monotonic: float,
) -> _EvaluationPacket:
    process = _spawn_child("evaluate", artifact_root, limits)
    stdout = _communicate_child(
        process,
        job_snapshot=job_snapshot,
        timeout_seconds=_remaining_timeout(job, limits, admitted_monotonic),
    )
    packet = _packet_or_error(stdout, limits)
    if process.returncode != 0 or set(packet) != {
        "ok",
        "started_at",
        "wall_milliseconds",
        "records_evaluated",
        "peak_rss_bytes",
        "canonical_result",
    }:
        raise ValidationError("WORKER_PROTOCOL_INVALID", "Worker packet is invalid")
    for field in ("wall_milliseconds", "records_evaluated", "peak_rss_bytes"):
        value = packet.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValidationError("WORKER_PROTOCOL_INVALID", "Worker packet is invalid")
    started_at = packet.get("started_at")
    canonical_result = packet.get("canonical_result")
    if not isinstance(started_at, str) or not isinstance(canonical_result, Mapping):
        raise ValidationError("WORKER_PROTOCOL_INVALID", "Worker packet is invalid")
    parsed_started_at = parse_timestamp(started_at, field="worker.started_at")
    if parsed_started_at < job.created_at or parsed_started_at > job.deadline:
        raise ValidationError(
            "WORKER_PROTOCOL_INVALID", "Worker timestamp is outside the job window"
        )
    return _EvaluationPacket(
        started_at=started_at,
        wall_milliseconds=cast(int, packet["wall_milliseconds"]),
        records_evaluated=cast(int, packet["records_evaluated"]),
        peak_rss_bytes=cast(int, packet["peak_rss_bytes"]),
        canonical_result=cast(CanonicalResult, dict(canonical_result)),
    )


def _git_commit() -> str:
    repository = Path(__file__).resolve().parents[2]
    try:
        completed = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=3,
            env={"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin:/usr/local/bin"},
        )
        cleanliness = subprocess.run(
            [
                "git",
                "-C",
                str(repository),
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ],
            check=True,
            capture_output=True,
            timeout=3,
            env={"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin:/usr/local/bin"},
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ValidationError(
            "PROVENANCE_UNAVAILABLE", "Git commit provenance is unavailable"
        ) from error
    commit = completed.stdout.strip()
    if not _COMMIT.fullmatch(commit):
        raise ValidationError("PROVENANCE_UNAVAILABLE", "Git commit provenance is unavailable")
    if cleanliness.stdout:
        raise ValidationError(
            "PROVENANCE_DIRTY", "Source tree is dirty; exact code provenance is unavailable"
        )
    return commit


def _source_provenance() -> SourceProvenance:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(_INSTALLED_PROVENANCE, flags)
    except FileNotFoundError:
        return SourceProvenance(
            git_commit=_git_commit(), revision_kind="verified_checkout"
        ).normalized()
    except OSError as error:
        raise ValidationError(
            "PROVENANCE_UNAVAILABLE", "Installed source provenance is unavailable"
        ) from error
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != 0
            or stat.S_IMODE(metadata.st_mode) & 0o022
            or metadata.st_size > 256
        ):
            raise ValidationError("PROVENANCE_UNAVAILABLE", "Installed source provenance is unsafe")
        data = os.read(descriptor, 257)
    finally:
        os.close(descriptor)
    try:
        text = data.decode("ascii")
    except UnicodeDecodeError as error:
        raise ValidationError(
            "PROVENANCE_UNAVAILABLE", "Installed source provenance is invalid"
        ) from error
    lines = text.splitlines()
    if len(lines) != 2:
        raise ValidationError("PROVENANCE_UNAVAILABLE", "Installed source provenance is invalid")
    return SourceProvenance(
        git_commit=lines[0],
        revision_kind="declared_wheel_revision",
        source_artifact_sha256=lines[1],
    ).normalized()


def _receipt_document(
    job: ResearchJob,
    packet: _EvaluationPacket,
    identity: WorkerIdentity,
    *,
    previous_sha256: str | None,
    created_at: str,
    source_provenance: SourceProvenance,
) -> dict[str, object]:
    suffix = hashlib.sha256(f"{job.spec_sha256}:{job.attempt}".encode("ascii")).hexdigest()[:32]
    canonical_result = dict(packet.canonical_result)
    provenance: dict[str, object] = {
        "atlas_research_version": __version__,
        "git_commit": source_provenance.git_commit,
        "source_revision_kind": source_provenance.revision_kind,
        "python_version": platform.python_version(),
        "platform": f"{platform.system()} {platform.machine()}",
        "worker_id": identity.worker_id,
        "worker_session_id": identity.session_id,
    }
    if source_provenance.source_artifact_sha256 is not None:
        provenance["source_artifact_sha256"] = source_provenance.source_artifact_sha256
    return {
        "schema_version": SCHEMA_VERSION,
        "receipt_id": f"receipt-{suffix}",
        "previous_receipt_sha256": previous_sha256,
        "created_at": created_at,
        "started_at": packet.started_at,
        "finished_at": created_at,
        "experiment_id": f"experiment-{suffix}",
        "job_id": job.job_id,
        "attempt": job.attempt,
        "idempotency_key": job.idempotency_key,
        "job_spec_sha256": job.spec_sha256,
        "canonical_result_sha256": canonical_result_sha256(canonical_result),
        "dataset_manifest": job.dataset_manifest.to_mapping(),
        "benchmark_manifest": job.benchmark_manifest.to_mapping(),
        "baseline_evaluation_payload": job.baseline_evaluation_payload.to_mapping(),
        "candidate": job.candidate.to_mapping(),
        "evaluation_split": job.evaluation_split,
        "canonical_result": canonical_result,
        "resource_usage": {
            "wall_milliseconds": packet.wall_milliseconds,
            "records_evaluated": packet.records_evaluated,
            "peak_rss_bytes": packet.peak_rss_bytes,
        },
        "provenance": provenance,
    }


def _result_document(
    job: ResearchJob,
    identity: WorkerIdentity,
    *,
    status: str,
    started_at: str,
    finished_at: str,
    receipt: ArtifactRef | None = None,
    error: AtlasResearchError | None = None,
    retryable: bool = False,
) -> dict[str, object]:
    result: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "task": "research.experiment",
        "job_id": job.job_id,
        "attempt": job.attempt,
        "idempotency_key": job.idempotency_key,
        "job_spec_sha256": job.spec_sha256,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "worker": {
            "worker_id": identity.worker_id,
            "session_id": identity.session_id,
            "version": __version__,
        },
        "artifacts": [],
    }
    if receipt is not None:
        result["receipt"] = receipt.to_mapping()
    elif error is not None:
        result["error"] = {**error.as_dict(), "retryable": retryable}
    return result


def _validate_result_document(document: Mapping[str, object], job: ResearchJob) -> None:
    required = {
        "schema_version",
        "task",
        "job_id",
        "attempt",
        "idempotency_key",
        "job_spec_sha256",
        "status",
        "started_at",
        "finished_at",
        "worker",
        "artifacts",
    }
    status = document.get("status")
    conditional = {"receipt"} if status == "completed" else {"error"}
    if set(document) != required | conditional:
        raise ConflictError("RESULT_CONFLICT", "Existing result fields are invalid")
    if (
        document.get("schema_version") != SCHEMA_VERSION
        or document.get("task") != "research.experiment"
        or document.get("job_id") != job.job_id
        or document.get("attempt") != job.attempt
        or document.get("idempotency_key") != job.idempotency_key
        or document.get("job_spec_sha256") != job.spec_sha256
        or status not in {"completed", "rejected", "cancelled", "expired"}
    ):
        raise ConflictError("RESULT_CONFLICT", "Existing result does not match the job")
    try:
        started_at = parse_timestamp(document.get("started_at"), field="result.started_at")
        finished_at = parse_timestamp(document.get("finished_at"), field="result.finished_at")
    except AtlasResearchError as error:
        raise ConflictError("RESULT_CONFLICT", "Existing result timestamps are invalid") from error
    if started_at > finished_at or started_at < job.created_at:
        raise ConflictError("RESULT_CONFLICT", "Existing result timestamp order is invalid")
    if status == "completed" and finished_at > job.deadline:
        raise ConflictError("RESULT_CONFLICT", "Completed result exceeded the deadline")
    worker = _as_mapping(
        document.get("worker"),
        code="RESULT_CONFLICT",
        message="Existing result worker is invalid",
    )
    if set(worker) != {"worker_id", "session_id", "version"}:
        raise ConflictError("RESULT_CONFLICT", "Existing result worker is invalid")
    worker_id = worker.get("worker_id")
    session_id = worker.get("session_id")
    version = worker.get("version")
    if (
        not isinstance(worker_id, str)
        or _IDENTIFIER.fullmatch(worker_id) is None
        or not isinstance(session_id, str)
        or _IDENTIFIER.fullmatch(session_id) is None
        or not isinstance(version, str)
        or not 1 <= len(version) <= 64
    ):
        raise ConflictError("RESULT_CONFLICT", "Existing result worker is invalid")
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) > 64:
        raise ConflictError("RESULT_CONFLICT", "Existing result artifacts are invalid")
    try:
        parsed_artifacts = [
            ArtifactRef.from_mapping(
                _as_mapping(
                    value,
                    code="RESULT_CONFLICT",
                    message="Existing result artifact is invalid",
                )
            )
            for value in artifacts
        ]
    except AtlasResearchError as error:
        raise ConflictError("RESULT_CONFLICT", "Existing result artifact is invalid") from error
    if any(reference.role == "experiment_receipt" for reference in parsed_artifacts):
        raise ConflictError("RESULT_CONFLICT", "Existing result artifacts are invalid")
    if status == "completed":
        try:
            receipt = ArtifactRef.from_mapping(
                _as_mapping(
                    document.get("receipt"),
                    code="RESULT_CONFLICT",
                    message="Existing result receipt is invalid",
                )
            )
        except AtlasResearchError as artifact_error:
            raise ConflictError(
                "RESULT_CONFLICT", "Existing result receipt is invalid"
            ) from artifact_error
        if receipt.role != "experiment_receipt":
            raise ConflictError("RESULT_CONFLICT", "Existing result receipt is invalid")
        return
    error_document = _as_mapping(
        document.get("error"),
        code="RESULT_CONFLICT",
        message="Existing result error is invalid",
    )
    if set(error_document) != {"code", "message", "retryable"}:
        raise ConflictError("RESULT_CONFLICT", "Existing result error is invalid")
    code = error_document.get("code")
    message = error_document.get("message")
    if (
        not isinstance(code, str)
        or _ERROR_CODE.fullmatch(code) is None
        or not isinstance(message, str)
        or not 1 <= len(message.encode("utf-8")) <= 2_000
        or not isinstance(error_document.get("retryable"), bool)
    ):
        raise ConflictError("RESULT_CONFLICT", "Existing result error is invalid")


def _receipt_ref(commit: ReceiptCommit, output_root: Path) -> ArtifactRef:
    try:
        uri = commit.path.relative_to(output_root).as_posix()
    except ValueError as error:
        raise ValidationError(
            "RECEIPT_PATH_INVALID", "Receipt is outside the output root"
        ) from error
    return build_artifact_ref(
        uri=uri,
        role="experiment_receipt",
        media_type="application/vnd.atlas-research.experiment-receipt+json",
        data=commit.data,
        external_schema_id=RECEIPT_SCHEMA,
        external_schema_version=SCHEMA_VERSION,
    )


def _receipt_job_identity(
    stored: Mapping[str, object], job: ResearchJob
) -> tuple[str, str, str, str]:
    validate_receipt_document(stored)
    provenance = _as_mapping(
        stored.get("provenance"),
        code="RECEIPT_INVALID",
        message="Stored receipt is invalid",
    )
    started_at = stored.get("started_at")
    finished_at = stored.get("finished_at")
    worker_id = provenance.get("worker_id")
    session_id = provenance.get("worker_session_id")
    if (
        not isinstance(started_at, str)
        or not isinstance(finished_at, str)
        or not isinstance(worker_id, str)
        or not isinstance(session_id, str)
    ):
        raise ValidationError("RECEIPT_INVALID", "Stored receipt is invalid")
    if (
        stored.get("job_id") != job.job_id
        or stored.get("attempt") != job.attempt
        or stored.get("idempotency_key") != job.idempotency_key
        or stored.get("job_spec_sha256") != job.spec_sha256
        or stored.get("dataset_manifest") != job.dataset_manifest.to_mapping()
        or stored.get("benchmark_manifest") != job.benchmark_manifest.to_mapping()
        or stored.get("baseline_evaluation_payload") != job.baseline_evaluation_payload.to_mapping()
        or stored.get("candidate") != job.candidate.to_mapping()
        or stored.get("evaluation_split") != job.evaluation_split
    ):
        raise ConflictError(
            "RECEIPT_JOB_CONFLICT", "Stored receipt does not match the admitted job"
        )
    return started_at, finished_at, worker_id, session_id


@contextmanager
def _single_worker(root: Path) -> Iterator[None]:
    path = root / ".worker.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as error:
        raise ConflictError("WORKER_LOCK_FAILED", "Worker lock could not be opened") from error
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) & 0o077
        ):
            raise ConflictError("WORKER_LOCK_UNSAFE", "Worker lock is not a private regular file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ConflictError("WORKER_BUSY", "The bounded worker is already running") from error
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _safe_output_relative(value: str, *, field: str) -> str:
    if _SAFE_RELATIVE.fullmatch(value) is None or any(
        part in {".", ".."} for part in value.split("/")
    ):
        raise ValidationError("OUTPUT_PATH_INVALID", f"{field} must be a safe relative path")
    return value


def _read_existing_result(root: Path, result_uri: str) -> tuple[Path, Mapping[str, object]] | None:
    try:
        stored = read_private_bytes(root, result_uri, max_bytes=1 << 20)
    except (OSError, ValidationError) as error:
        raise ConflictError("RESULT_CONFLICT", "Existing result cannot be replayed") from error
    if stored is None:
        return None
    path, data = stored
    existing = _as_mapping(
        strict_json_loads(data),
        code="RESULT_CONFLICT",
        message="Existing result is invalid",
    )
    if canonical_json_bytes(existing) + b"\n" != data:
        raise ConflictError("RESULT_CONFLICT", "Existing result is not canonical")
    return path, existing


def _write_result(
    root: Path,
    result_uri: str,
    result: Mapping[str, object],
) -> WorkerOutcome:
    try:
        path = write_canonical_json_private(root, result_uri, result)
        return WorkerOutcome(result=dict(result), path=path, replayed=False)
    except ConflictError:
        stored = _read_existing_result(root, result_uri)
        if stored is None:  # pragma: no cover - raced external deletion
            raise ConflictError("RESULT_CONFLICT", "Existing result disappeared") from None
        path, existing = stored
        if canonical_sha256(existing) != canonical_sha256(result):
            raise ConflictError(
                "RESULT_CONFLICT", "Existing result differs from this execution"
            ) from None
        return WorkerOutcome(result=dict(existing), path=path, replayed=True)


def run_isolated_job(
    *,
    job_path: Path,
    artifact_root: Path,
    output_root: Path,
    result_uri: str,
    receipt_dir: str = "receipt-log",
    identity: WorkerIdentity | None = None,
) -> WorkerOutcome:
    """Run one offline job under hard ceilings and commit one immutable result."""

    normalized_identity = (identity or WorkerIdentity()).normalized()
    result_uri = _safe_output_relative(result_uri, field="result_uri")
    receipt_dir = _safe_output_relative(receipt_dir, field="receipt_dir")
    if "/" in receipt_dir:
        raise ValidationError(
            "OUTPUT_PATH_INVALID", "receipt_dir must be one private directory name"
        )
    private_root = ensure_private_directory(output_root)
    job = load_job(job_path)
    job_snapshot = canonical_json_bytes(job.raw) + b"\n"
    preliminary_limits = reduce_limits(job.limits)
    started_at = _utc_now()
    with _single_worker(private_root):
        prior_result = _read_existing_result(private_root, result_uri)
        if prior_result is not None:
            path, document = prior_result
            _validate_result_document(document, job)
            if document.get("status") == "completed":
                receipt_log = ReceiptLog(private_root / receipt_dir)
                committed = receipt_log.find(job.idempotency_key, job.spec_sha256)
                if committed is None:
                    raise ConflictError(
                        "RESULT_CONFLICT", "Completed result receipt is unavailable"
                    )
                stored_receipt = _as_mapping(
                    strict_json_loads(committed.data),
                    code="RECEIPT_INVALID",
                    message="Stored receipt is invalid",
                )
                _, _, receipt_worker, receipt_session = _receipt_job_identity(stored_receipt, job)
                result_worker = _as_mapping(
                    document.get("worker"),
                    code="RESULT_CONFLICT",
                    message="Existing result worker is invalid",
                )
                expected_receipt = _receipt_ref(committed, private_root).to_mapping()
                if (
                    document.get("receipt") != expected_receipt
                    or result_worker.get("worker_id") != receipt_worker
                    or result_worker.get("session_id") != receipt_session
                ):
                    raise ConflictError(
                        "RESULT_CONFLICT", "Completed result receipt binding is invalid"
                    )
            return WorkerOutcome(result=dict(document), path=path, replayed=True)

        receipt_log = ReceiptLog(private_root / receipt_dir)
        existing = receipt_log.find(job.idempotency_key, job.spec_sha256)
        if existing is not None:
            receipt = _receipt_ref(existing, private_root)
            stored = _as_mapping(
                strict_json_loads(existing.data),
                code="RECEIPT_INVALID",
                message="Stored receipt is invalid",
            )
            stored_started_at, stored_finished_at, worker_id, session_id = _receipt_job_identity(
                stored, job
            )
            result = _result_document(
                job,
                WorkerIdentity(worker_id=worker_id, session_id=session_id).normalized(),
                status="completed",
                started_at=stored_started_at,
                finished_at=stored_finished_at,
                receipt=receipt,
            )
            return _write_result(private_root, result_uri, result)

        job.ensure_not_expired()
        try:
            admitted_monotonic = time.monotonic()
            limits = _run_preflight(
                job,
                job_snapshot,
                artifact_root,
                preliminary_limits,
                admitted_monotonic,
            )
            packet = _run_child(
                job,
                job_snapshot,
                artifact_root,
                limits,
                admitted_monotonic,
            )
            job.ensure_not_expired()
            source_provenance = _source_provenance()
            previous_sha256 = receipt_log.head_sha256
            created_at = _utc_now()
            job.ensure_not_expired(now=datetime.fromisoformat(created_at.replace("Z", "+00:00")))
            receipt_document = _receipt_document(
                job,
                packet,
                normalized_identity,
                previous_sha256=previous_sha256,
                created_at=created_at,
                source_provenance=source_provenance,
            )
            commit = receipt_log.commit(receipt_document, not_after=job.deadline)
            receipt = _receipt_ref(commit, private_root)
            result = _result_document(
                job,
                normalized_identity,
                status="completed",
                started_at=packet.started_at,
                finished_at=created_at,
                receipt=receipt,
            )
        except AtlasResearchError as error:
            if error.code == "RECEIPT_HEAD_WRITE_FAILED":
                # A durable completed receipt exists. Do not publish a
                # contradictory immutable result; explicit HEAD recovery can
                # make the same result URI reconstruct the completed outcome.
                raise
            finished_at = _utc_now()
            expired = datetime.now(UTC) > job.deadline or error.code == "JOB_EXPIRED"
            status = "expired" if expired else "rejected"
            if not expired and error.code in {"WORKER_TIMEOUT", "WORKER_OUTPUT_EXCEEDED"}:
                status = "cancelled"
            result = _result_document(
                job,
                normalized_identity,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                error=error,
                retryable=error.code
                in {"WORKER_TIMEOUT", "WORKER_OUTPUT_EXCEEDED", "INTERNAL_ERROR"},
            )
        return _write_result(private_root, result_uri, result)


__all__ = [
    "SourceProvenance",
    "WorkerIdentity",
    "WorkerOutcome",
    "child_main",
    "evaluate_job",
    "run_isolated_job",
]

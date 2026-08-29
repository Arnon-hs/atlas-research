# SPDX-License-Identifier: MIT
from __future__ import annotations

import copy
import json
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

import atlas_research.worker as worker
from atlas_research.artifacts import (
    ArtifactRef,
    atomic_write_private,
    build_artifact_ref,
    ensure_private_directory,
    write_canonical_json_private,
)
from atlas_research.canonical import canonical_json_bytes, strict_json_loads
from atlas_research.constants import (
    BENCHMARK_MANIFEST_SCHEMA,
    CANDIDATE_SCHEMA,
    DATASET_MANIFEST_SCHEMA,
    LINEAR_EVALUATOR_SCHEMA,
    SCHEMA_VERSION,
    SCORING_EXAMPLE_SCHEMA,
)
from atlas_research.dataset import SplitName, build_dataset_manifest, freeze_records
from atlas_research.errors import ConflictError, ResourceLimitError, ValidationError
from atlas_research.job import load_job
from atlas_research.receipts import ReceiptError, ReceiptLog
from atlas_research.worker import (
    SourceProvenance,
    WorkerIdentity,
    evaluate_job,
    run_isolated_job,
)


@dataclass(frozen=True, slots=True)
class _Graph:
    artifact_root: Path
    job_path: Path
    job: dict[str, object]
    proposed_path: Path
    proposed_payload: dict[str, object]


def _timestamp(value: datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _limits() -> dict[str, int]:
    return {
        "wall_seconds": 30,
        "max_records": 100,
        "max_input_bytes": 1 << 20,
        "max_output_bytes": 1 << 20,
        "max_workspace_bytes": 1 << 20,
        "max_peak_rss_bytes": 1 << 30,
        "max_open_files": 64,
        "max_json_depth": 16,
        "max_string_bytes": 4_096,
    }


def _write_artifact(
    root: Path,
    *,
    uri: str,
    role: str,
    media_type: str,
    value: object,
    schema_id: str,
) -> tuple[Path, ArtifactRef]:
    data = canonical_json_bytes(value) + b"\n"
    path = atomic_write_private(root, uri, data)
    return path, build_artifact_ref(
        uri=uri,
        role=role,
        media_type=media_type,
        data=data,
        external_schema_id=schema_id,
        external_schema_version=SCHEMA_VERSION,
    )


def _build_graph(tmp_path: Path, *, candidate_weight: int) -> _Graph:
    if candidate_weight not in {0, 2}:
        raise ValueError("fixture supports only the KEEP and DISCARD candidates")
    artifact_root = ensure_private_directory(tmp_path / "artifacts")
    now = datetime.now(UTC).replace(microsecond=0)
    created_at = _timestamp(now - timedelta(minutes=1))
    deadline = _timestamp(now + timedelta(minutes=30))
    decision_name = "keep" if candidate_weight == 2 else "discard"

    records = [
        {"id": "repo-000", "features": {"quality": 10}, "label": 20},
        {"id": "repo-004", "features": {"quality": 10}, "label": 20},
        {"id": "repo-007", "features": {"quality": 10}, "label": 20},
    ]
    dataset = freeze_records(records, seed=7)
    assert {name: split.record_count for name, split in dataset.splits.items()} == {
        "train": 1,
        "validation": 1,
        "test": 1,
    }

    source_data = b"".join(dataset.splits[name].data for name in ("train", "validation", "test"))
    atomic_write_private(artifact_root, "source.jsonl", source_data)
    source_ref = build_artifact_ref(
        uri="source.jsonl",
        role="source_export",
        media_type="application/x-ndjson",
        data=source_data,
        producer_name="fixture",
        producer_version="1.0.0",
        external_schema_id="urn:atlasrepo:test:source:v1",
        external_schema_version=SCHEMA_VERSION,
    )

    split_refs: dict[SplitName, ArtifactRef] = {}
    for name, split in dataset.splits.items():
        uri = f"{name}.jsonl"
        atomic_write_private(artifact_root, uri, split.data)
        split_refs[name] = build_artifact_ref(
            uri=uri,
            role="dataset_split",
            media_type="application/x-ndjson",
            data=split.data,
            external_schema_id=SCORING_EXAMPLE_SCHEMA,
            external_schema_version=SCHEMA_VERSION,
        )

    dataset_manifest = build_dataset_manifest(
        dataset,
        dataset_id="small-dataset",
        created_at=created_at,
        source_artifacts=[source_ref],
        split_artifacts=split_refs,
    )
    dataset_data = (
        json.dumps(
            dataset_manifest,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    atomic_write_private(artifact_root, "dataset.json", dataset_data)
    dataset_ref = build_artifact_ref(
        uri="dataset.json",
        role="dataset_manifest",
        media_type="application/vnd.atlas-research.dataset-manifest+json",
        data=dataset_data,
        external_schema_id=DATASET_MANIFEST_SCHEMA,
        external_schema_version=SCHEMA_VERSION,
    )

    baseline_payload: dict[str, object] = {
        "schema": LINEAR_EVALUATOR_SCHEMA,
        "bias": 0,
        "weights": {"quality": 1},
    }
    _, baseline_ref = _write_artifact(
        artifact_root,
        uri="baseline.json",
        role="evaluation_payload",
        media_type="application/json",
        value=baseline_payload,
        schema_id=LINEAR_EVALUATOR_SCHEMA,
    )
    proposed_payload: dict[str, object] = {
        "schema": LINEAR_EVALUATOR_SCHEMA,
        "bias": 0,
        "weights": {"quality": candidate_weight},
    }
    proposed_path, proposed_ref = _write_artifact(
        artifact_root,
        uri=f"candidate-payload-{decision_name}.json",
        role="evaluation_payload",
        media_type="application/json",
        value=proposed_payload,
        schema_id=LINEAR_EVALUATOR_SCHEMA,
    )

    candidate = {
        "schema_version": SCHEMA_VERSION,
        "candidate_id": f"candidate-{decision_name}",
        "created_at": created_at,
        "status": "proposed",
        "parent_evaluation_payload": baseline_ref.to_mapping(),
        "research_level": "LEVEL_1",
        "hypothesis": "Change one bounded synthetic quality weight.",
        "changed_variable": {
            "path": "weights.quality",
            "old_value": 1,
            "new_value": candidate_weight,
        },
        "evaluation_payload": proposed_ref.to_mapping(),
        "target_contract": {
            "id": "urn:atlasrepo:scout:scoring-definition",
            "version": SCHEMA_VERSION,
        },
        "generator": {"kind": "human"},
    }
    _, candidate_ref = _write_artifact(
        artifact_root,
        uri=f"candidate-{decision_name}.json",
        role="candidate",
        media_type="application/vnd.atlas-research.candidate+json",
        value=candidate,
        schema_id=CANDIDATE_SCHEMA,
    )

    benchmark = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_id": f"benchmark-{decision_name}",
        "created_at": created_at,
        "dataset_manifest": dataset_ref.to_mapping(),
        "baseline_evaluation_payload": baseline_ref.to_mapping(),
        "evaluation_split": "validation",
        "metrics": {
            "mae": {
                "direction": "lower",
                "gate": {"absolute_threshold": 0, "minimum_delta": 0},
            }
        },
        "minimum_records": {"train": 1, "validation": 1, "test": 1},
        "limits": _limits(),
    }
    _, benchmark_ref = _write_artifact(
        artifact_root,
        uri=f"benchmark-{decision_name}.json",
        role="benchmark_manifest",
        media_type="application/vnd.atlas-research.benchmark-manifest+json",
        value=benchmark,
        schema_id=BENCHMARK_MANIFEST_SCHEMA,
    )

    job: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "task": "research.experiment",
        "job_id": f"job-{decision_name}",
        "attempt": 1,
        "idempotency_key": f"experiment-key-{decision_name}",
        "created_at": created_at,
        "deadline": deadline,
        "dataset_manifest": dataset_ref.to_mapping(),
        "benchmark_manifest": benchmark_ref.to_mapping(),
        "baseline_evaluation_payload": baseline_ref.to_mapping(),
        "candidate": candidate_ref.to_mapping(),
        "evaluation_split": "validation",
        "limits": _limits(),
    }
    job_path = write_canonical_json_private(artifact_root, f"job-{decision_name}.json", job)
    return _Graph(artifact_root, job_path, job, proposed_path, proposed_payload)


@pytest.mark.parametrize(("candidate_weight", "decision"), [(2, "KEEP"), (0, "DISCARD")])
def test_evaluate_job_over_complete_small_artifact_graph(
    tmp_path: Path,
    candidate_weight: int,
    decision: str,
) -> None:
    graph = _build_graph(tmp_path, candidate_weight=candidate_weight)

    packet = evaluate_job(graph.job_path, graph.artifact_root)

    assert packet.records_evaluated == 1
    assert packet.canonical_result["decision"] == decision
    assert packet.canonical_result["all_gates_passed"] is (decision == "KEEP")
    assert packet.canonical_result["metrics"]["mae"]["passed"] is (decision == "KEEP")


def test_isolated_run_commits_private_result_receipt_and_replays_exactly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    output_root = tmp_path / "output"
    output_root.mkdir(mode=0o700)
    output_root.chmod(0o700)
    monkeypatch.setattr(
        "atlas_research.worker._source_provenance",
        lambda: SourceProvenance("a" * 40, "verified_checkout"),
    )
    identity = WorkerIdentity(worker_id="worker-test", session_id="session-test")

    first = run_isolated_job(
        job_path=graph.job_path,
        artifact_root=graph.artifact_root,
        output_root=output_root,
        result_uri="result.json",
        identity=identity,
    )
    result_before = first.path.read_bytes()
    receipt_mapping = cast(Mapping[str, object], first.result["receipt"])
    receipt_path = output_root / cast(str, receipt_mapping["uri"])
    receipt_before = receipt_path.read_bytes()

    assert first.result["status"] == "completed"
    assert first.replayed is False
    assert canonical_json_bytes(first.result) + b"\n" == result_before
    assert stat.S_IMODE(output_root.stat().st_mode) == 0o700
    assert stat.S_IMODE(first.path.stat().st_mode) == 0o600
    assert stat.S_IMODE(receipt_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(receipt_path.parent.stat().st_mode) == 0o700
    verification = ReceiptLog(output_root / "receipt-log").verify()
    assert verification.entry_count == 1
    assert verification.head_sha256 is not None
    receipt = cast(Mapping[str, object], strict_json_loads(receipt_before))
    canonical_result = cast(Mapping[str, object], receipt["canonical_result"])
    assert canonical_result["decision"] == "KEEP"

    replay = run_isolated_job(
        job_path=graph.job_path,
        artifact_root=graph.artifact_root,
        output_root=output_root,
        result_uri="result.json",
        identity=WorkerIdentity(worker_id="another-worker", session_id="another-session"),
    )

    assert replay.replayed is True
    assert replay.path.read_bytes() == result_before
    assert receipt_path.read_bytes() == receipt_before
    assert ReceiptLog(output_root / "receipt-log").verify().entry_count == 1


def test_exact_terminal_result_replays_without_new_deadline_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    output_root = ensure_private_directory(tmp_path / "output")
    monkeypatch.setattr(
        worker,
        "_source_provenance",
        lambda: SourceProvenance("a" * 40, "verified_checkout"),
    )
    first = run_isolated_job(
        job_path=graph.job_path,
        artifact_root=graph.artifact_root,
        output_root=output_root,
        result_uri="result.json",
    )

    def reject_new_admission(*, now: datetime | None = None) -> None:
        del now
        raise ValidationError("JOB_EXPIRED", "job deadline has expired")

    monkeypatch.setattr(worker.ResearchJob, "ensure_not_expired", reject_new_admission)
    replay = run_isolated_job(
        job_path=graph.job_path,
        artifact_root=graph.artifact_root,
        output_root=output_root,
        result_uri="result.json",
    )

    assert replay.replayed is True
    assert replay.path.read_bytes() == first.path.read_bytes()


def test_binding_and_artifact_tamper_fail_closed_without_receipt(tmp_path: Path) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    bad_job = copy.deepcopy(graph.job)
    bad_dataset_ref = cast(dict[str, object], bad_job["dataset_manifest"])
    bad_dataset_ref["sha256"] = "f" * 64
    bad_job_path = write_canonical_json_private(
        graph.artifact_root, "job-bad-binding.json", bad_job
    )
    output_root = tmp_path / "rejected-output"
    output_root.mkdir(mode=0o700)
    output_root.chmod(0o700)

    rejected = run_isolated_job(
        job_path=bad_job_path,
        artifact_root=graph.artifact_root,
        output_root=output_root,
        result_uri="rejected.json",
        identity=WorkerIdentity(worker_id="worker-test", session_id="session-test"),
    )

    assert rejected.result["status"] == "rejected"
    assert "receipt" not in rejected.result
    error = cast(Mapping[str, object], rejected.result["error"])
    assert error["code"] == "JOB_DATASET_MISMATCH"
    assert list((output_root / "receipt-log" / "entries").iterdir()) == []

    replay = run_isolated_job(
        job_path=bad_job_path,
        artifact_root=graph.artifact_root,
        output_root=output_root,
        result_uri="rejected.json",
        identity=WorkerIdentity(worker_id="other-worker", session_id="other-session"),
    )
    assert replay.replayed is True
    assert replay.path.read_bytes() == rejected.path.read_bytes()

    tampered = {
        **graph.proposed_payload,
        "weights": {"quality": 3},
    }
    tampered_bytes = canonical_json_bytes(tampered) + b"\n"
    assert len(tampered_bytes) == graph.proposed_path.stat().st_size
    graph.proposed_path.write_bytes(tampered_bytes)
    graph.proposed_path.chmod(0o600)
    with pytest.raises(ValidationError) as raised:
        evaluate_job(graph.job_path, graph.artifact_root)
    assert raised.value.code == "ARTIFACT_DIGEST_MISMATCH"


def test_combined_input_inventory_enforces_union_bytes_and_artifact_count(
    tmp_path: Path,
) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    job = load_job(graph.job_path)
    dataset = cast(
        dict[str, object], strict_json_loads((graph.artifact_root / "dataset.json").read_bytes())
    )
    benchmark = cast(
        dict[str, object],
        strict_json_loads((graph.artifact_root / "benchmark-keep.json").read_bytes()),
    )
    candidate = cast(
        Mapping[str, object],
        strict_json_loads((graph.artifact_root / "candidate-keep.json").read_bytes()),
    )
    proposed = ArtifactRef.from_mapping(cast(Mapping[str, object], candidate["evaluation_payload"]))
    splits = cast(Mapping[str, object], dataset["splits"])
    split_bytes = 0
    for raw_split in splits.values():
        split = cast(Mapping[str, object], raw_split)
        artifact = cast(Mapping[str, object], split["artifact"])
        split_bytes += cast(int, artifact["size_bytes"])
    sources = cast(list[object], dataset["source_artifacts"])
    oversized_source = cast(dict[str, object], copy.deepcopy(sources[0]))
    oversized_source["size_bytes"] = job.limits.max_input_bytes - split_bytes
    dataset["source_artifacts"] = [oversized_source]

    with pytest.raises(ResourceLimitError) as byte_error:
        worker._validate_input_inventory(job, benchmark, proposed, dataset, job.limits)
    assert byte_error.value.code == "INPUT_BYTES_EXCEEDED"

    many_sources: list[dict[str, object]] = []
    for index in range(64):
        source = cast(dict[str, object], copy.deepcopy(sources[0]))
        source["uri"] = f"source-{index:03d}.jsonl"
        source["sha256"] = f"{index:064x}"
        source["size_bytes"] = 1
        many_sources.append(source)
    dataset["source_artifacts"] = many_sources

    with pytest.raises(ResourceLimitError) as count_error:
        worker._validate_input_inventory(job, benchmark, proposed, dataset, job.limits)
    assert count_error.value.code == "ARTIFACT_COUNT_EXCEEDED"


def test_invalid_job_is_rejected_before_receipt_log_exists(tmp_path: Path) -> None:
    artifact_root = ensure_private_directory(tmp_path / "artifacts")
    job_path = write_canonical_json_private(artifact_root, "invalid-job.json", {})
    output_root = tmp_path / "output"
    output_root.mkdir(mode=0o700)
    output_root.chmod(0o700)

    with pytest.raises(ValidationError):
        run_isolated_job(
            job_path=job_path,
            artifact_root=artifact_root,
            output_root=output_root,
            result_uri="result.json",
        )

    assert not (output_root / "receipt-log").exists()
    assert not (output_root / "result.json").exists()


def test_parent_snapshot_survives_job_path_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    output_root = ensure_private_directory(tmp_path / "output")
    original_preflight = worker._run_preflight

    def swap_then_preflight(*args: object, **kwargs: object) -> object:
        graph.job_path.write_bytes(b"{}\n")
        graph.job_path.chmod(0o600)
        return original_preflight(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(worker, "_run_preflight", swap_then_preflight)
    monkeypatch.setattr(
        worker,
        "_source_provenance",
        lambda: SourceProvenance("a" * 40, "verified_checkout"),
    )

    outcome = run_isolated_job(
        job_path=graph.job_path,
        artifact_root=graph.artifact_root,
        output_root=output_root,
        result_uri="result.json",
    )

    assert outcome.result["status"] == "completed"


def test_future_job_is_rejected_before_preflight_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    future_job = copy.deepcopy(graph.job)
    now = datetime.now(UTC).replace(microsecond=0)
    future_job["created_at"] = _timestamp(now + timedelta(hours=1))
    future_job["deadline"] = _timestamp(now + timedelta(hours=2))
    path = write_canonical_json_private(graph.artifact_root, "future-job.json", future_job)

    def unexpected_preflight(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("future job reached preflight")

    monkeypatch.setattr(worker, "_run_preflight", unexpected_preflight)

    with pytest.raises(ValidationError) as captured:
        run_isolated_job(
            job_path=path,
            artifact_root=graph.artifact_root,
            output_root=ensure_private_directory(tmp_path / "output"),
            result_uri="result.json",
        )

    assert captured.value.code == "JOB_NOT_YET_VALID"


def test_sealed_test_is_rejected_without_external_operator_capability(tmp_path: Path) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    test_job = copy.deepcopy(graph.job)
    test_job["evaluation_split"] = "test"
    test_job["review_authorization"] = {
        "reviewer": "reviewer-test",
        "approved_at": test_job["created_at"],
        "reason": "Exercise the sealed-test fail-closed boundary.",
    }
    path = write_canonical_json_private(graph.artifact_root, "job-test.json", test_job)

    with pytest.raises(ValidationError) as captured:
        evaluate_job(path, graph.artifact_root)

    assert captured.value.code == "TEST_CAPABILITY_REQUIRED"


def test_receipt_directory_cannot_escape_private_output(
    tmp_path: Path,
) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    output_root = ensure_private_directory(tmp_path / "output")
    outside = ensure_private_directory(tmp_path / "outside")
    (output_root / "link").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValidationError, match="receipt_dir"):
        run_isolated_job(
            job_path=graph.job_path,
            artifact_root=graph.artifact_root,
            output_root=output_root,
            result_uri="result.json",
            receipt_dir="link/nested",
        )
    with pytest.raises(ReceiptError):
        run_isolated_job(
            job_path=graph.job_path,
            artifact_root=graph.artifact_root,
            output_root=output_root,
            result_uri="result.json",
            receipt_dir="link",
        )

    assert list(outside.iterdir()) == []


def test_forged_existing_result_is_never_accepted_as_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    output_root = ensure_private_directory(tmp_path / "output")
    forged = {
        "schema_version": "1.0.0",
        "task": "research.experiment",
        "job_id": "job-keep",
        "attempt": 1,
        "idempotency_key": "experiment-key-keep",
        "job_spec_sha256": "f" * 64,
        "status": "completed",
        "worker": "forged",
        "artifacts": ["not-an-artifact"],
        "production": "active",
    }
    write_canonical_json_private(output_root, "result.json", forged)
    monkeypatch.setattr(
        worker,
        "_source_provenance",
        lambda: SourceProvenance("a" * 40, "verified_checkout"),
    )

    with pytest.raises(ConflictError) as captured:
        run_isolated_job(
            job_path=graph.job_path,
            artifact_root=graph.artifact_root,
            output_root=output_root,
            result_uri="result.json",
        )

    assert captured.value.code == "RESULT_CONFLICT"


def test_deadline_is_rechecked_after_provenance_and_chain_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    output_root = ensure_private_directory(tmp_path / "output")
    now = datetime.now(UTC).replace(microsecond=0)
    future = _timestamp(now + timedelta(hours=1))
    parent_times = iter((_timestamp(now), future, future))
    monkeypatch.setattr(worker, "_utc_now", lambda: next(parent_times))
    monkeypatch.setattr(
        worker,
        "_source_provenance",
        lambda: SourceProvenance("a" * 40, "verified_checkout"),
    )

    outcome = run_isolated_job(
        job_path=graph.job_path,
        artifact_root=graph.artifact_root,
        output_root=output_root,
        result_uri="result.json",
    )

    assert outcome.result["status"] == "expired"
    assert list((output_root / "receipt-log" / "entries").iterdir()) == []


def test_head_update_failure_requires_recovery_without_rejected_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    output_root = ensure_private_directory(tmp_path / "output")
    monkeypatch.setattr(
        worker,
        "_source_provenance",
        lambda: SourceProvenance("a" * 40, "verified_checkout"),
    )

    with monkeypatch.context() as context:
        context.setattr(
            worker.ReceiptLog,
            "_write_head",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("simulated head fault")),
        )
        with pytest.raises(ReceiptError) as captured:
            run_isolated_job(
                job_path=graph.job_path,
                artifact_root=graph.artifact_root,
                output_root=output_root,
                result_uri="result.json",
            )

    assert captured.value.code == "RECEIPT_HEAD_WRITE_FAILED"
    assert not (output_root / "result.json").exists()
    recovered = ReceiptLog(output_root / "receipt-log").verify(recover=True)
    assert recovered.recovered is True
    assert recovered.entry_count == 1

    completed = run_isolated_job(
        job_path=graph.job_path,
        artifact_root=graph.artifact_root,
        output_root=output_root,
        result_uri="result.json",
    )

    assert completed.result["status"] == "completed"
    assert completed.replayed is False


def test_precreated_result_fifo_fails_without_blocking(tmp_path: Path) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    output_root = ensure_private_directory(tmp_path / "output")
    os.mkfifo(output_root / "result.json", mode=0o600)

    with pytest.raises(ConflictError) as captured:
        run_isolated_job(
            job_path=graph.job_path,
            artifact_root=graph.artifact_root,
            output_root=output_root,
            result_uri="result.json",
        )

    assert captured.value.code == "RESULT_CONFLICT"


def test_existing_result_reader_rejects_symlinked_parent(tmp_path: Path) -> None:
    graph = _build_graph(tmp_path, candidate_weight=2)
    output_root = ensure_private_directory(tmp_path / "output")
    outside = ensure_private_directory(tmp_path / "outside")
    write_canonical_json_private(outside, "result.json", {"forged": True})
    (output_root / "link").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ConflictError) as captured:
        run_isolated_job(
            job_path=graph.job_path,
            artifact_root=graph.artifact_root,
            output_root=output_root,
            result_uri="link/result.json",
        )

    assert captured.value.code == "RESULT_CONFLICT"

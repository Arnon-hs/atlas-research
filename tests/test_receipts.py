# SPDX-License-Identifier: MIT
from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from atlas_research.receipts import (
    ReceiptChainError,
    ReceiptConflictError,
    ReceiptError,
    ReceiptLog,
    ReceiptValidationError,
    canonical_result_sha256,
)


def _artifact(role: str) -> dict[str, object]:
    contracts = {
        "dataset_manifest": (
            "application/vnd.atlas-research.dataset-manifest+json",
            "urn:atlasrepo:atlas-research:schema:v1:dataset-manifest",
        ),
        "benchmark_manifest": (
            "application/vnd.atlas-research.benchmark-manifest+json",
            "urn:atlasrepo:atlas-research:schema:v1:benchmark-manifest",
        ),
        "evaluation_payload": (
            "application/json",
            "urn:atlasrepo:atlas-research:fixture:v1:linear-evaluator",
        ),
        "candidate": (
            "application/vnd.atlas-research.candidate+json",
            "urn:atlasrepo:atlas-research:schema:v1:candidate-artifact",
        ),
    }
    media_type, schema_id = contracts[role]
    return {
        "uri": f"{role}.json",
        "role": role,
        "media_type": media_type,
        "sha256": hashlib.sha256(role.encode()).hexdigest(),
        "size_bytes": 1,
        "producer": {"name": "atlas-research", "version": "0.1.0"},
        "external_schema": {"id": schema_id, "version": "1.0.0"},
    }


def _result(*, candidate: str = "0.4") -> dict[str, object]:
    return {
        "metrics": {
            "mae": {
                "baseline": "0.5",
                "candidate": candidate,
                "candidate_minus_baseline": "-0.1",
                "passed": True,
            }
        },
        "all_gates_passed": True,
        "decision": "KEEP",
        "reason_codes": ["ALL_GATES_PASSED"],
    }


def _receipt(
    receipt_id: str,
    key: str,
    *,
    previous: str | None,
    job_digest: str = "a" * 64,
    result: dict[str, object] | None = None,
) -> dict[str, object]:
    canonical_result = result if result is not None else _result()
    return {
        "schema_version": "1.0.0",
        "receipt_id": receipt_id,
        "previous_receipt_sha256": previous,
        "created_at": "2026-08-30T00:00:01Z",
        "started_at": "2026-08-30T00:00:00Z",
        "finished_at": "2026-08-30T00:00:01Z",
        "experiment_id": f"experiment-{receipt_id}",
        "job_id": f"job-{receipt_id}",
        "attempt": 1,
        "idempotency_key": key,
        "job_spec_sha256": job_digest,
        "canonical_result_sha256": canonical_result_sha256(canonical_result),
        "dataset_manifest": _artifact("dataset_manifest"),
        "benchmark_manifest": _artifact("benchmark_manifest"),
        "baseline_evaluation_payload": _artifact("evaluation_payload"),
        "candidate": _artifact("candidate"),
        "evaluation_split": "validation",
        "canonical_result": canonical_result,
        "resource_usage": {
            "wall_milliseconds": 1,
            "records_evaluated": 1,
            "peak_rss_bytes": 1,
        },
        "provenance": {
            "atlas_research_version": "0.1.0",
            "git_commit": "1" * 40,
            "source_revision_kind": "verified_checkout",
            "python_version": "3.11.0",
            "platform": "Test arm64",
            "worker_id": "worker-test",
            "worker_session_id": "session-test",
        },
    }


def test_commit_verify_chain_and_exact_replay(tmp_path: Path) -> None:
    log = ReceiptLog(tmp_path / "receipts")
    first_document = _receipt("receipt-1", "idempotency-key-0001", previous=None)
    first = log.commit(first_document)
    second_document = _receipt(
        "receipt-2",
        "idempotency-key-0002",
        previous=first.sha256,
        job_digest="b" * 64,
    )
    second = log.commit(second_document)

    verification = log.verify()
    replay = log.commit(first_document)

    assert verification.entry_count == 2
    assert verification.head_sha256 == second.sha256
    assert not verification.recovered
    assert replay.replayed
    assert replay.data == first.data
    assert replay.path == first.path
    assert hashlib.sha256(replay.data).hexdigest() == first.sha256
    assert stat.S_IMODE(first.path.stat().st_mode) == 0o600
    assert stat.S_IMODE((log.root / "HEAD").stat().st_mode) == 0o600


def test_same_key_different_job_digest_conflicts(tmp_path: Path) -> None:
    log = ReceiptLog(tmp_path / "receipts")
    original = _receipt("receipt-1", "idempotency-key-0001", previous=None)
    log.commit(original)
    conflicting = _receipt(
        "receipt-1",
        "idempotency-key-0001",
        previous=None,
        job_digest="b" * 64,
    )

    with pytest.raises(ReceiptConflictError) as captured:
        log.commit(conflicting)

    assert captured.value.code == "RECEIPT_JOB_CONFLICT"


def test_find_returns_existing_binding_and_detects_job_conflict(tmp_path: Path) -> None:
    log = ReceiptLog(tmp_path / "receipts")
    committed = log.commit(_receipt("receipt-1", "idempotency-key-0001", previous=None))

    found = log.find("idempotency-key-0001", "a" * 64)

    assert found is not None
    assert found.replayed
    assert found.data == committed.data
    assert found.sha256 == committed.sha256
    assert log.find("idempotency-key-missing", "c" * 64) is None
    with pytest.raises(ReceiptConflictError) as captured:
        log.find("idempotency-key-0001", "b" * 64)
    assert captured.value.code == "RECEIPT_JOB_CONFLICT"


def test_same_key_same_job_different_bytes_conflicts(tmp_path: Path) -> None:
    log = ReceiptLog(tmp_path / "receipts")
    original = _receipt("receipt-1", "idempotency-key-0001", previous=None)
    log.commit(original)
    changed = _receipt("receipt-other", "idempotency-key-0001", previous=None)

    with pytest.raises(ReceiptConflictError) as captured:
        log.commit(changed)

    assert captured.value.code == "RECEIPT_REPLAY_CONFLICT"


def test_stale_previous_digest_is_rejected(tmp_path: Path) -> None:
    log = ReceiptLog(tmp_path / "receipts")
    log.commit(_receipt("receipt-1", "idempotency-key-0001", previous=None))

    with pytest.raises(ReceiptChainError) as captured:
        log.commit(
            _receipt(
                "receipt-2",
                "idempotency-key-0002",
                previous="f" * 64,
                job_digest="b" * 64,
            )
        )

    assert captured.value.code == "RECEIPT_STALE_HEAD"


def test_verify_can_recover_missing_head_without_rewriting_entry(tmp_path: Path) -> None:
    log = ReceiptLog(tmp_path / "receipts")
    committed = log.commit(_receipt("receipt-1", "idempotency-key-0001", previous=None))
    before = committed.path.read_bytes()
    (log.root / "HEAD").unlink()

    with pytest.raises(ReceiptChainError):
        log.verify()
    recovered = log.verify(recover=True)

    assert recovered.recovered
    assert recovered.head_sha256 == committed.sha256
    assert committed.path.read_bytes() == before
    assert log.verify().head_sha256 == committed.sha256


def test_partial_entry_write_never_publishes_a_final_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log = ReceiptLog(tmp_path / "receipts")
    original_write = os.write
    writes = 0

    def fail_after_partial(descriptor: int, data: bytes | memoryview) -> int:
        nonlocal writes
        writes += 1
        if writes == 1:
            return original_write(descriptor, data[:1])
        raise OSError("simulated disk fault")

    monkeypatch.setattr("atlas_research.artifacts.os.write", fail_after_partial)

    with pytest.raises(ReceiptError) as captured:
        log.commit(_receipt("receipt-1", "idempotency-key-0001", previous=None))

    assert captured.value.code == "RECEIPT_WRITE_FAILED"
    assert list(log.entries_dir.iterdir()) == []


def test_verify_detects_noncanonical_or_tampered_entry(tmp_path: Path) -> None:
    log = ReceiptLog(tmp_path / "receipts")
    committed = log.commit(_receipt("receipt-1", "idempotency-key-0001", previous=None))
    document = json.loads(committed.data)
    document["canonical_result"]["reason_codes"] = ["TAMPERED"]
    committed.path.write_text(json.dumps(document), encoding="utf-8")
    committed.path.chmod(0o600)

    with pytest.raises((ReceiptChainError, ReceiptValidationError)):
        log.verify(recover=True)


def test_canonical_result_rejects_json_numbers() -> None:
    with pytest.raises(ReceiptValidationError) as captured:
        canonical_result_sha256({"metric": 0.5})

    assert captured.value.code == "RECEIPT_RESULT_NUMBER_FORBIDDEN"


def test_canonical_result_uses_utf16_property_order() -> None:
    value = {"\U0001f600": "astral", "\ufffd": "replacement"}
    expected = '{"😀":"astral","�":"replacement"}'.encode()

    assert canonical_result_sha256(value) == hashlib.sha256(expected).hexdigest()


def test_result_digest_is_recomputed_before_commit(tmp_path: Path) -> None:
    log = ReceiptLog(tmp_path / "receipts")
    invalid = _receipt("receipt-1", "idempotency-key-0001", previous=None)
    invalid["canonical_result_sha256"] = "0" * 64

    with pytest.raises(ReceiptValidationError) as captured:
        log.commit(invalid)

    assert captured.value.code == "RECEIPT_RESULT_DIGEST_MISMATCH"
    assert not list((log.root / "entries").iterdir())


def test_storage_rejects_hardlinked_entry(tmp_path: Path) -> None:
    log = ReceiptLog(tmp_path / "receipts")
    committed = log.commit(_receipt("receipt-1", "idempotency-key-0001", previous=None))
    committed.path.with_suffix(".link").hardlink_to(committed.path)

    with pytest.raises(ReceiptError) as captured:
        log.verify()

    assert captured.value.code == "RECEIPT_UNSAFE_ENTRY"


@pytest.mark.parametrize("mutation", ["extra", "false_keep", "wrong_role", "wheel_without_digest"])
def test_closed_receipt_semantics_reject_forged_evidence(tmp_path: Path, mutation: str) -> None:
    log = ReceiptLog(tmp_path / "receipts")
    invalid = _receipt("receipt-1", "idempotency-key-0001", previous=None)
    if mutation == "extra":
        invalid["production"] = "active"
    elif mutation == "false_keep":
        result = invalid["canonical_result"]
        assert isinstance(result, dict)
        metrics = result["metrics"]
        assert isinstance(metrics, dict)
        metric = metrics["mae"]
        assert isinstance(metric, dict)
        metric["passed"] = False
        invalid["canonical_result_sha256"] = canonical_result_sha256(result)
    elif mutation == "wrong_role":
        invalid["candidate"] = _artifact("dataset_manifest")
    else:
        provenance = invalid["provenance"]
        assert isinstance(provenance, dict)
        provenance["source_revision_kind"] = "declared_wheel_revision"

    with pytest.raises(ReceiptValidationError):
        log.commit(invalid)

    assert list((log.root / "entries").iterdir()) == []


def test_commit_deadline_fails_before_durable_entry(tmp_path: Path) -> None:
    log = ReceiptLog(tmp_path / "receipts")

    with pytest.raises(ReceiptValidationError) as captured:
        log.commit(
            _receipt("receipt-1", "idempotency-key-0001", previous=None),
            not_after=datetime.now(UTC) - timedelta(seconds=1),
        )

    assert captured.value.code == "RECEIPT_DEADLINE_EXPIRED"
    assert log.verified_receipts() == ()


def test_receipt_fifo_fails_without_blocking(tmp_path: Path) -> None:
    log = ReceiptLog(tmp_path / "receipts")
    os.mkfifo(log.entries_dir / "0000000000000001-receipt-1.json", mode=0o600)

    with pytest.raises(ReceiptError) as captured:
        log.verify()

    assert captured.value.code == "RECEIPT_UNSAFE_ENTRY"

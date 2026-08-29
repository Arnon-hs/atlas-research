# SPDX-License-Identifier: MIT
from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pytest

import atlas_research.worker as worker
from atlas_research.canonical import strict_json_loads
from atlas_research.job import load_job
from atlas_research.receipts import ReceiptLog
from atlas_research.worker import SourceProvenance, WorkerIdentity, evaluate_job, run_isolated_job


def test_committed_fixture_is_digest_pinned_and_runs_one_shot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).parents[1] / "examples" / "fixture-v1"
    manifest = cast(
        Mapping[str, object], strict_json_loads((root / "bundle-manifest.json").read_bytes())
    )
    files = cast(Mapping[str, object], manifest["files"])
    for relative, raw_reference in files.items():
        reference = cast(Mapping[str, object], raw_reference)
        data = (root / relative).read_bytes()
        assert len(data) == reference["size_bytes"]
        assert hashlib.sha256(data).hexdigest() == reference["sha256"]

    job = load_job(root / cast(str, manifest["job"]))
    assert job.spec_sha256 == manifest["job_spec_sha256"]
    packet = evaluate_job(root / "job.json", root)
    expected = cast(Mapping[str, object], manifest["expected"])
    assert packet.records_evaluated == expected["records_evaluated"]
    assert packet.canonical_result["decision"] == expected["decision"]

    monkeypatch.setattr(
        worker,
        "_source_provenance",
        lambda: SourceProvenance("a" * 40, "verified_checkout"),
    )
    output_root = tmp_path / "output"
    output_root.mkdir(mode=0o700)
    outcome = run_isolated_job(
        job_path=root / "job.json",
        artifact_root=root,
        output_root=output_root,
        result_uri="result.json",
        identity=WorkerIdentity(worker_id="fixture-test", session_id="fixture-session"),
    )
    assert outcome.result["status"] == expected["status"]
    receipts = ReceiptLog(output_root / "receipt-log").verified_receipts()
    assert len(receipts) == 1
    assert receipts[0]["canonical_result"] == packet.canonical_result

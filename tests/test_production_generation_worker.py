# SPDX-License-Identifier: MIT
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from atlas_research.canonical import canonical_json_bytes
from atlas_research.errors import ValidationError
from atlas_research.production_generation import (
    DESCRIPTION_OUTPUT_SCHEMA,
    DESCRIPTION_WORKLOAD,
    JOB_CONTRACT,
)
from atlas_research.production_generation_worker import (
    ProductionGenerationCacheIdentity,
    ProductionGenerationClaim,
    ProductionGenerationClient,
    ProductionGenerationHeartbeat,
    ProductionGenerationSession,
    ProductionGenerationWorker,
    ProductionGenerationWorkerConfig,
    _assignment_sha256,
    _parse_claim,
    _parse_receipt,
)
from atlas_research.qwen import QWEN_MODEL, QwenHTTPResponse
from atlas_research.remote_worker import RemoteWorkerError


@dataclass
class FakeQwen:
    description: str = "A bounded production description."
    calls: list[str] = field(default_factory=list)

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None,
        *,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> QwenHTTPResponse:
        del method, body, timeout_seconds, max_response_bytes
        self.calls.append(path)
        value: dict[str, object]
        if path == "/api/tags":
            value = {"models": [{"name": QWEN_MODEL, "digest": "a" * 64}]}
        else:
            value = {
                "model": QWEN_MODEL,
                "done": True,
                "response": json.dumps({"description": self.description}),
            }
        return QwenHTTPResponse(200, "application/json", json.dumps(value).encode())


def _config(tmp_path: Path) -> ProductionGenerationWorkerConfig:
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    token = tmp_path / "token"
    token.write_text("a" * 32, encoding="ascii")
    token.chmod(0o600)
    return ProductionGenerationWorkerConfig(
        controller_url="https://scout.example.test",
        worker_id="atlasrepo-generation",
        release_id=f"pgr_release_{'e' * 32}",
        model_revision="a" * 64,
        enrollment_token_file=token,
        state_root=state,
        poll_seconds=0.1,
        request_timeout_seconds=1,
    )


def _job() -> dict[str, object]:
    source = '{"current_description":"","source_urls":[],"title":"Example"}'
    return {
        "contract_version": JOB_CONTRACT,
        "job_id": "generation-job-1",
        "idempotency_key": "generation-key-0001",
        "workload_type": DESCRIPTION_WORKLOAD,
        "priority": "high",
        "target": {"entity_type": "repo", "entity_id": "repo-1", "expected_version": "7"},
        "input": {
            "source_text": source,
            "source_sha256": hashlib.sha256(source.encode()).hexdigest(),
            "language": "en",
        },
        "requirements": {
            "prompt_template_version": "atlas-content-description-prompt.v1",
            "prompt_guard_policy_version": "atlas-prompt-guard.v1",
            "generation_policy_version": "atlas-content-description-generation.v1",
            "output_schema_version": DESCRIPTION_OUTPUT_SCHEMA,
            "timeout_seconds": 60,
            "max_output_bytes": 8192,
        },
    }


def _claim(config: ProductionGenerationWorkerConfig) -> ProductionGenerationClaim:
    raw_job = _job()
    assignment = _assignment_sha256(
        raw_job,
        config,
        attempt=1,
        fence=4,
        cancellation_generation=0,
    )
    value = {
        "protocol_version": "1",
        "job": raw_job,
        "worker_id": config.worker_id,
        "release_id": config.release_id,
        "session_id": "session-1",
        "attempt": 1,
        "fence": 4,
        "cancellation_generation": 0,
        "lease_expires_at": (datetime.now(UTC) + timedelta(minutes=2))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "assignment_sha256": assignment,
    }
    return _parse_claim(value, config)


class FakeClient:
    def __init__(
        self,
        config: ProductionGenerationWorkerConfig,
        claim: ProductionGenerationClaim | None,
        *,
        terminal_error: bool = False,
        receipt_kind: str | None = None,
    ) -> None:
        self.config = config
        self.next_claim = claim
        self.terminal_error = terminal_error
        self.receipt_kind = receipt_kind
        self.completed: list[tuple[dict[str, object], str]] = []
        self.failed: list[tuple[str, bool]] = []
        self.heartbeats: list[int] = []

    def exchange_session(self) -> ProductionGenerationSession:
        return ProductionGenerationSession(
            session_id="session-1",
            token="s" * 32,
            expires_at=datetime.now(UTC) + timedelta(minutes=10),
            lease_seconds=120,
            heartbeat_interval_seconds=10,
        )

    def claim(self, _session: ProductionGenerationSession) -> ProductionGenerationClaim | None:
        return self.next_claim

    def heartbeat(
        self,
        _session: ProductionGenerationSession,
        _claim: ProductionGenerationClaim,
        sequence: int,
    ) -> ProductionGenerationHeartbeat:
        self.heartbeats.append(sequence)
        return ProductionGenerationHeartbeat(False, datetime.now(UTC) + timedelta(minutes=2))

    def complete(
        self,
        _session: ProductionGenerationSession,
        _claim: ProductionGenerationClaim,
        result: dict[str, object],
        digest: str,
    ) -> None:
        self.completed.append((result, digest))
        if self.terminal_error:
            raise RemoteWorkerError(
                "GENERATION_WORKER_TERMINAL_AMBIGUOUS", "Fixture terminal is ambiguous"
            )

    def fail(
        self,
        _session: ProductionGenerationSession,
        _claim: ProductionGenerationClaim,
        *,
        code: str,
        retryable: bool,
    ) -> None:
        self.failed.append((code, retryable))

    def receipt(
        self,
        _session: ProductionGenerationSession,
        identity: ProductionGenerationCacheIdentity,
    ) -> dict[str, object] | None:
        if self.receipt_kind is None:
            return None
        terminal_kind = self.receipt_kind
        without_digest: dict[str, object] = {
            "contract_version": "atlas-production-generation-terminal-receipt.v1",
            "job_id": identity.job_id,
            "workload_type": identity.workload_type,
            "target": dict(identity.target),
            "terminal_kind": terminal_kind,
            "worker_id": self.config.worker_id,
            "release_id": self.config.release_id,
            "session_id": "session-old",
            "attempt": identity.attempt,
            "fence": identity.fence,
            "cancellation_generation": identity.cancellation_generation,
            "assignment_sha256": identity.assignment_sha256,
            "request_sha256": "f" * 64,
            "result_sha256": identity.result_sha256,
            "failure_code": None,
            "failure_retryable": None,
            "terminal_at": "2026-09-03T12:00:02.000Z",
        }
        receipt = {
            **without_digest,
            "receipt_sha256": hashlib.sha256(canonical_json_bytes(without_digest)).hexdigest(),
        }
        return dict(_parse_receipt(receipt, self.config, identity))


def test_worker_completes_exact_fenced_generation_and_removes_cache(tmp_path: Path) -> None:
    config = _config(tmp_path)
    claim = _claim(config)
    client = FakeClient(config, claim)
    qwen = FakeQwen()
    worker = ProductionGenerationWorker(
        config,
        client=cast(ProductionGenerationClient, client),
        qwen_transport=qwen,
    )

    outcome = worker.run_once()

    assert outcome.state == "completed"
    assert outcome.job_id == claim.job.job_id
    assert client.heartbeats == [1]
    assert len(client.completed) == 1
    result, digest = client.completed[0]
    assert (
        hashlib.sha256(
            json.dumps(result, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()
        == digest
    )
    assert list(worker.results_root.iterdir()) == []


def test_ambiguous_terminal_replays_cached_bytes_without_qwen(tmp_path: Path) -> None:
    config = _config(tmp_path)
    claim = _claim(config)
    first_client = FakeClient(config, claim, terminal_error=True)
    first_qwen = FakeQwen()
    first = ProductionGenerationWorker(
        config,
        client=cast(ProductionGenerationClient, first_client),
        qwen_transport=first_qwen,
    )

    with pytest.raises(RemoteWorkerError, match="TERMINAL_AMBIGUOUS"):
        first.run_once()

    cached = list(first.results_root.iterdir())
    assert len(cached) == 1
    second_client = FakeClient(config, claim)
    second_qwen = FakeQwen(description="This value must never be generated.")
    second = ProductionGenerationWorker(
        config,
        client=cast(ProductionGenerationClient, second_client),
        qwen_transport=second_qwen,
    )
    outcome = second.run_once()

    assert outcome.state == "completed"
    assert second_qwen.calls == []
    assert second_client.completed[0] == first_client.completed[0]
    assert list(second.results_root.iterdir()) == []


def test_restart_reconciles_committed_receipt_without_claim_or_qwen(tmp_path: Path) -> None:
    config = _config(tmp_path)
    claim = _claim(config)
    first_client = FakeClient(config, claim, terminal_error=True)
    first = ProductionGenerationWorker(
        config,
        client=cast(ProductionGenerationClient, first_client),
        qwen_transport=FakeQwen(),
    )
    with pytest.raises(RemoteWorkerError, match="TERMINAL_AMBIGUOUS"):
        first.run_once()

    second_client = FakeClient(config, None, receipt_kind="completed")
    second_qwen = FakeQwen(description="This value must never be generated.")
    second = ProductionGenerationWorker(
        config,
        client=cast(ProductionGenerationClient, second_client),
        qwen_transport=second_qwen,
    )

    outcome = second.run_once()

    assert outcome.state == "idle"
    assert second_qwen.calls == []
    assert second_client.completed == []
    assert list(second.results_root.iterdir()) == []


def test_assignment_digest_and_model_output_fail_closed(tmp_path: Path) -> None:
    config = _config(tmp_path)
    raw_job = _job()
    claim_value = {
        "protocol_version": "1",
        "job": raw_job,
        "worker_id": config.worker_id,
        "release_id": config.release_id,
        "session_id": "session-1",
        "attempt": 1,
        "fence": 4,
        "cancellation_generation": 0,
        "lease_expires_at": "2026-09-03T12:00:00Z",
        "assignment_sha256": "0" * 64,
    }
    with pytest.raises(ValidationError, match="assignment identity"):
        _parse_claim(claim_value, config)

    claim = _claim(config)
    client = FakeClient(config, claim)
    worker = ProductionGenerationWorker(
        config,
        client=cast(ProductionGenerationClient, client),
        qwen_transport=FakeQwen(description="<script>unsafe</script>"),
    )
    outcome = worker.run_once()
    assert outcome.state == "failed"
    assert client.failed == [("GENERATION_OUTPUT_INVALID", False)]


def test_worker_rejects_model_revision_drift_before_generation(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config = ProductionGenerationWorkerConfig(
        controller_url=config.controller_url,
        worker_id=config.worker_id,
        release_id=config.release_id,
        model_revision="b" * 64,
        enrollment_token_file=config.enrollment_token_file,
        state_root=config.state_root,
        poll_seconds=config.poll_seconds,
        request_timeout_seconds=config.request_timeout_seconds,
    )
    claim = _claim(config)
    client = FakeClient(config, claim)
    qwen = FakeQwen()

    outcome = ProductionGenerationWorker(
        config,
        client=cast(ProductionGenerationClient, client),
        qwen_transport=qwen,
    ).run_once()

    assert outcome.state == "failed"
    assert client.failed == [("QWEN_MODEL_REVISION_MISMATCH", False)]
    assert qwen.calls == ["/api/tags"]

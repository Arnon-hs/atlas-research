# SPDX-License-Identifier: MIT
from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import socket
import ssl
import stat
import threading
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar, cast

import pytest

import atlas_research.remote_worker as remote_worker_module
from atlas_research.canonical import canonical_json_bytes, canonical_sha256, strict_json_loads
from atlas_research.constants import MAX_ARTIFACT_BYTES, MAX_TOTAL_INPUT_BYTES, SCHEMA_VERSION
from atlas_research.errors import ResourceLimitError, ValidationError
from atlas_research.operations_telemetry import (
    ScoutTelemetry,
    TelemetryCommitAmbiguousError,
    parse_scout_telemetry,
)
from atlas_research.remote_worker import (
    PROTOCOL_VERSION,
    HeartbeatReply,
    RemoteArtifact,
    RemoteClaim,
    RemoteWorker,
    RemoteWorkerError,
    ScoutWorkerClient,
    WorkerConfig,
    WorkerSession,
    _claim_from_mapping,
    _controller_origin,
    _duplicate_interrupt_socket,
    _read_secret_file,
    _resolved_addresses,
    _session_from_mapping,
    _TelemetryPublisher,
    load_worker_config,
)

ENROLLMENT_TOKEN = "enrollment_" + ("e" * 32)
SESSION_TOKEN = "session_" + ("s" * 32)
SESSION_ID = "session-1234567890"
JOB_OBJECT_ID = "123e4567-e89b-12d3-a456-426614174000"
DATA_OBJECT_ID = "123e4567-e89b-42d3-a456-426614174001"
FIXTURE_ROOT = Path(__file__).parents[1] / "examples" / "fixture-v1"
STORAGE_EXHAUSTION_ERRNOS = tuple(sorted({errno.ENOSPC, getattr(errno, "EDQUOT", errno.ENOSPC)}))


def _future(seconds: int = 600) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=seconds)


def _timestamp(value: datetime | None = None) -> str:
    return (value or _future()).isoformat(timespec="seconds").replace("+00:00", "Z")


def _telemetry_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _scout_telemetry(
    value: datetime | None = None,
    *,
    pending: int = 7,
    in_flight: int = 1,
    queue_failed: int = 2,
    processed: int = 3,
    failed: int = 1,
) -> ScoutTelemetry:
    collected_at = (value or datetime.now(UTC)).replace(microsecond=123_000)
    minute = collected_at.replace(second=0, microsecond=0)
    history: list[dict[str, object]] = []
    if processed or failed:
        history.append(
            {"at": _telemetry_timestamp(minute), "processed": processed, "failed": failed}
        )
    return parse_scout_telemetry(
        {
            "protocol_version": PROTOCOL_VERSION,
            "collected_at": _telemetry_timestamp(collected_at),
            "queue": {
                "pending": pending,
                "in_flight": in_flight,
                "failed": queue_failed,
            },
            "totals": {"processed": processed, "failed": failed},
            "history": history,
        },
        now=datetime.now(UTC),
    )


def _write_private(path: Path, data: str, mode: int = 0o600) -> Path:
    path.write_text(data, encoding="utf-8")
    path.chmod(mode)
    return path


def _fixture_job(*, attempt: int = 1) -> bytes:
    value = cast(dict[str, object], strict_json_loads((FIXTURE_ROOT / "job.json").read_bytes()))
    value["attempt"] = attempt
    return canonical_json_bytes(value) + b"\n"


def _fixture_result(
    job_data: bytes,
    *,
    worker_id: str = "mac-mini-test",
    session_id: str = SESSION_ID,
) -> dict[str, object]:
    job = cast(dict[str, object], strict_json_loads(job_data))
    return {
        "schema_version": SCHEMA_VERSION,
        "task": "research.experiment",
        "job_id": job["job_id"],
        "attempt": job["attempt"],
        "idempotency_key": job["idempotency_key"],
        "job_spec_sha256": canonical_sha256(job),
        "status": "cancelled",
        "started_at": "2026-08-30T00:00:00Z",
        "finished_at": "2026-08-30T00:00:01Z",
        "worker": {"worker_id": worker_id, "session_id": session_id, "version": "test"},
        "artifacts": [],
        "error": {"code": "FIXTURE_CANCELLED", "message": "Fixture result", "retryable": False},
    }


def _executor(
    path: Path,
    *,
    result: Mapping[str, object] | None = None,
    sleep: float = 0,
    capture_path: Path | None = None,
) -> Path:
    pause = f"sleep {sleep}\n" if sleep else ""
    capture = ""
    if capture_path is not None:
        assert "'" not in str(capture_path)
        capture = f"printf '%s\\n' \"$@\" > '{capture_path}'\n"
    result_line = ""
    if result is not None:
        encoded = canonical_json_bytes(result).decode("utf-8")
        assert "'" not in encoded
        result_line = (
            f"printf '%s\\n' '{encoded}' > \"$run_root/output/result.json\"\n"
            'chmod 600 "$run_root/output/result.json"\n'
        )
    path.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        f"{capture}"
        "run_root=\n"
        'while [ "$#" -gt 0 ]; do\n'
        '  case "$1" in --run-root) run_root=$2; shift 2 ;; *) shift ;; esac\n'
        "done\n"
        f"{pause}"
        f"{result_line}",
        encoding="utf-8",
    )
    path.chmod(0o700)
    return path


def _config(
    tmp_path: Path,
    *,
    executor: Path | None = None,
    telemetry: bool = False,
) -> WorkerConfig:
    token = _write_private(tmp_path / "enrollment.token", f"{ENROLLMENT_TOKEN}\n")
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    executable = executor or _executor(tmp_path / "executor")
    mapping: dict[str, object] = {
        "protocol_version": PROTOCOL_VERSION,
        "controller_url": "http://127.0.0.1:8123",
        "worker_id": "mac-mini-test",
        "enrollment_token_file": str(token),
        "state_root": str(state),
        "executor_path": str(executable),
        "poll_seconds": 0.1,
        "request_timeout_seconds": 2,
        "max_job_seconds": 10,
        "max_bundle_bytes": 1 << 20,
    }
    if telemetry:
        mapping["telemetry_file"] = str(state / "worker-telemetry.json")
    return WorkerConfig.from_mapping(mapping)


def _artifact(
    path: str, data: bytes, object_id: str = JOB_OBJECT_ID
) -> tuple[RemoteArtifact, bytes]:
    return (
        RemoteArtifact(
            path=path,
            sha256=hashlib.sha256(data).hexdigest(),
            size_bytes=len(data),
            download_path=f"/api/worker/v1/objects/{object_id}",
        ),
        data,
    )


def _claim(
    artifacts: tuple[RemoteArtifact, ...],
    *,
    attempt: int = 1,
    fence: int = 7,
    cancellation_generation: int = 0,
) -> RemoteClaim:
    return RemoteClaim(
        job_id="atlas-research-fixture-v1-job",
        workload_type="research.experiment",
        attempt=attempt,
        fence=fence,
        cancellation_generation=cancellation_generation,
        lease_expires_at=_future(),
        artifacts=artifacts,
        job_path="job.json",
    )


def _session(
    *,
    expires_in: int = 600,
    lease_seconds: int = 60,
    heartbeat_interval_seconds: int = 1,
) -> WorkerSession:
    return WorkerSession(
        session_id=SESSION_ID,
        token=SESSION_TOKEN,
        expires_at=_future(expires_in),
        lease_seconds=lease_seconds,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    )


class FakeClient:
    def __init__(
        self,
        claim: RemoteClaim | None,
        objects: Mapping[str, bytes],
        *,
        cancel: bool = False,
        cancel_after: int | None = None,
        download_delay: float = 0,
        download_error: RemoteWorkerError | None = None,
        download_hook: Callable[[], None] | None = None,
        heartbeat_error_at: int | None = None,
        heartbeat_hook: Callable[[int], None] | None = None,
        complete_error: RemoteWorkerError | None = None,
        complete_error_after_accept: bool = False,
        complete_hook: Callable[[], None] | None = None,
        session: WorkerSession | None = None,
    ) -> None:
        self.next_claim = claim
        self.objects = objects
        self.cancel = cancel
        self.cancel_after = cancel_after
        self.download_delay = download_delay
        self.download_error = download_error
        self.download_hook = download_hook
        self.heartbeat_error_at = heartbeat_error_at
        self.heartbeat_hook = heartbeat_hook
        self.complete_error = complete_error
        self.complete_error_after_accept = complete_error_after_accept
        self.complete_hook = complete_hook
        self.session = session or _session()
        self.completed: list[tuple[str, str, Mapping[str, object]]] = []
        self.failed: list[tuple[str, str, bool]] = []
        self.heartbeats: list[int] = []
        self.claim_calls = 0
        self.claim_error: RemoteWorkerError | None = None
        self.telemetry_error: Exception | None = None
        self.telemetry_responses: list[ScoutTelemetry] = []
        self.telemetry_delay = 0.0
        self.telemetry_calls: list[float] = []

    def exchange_session(self) -> WorkerSession:
        return self.session

    def claim(self, _session_value: WorkerSession) -> RemoteClaim | None:
        self.claim_calls += 1
        if self.claim_error is not None:
            raise self.claim_error
        claim = self.next_claim
        self.next_claim = None
        return claim

    def telemetry(
        self,
        _session_value: WorkerSession,
        *,
        cancel_event: threading.Event | None = None,
    ) -> ScoutTelemetry:
        self.telemetry_calls.append(time.monotonic())
        if self.telemetry_delay:
            if cancel_event is None:
                time.sleep(self.telemetry_delay)
            elif cancel_event.wait(self.telemetry_delay):
                raise RemoteWorkerError(
                    "WORKER_CONTROLLER_UNAVAILABLE", "Fixture telemetry was cancelled"
                )
        if self.telemetry_error is not None:
            raise self.telemetry_error
        if self.telemetry_responses:
            return self.telemetry_responses.pop(0)
        return _scout_telemetry()

    def download(self, _session_value: WorkerSession, artifact: RemoteArtifact) -> bytes:
        if self.download_hook is not None:
            self.download_hook()
        if self.download_delay:
            time.sleep(self.download_delay)
        if self.download_error is not None:
            raise self.download_error
        return self.objects[artifact.path]

    def heartbeat(
        self,
        _session_value: WorkerSession,
        _claim_value: RemoteClaim,
        sequence: int,
    ) -> HeartbeatReply:
        self.heartbeats.append(sequence)
        if self.heartbeat_hook is not None:
            self.heartbeat_hook(sequence)
        if self.heartbeat_error_at == sequence:
            raise RemoteWorkerError("WORKER_CONTROLLER_RETRY", "Fixture heartbeat failed")
        cancelled = self.cancel or (self.cancel_after is not None and sequence >= self.cancel_after)
        return HeartbeatReply(cancelled=cancelled, lease_expires_at=_future())

    def complete(
        self,
        _session_value: WorkerSession,
        claim: RemoteClaim,
        result: Mapping[str, object],
        result_sha256: str,
    ) -> None:
        if self.complete_hook is not None:
            self.complete_hook()
        if self.complete_error is not None and self.complete_error_after_accept:
            self.completed.append((claim.job_id, result_sha256, result))
            raise self.complete_error
        if self.complete_error is not None:
            raise self.complete_error
        self.completed.append((claim.job_id, result_sha256, result))

    def fail(
        self,
        _session_value: WorkerSession,
        claim: RemoteClaim,
        *,
        code: str,
        retryable: bool,
    ) -> None:
        self.failed.append((claim.job_id, code, retryable))


def test_config_requires_secure_local_paths_and_tls_or_loopback(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.worker_id == "mac-mini-test"
    assert _controller_origin("https://scout.atlasrepo.com") == (
        "https",
        "scout.atlasrepo.com",
        443,
    )
    with pytest.raises(ValidationError, match="HTTPS"):
        _controller_origin("http://scout.atlasrepo.com")
    with pytest.raises(ValidationError, match="origin"):
        _controller_origin("https://user@example.com/path")
    with pytest.raises(ValidationError, match="port"):
        _controller_origin("https://example.com:not-a-port")

    config.enrollment_token_file.chmod(0o644)
    with pytest.raises(ValidationError, match="unsafe"):
        _read_secret_file(config.enrollment_token_file)


def test_config_accepts_only_safe_absolute_optional_telemetry_file(tmp_path: Path) -> None:
    config = _config(tmp_path, telemetry=True)
    assert config.telemetry_file == config.state_root / "worker-telemetry.json"
    mapping: dict[str, object] = {
        "protocol_version": PROTOCOL_VERSION,
        "controller_url": config.controller_url,
        "worker_id": config.worker_id,
        "enrollment_token_file": str(config.enrollment_token_file),
        "state_root": str(config.state_root),
        "executor_path": str(config.executor_path),
        "telemetry_file": "relative/worker-telemetry.json",
    }
    with pytest.raises(ValidationError, match="absolute"):
        WorkerConfig.from_mapping(mapping)

    outside = tmp_path / "outside.json"
    outside.write_text("preserve", encoding="utf-8")
    outside.chmod(0o600)
    linked = config.state_root / "linked-telemetry.json"
    linked.symlink_to(outside)
    mapping["telemetry_file"] = str(linked)
    with pytest.raises(ValidationError, match="unsafe"):
        WorkerConfig.from_mapping(mapping)


def test_load_config_rejects_group_readable_config(tmp_path: Path) -> None:
    config = _config(tmp_path)
    mapping = {
        "protocol_version": PROTOCOL_VERSION,
        "controller_url": config.controller_url,
        "worker_id": config.worker_id,
        "enrollment_token_file": str(config.enrollment_token_file),
        "state_root": str(config.state_root),
        "executor_path": str(config.executor_path),
    }
    path = _write_private(tmp_path / "worker.json", json.dumps(mapping), 0o644)
    with pytest.raises(ValidationError, match="unsafe"):
        load_worker_config(path)
    path.chmod(0o600)
    assert load_worker_config(path).worker_id == config.worker_id


def test_session_and_claim_contracts_fail_closed() -> None:
    session = _session_from_mapping(
        {
            "protocol_version": PROTOCOL_VERSION,
            "session_id": SESSION_ID,
            "session_token": SESSION_TOKEN,
            "expires_at": _timestamp(),
            "lease_seconds": 60,
            "heartbeat_interval_seconds": 10,
        }
    )
    assert session.lease_seconds == 60

    job = b"{}\n"
    digest = hashlib.sha256(job).hexdigest()
    mapping: dict[str, object] = {
        "protocol_version": PROTOCOL_VERSION,
        "job_id": "research-job-1",
        "workload_type": "research.experiment",
        "attempt": 1,
        "fence": 2,
        "cancellation_generation": 0,
        "lease_expires_at": _timestamp(),
        "artifacts": [
            {
                "path": "job.json",
                "sha256": digest,
                "size_bytes": len(job),
                "download_path": f"/api/worker/v1/objects/{JOB_OBJECT_ID}",
            }
        ],
        "job_path": "job.json",
    }
    assert _claim_from_mapping(mapping, 1 << 20).fence == 2

    malformed = dict(mapping)
    malformed["artifacts"] = [
        {
            "path": "../job.json",
            "sha256": digest,
            "size_bytes": len(job),
            "download_path": "https://attacker.invalid/job",
        }
    ]
    with pytest.raises(ValidationError, match="path"):
        _claim_from_mapping(malformed, 1 << 20)

    too_large = dict(mapping)
    too_large["artifacts"] = [
        {
            "path": "job.json",
            "sha256": digest,
            "size_bytes": 100,
            "download_path": f"/api/worker/v1/objects/{JOB_OBJECT_ID}",
        }
    ]
    with pytest.raises(ResourceLimitError, match="bundle"):
        _claim_from_mapping(too_large, 99)

    oversized = dict(mapping)
    oversized["artifacts"] = [
        {
            "path": "job.json",
            "sha256": digest,
            "size_bytes": MAX_ARTIFACT_BYTES + 1,
            "download_path": f"/api/worker/v1/objects/{JOB_OBJECT_ID}",
        }
    ]
    with pytest.raises(ValidationError, match="size"):
        _claim_from_mapping(oversized, MAX_TOTAL_INPUT_BYTES)

    unicode_object = dict(mapping)
    unicode_object["artifacts"] = [
        {
            "path": "job.json",
            "sha256": digest,
            "size_bytes": len(job),
            "download_path": "/api/worker/v1/objects/é23e4567-e89b-12d3-a456-426614174000",
        }
    ]
    with pytest.raises(ValidationError, match="download"):
        _claim_from_mapping(unicode_object, MAX_TOTAL_INPUT_BYTES)


def test_scout_canonical_vectors_match_exactly() -> None:
    fixture = json.loads(
        (Path(__file__).parents[1] / "docs/contracts/research-worker-canonical-v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert fixture["protocol_version"] == PROTOCOL_VERSION
    assert fixture["canonicalization"] == "RFC8785"
    for vector in fixture["vectors"]:
        canonical = canonical_json_bytes(vector["value"])
        assert canonical.decode("utf-8") == vector["canonical"], vector["id"]
        assert hashlib.sha256(canonical).hexdigest() == vector["sha256"], vector["id"]


@pytest.mark.parametrize(
    "suffix",
    [".rar", ".whl", ".jar", ".zst", ".tar.zst", ".cab", ".iso", ".dmg"],
)
def test_claim_rejects_scout_archive_suffixes(suffix: str) -> None:
    job = _fixture_job()
    mapping: dict[str, object] = {
        "protocol_version": PROTOCOL_VERSION,
        "job_id": "atlas-research-fixture-v1-job",
        "workload_type": "research.experiment",
        "attempt": 1,
        "fence": 2,
        "cancellation_generation": 0,
        "lease_expires_at": _timestamp(),
        "artifacts": [
            {
                "path": "job.json",
                "sha256": hashlib.sha256(job).hexdigest(),
                "size_bytes": len(job),
                "download_path": f"/api/worker/v1/objects/{JOB_OBJECT_ID}",
            },
            {
                "path": f"payload{suffix}",
                "sha256": hashlib.sha256(b"").hexdigest(),
                "size_bytes": 0,
                "download_path": f"/api/worker/v1/objects/{DATA_OBJECT_ID}",
            },
        ],
        "job_path": "job.json",
    }
    with pytest.raises(ValidationError, match="Archive"):
        _claim_from_mapping(mapping, MAX_TOTAL_INPUT_BYTES)


def test_worker_stages_executes_completes_and_cleans_confirmed_run(tmp_path: Path) -> None:
    job = _fixture_job()
    result = _fixture_result(job)
    job_artifact, job = _artifact("job.json", job)
    nested_artifact, nested = _artifact("data/input.jsonl", b'{"id":1}\n', DATA_OBJECT_ID)
    claim = _claim((job_artifact, nested_artifact))
    capture = tmp_path / "executor-args"
    config = _config(
        tmp_path,
        executor=_executor(tmp_path / "executor", result=result, capture_path=capture),
    )
    assignment_root = config.state_root / "runs" / "atlas-research-fixture-v1-job-1-7-0"
    run_root = assignment_root / "atlas-research-run"

    def verify_before_terminal_ack() -> None:
        assert run_root.name == "atlas-research-run"
        assert stat.S_IMODE(run_root.stat().st_mode) == 0o700
        assert stat.S_IMODE((run_root / "artifacts").stat().st_mode) == 0o555
        assert stat.S_IMODE((run_root / "output").stat().st_mode) == 0o700
        assert stat.S_IMODE((run_root / "artifacts/data/input.jsonl").stat().st_mode) == 0o444

    fake = FakeClient(
        claim,
        {"job.json": job, "data/input.jsonl": nested},
        complete_hook=verify_before_terminal_ack,
    )
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))

    outcome = worker.run_once()

    assert outcome.state == "completed"
    assert fake.heartbeats == [1]
    assert len(fake.completed) == 1
    expected = hashlib.sha256(canonical_json_bytes(result)).hexdigest()
    assert fake.completed[0][1] == expected
    arguments = capture.read_text(encoding="utf-8").splitlines()
    assert arguments[arguments.index("--run-root") + 1] == str(run_root)
    assert arguments[arguments.index("--session-id") + 1] == SESSION_ID
    assert not assignment_root.exists()
    assert list((config.state_root / "runs").iterdir()) == []
    status = json.loads((config.state_root / "status.json").read_text(encoding="utf-8"))
    assert status["state"] == "completed"
    assert SESSION_TOKEN not in json.dumps(status)


def test_one_shot_publishes_scout_truth_without_fake_zeroes(tmp_path: Path) -> None:
    config = _config(tmp_path, telemetry=True)
    fake = FakeClient(None, {})

    outcome = RemoteWorker(config, cast(ScoutWorkerClient, fake)).run_once()

    assert outcome.state == "idle"
    assert len(fake.telemetry_calls) == 1
    telemetry = json.loads(cast(Path, config.telemetry_file).read_text(encoding="utf-8"))
    assert telemetry["schema_version"] == 1
    assert telemetry["worker_id"] == "atlasrepo"
    assert telemetry["state"] == "idle"
    assert telemetry["queue"] == {"pending": 7, "in_flight": 1, "failed": 2}
    assert telemetry["totals"] == {"processed": 3, "failed": 1}
    assert telemetry["active_model"] is None
    assert telemetry["history"][-1]["processed"] == 3


def test_controller_failure_projects_degraded_only_after_fresh_scout_truth(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, telemetry=True)
    fake = FakeClient(None, {})
    fake.claim_error = RemoteWorkerError(
        "WORKER_CONTROLLER_RETRY", "Fixture controller unavailable"
    )
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))

    with pytest.raises(RemoteWorkerError, match="controller unavailable"):
        worker.run_once()

    telemetry = json.loads(cast(Path, config.telemetry_file).read_text(encoding="utf-8"))
    assert telemetry["state"] == "degraded"
    assert telemetry["queue"]["pending"] == 7


def test_terminal_job_failure_returns_worker_to_idle(tmp_path: Path) -> None:
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    fake = FakeClient(_claim((artifact,)), {"job.json": job})
    config = _config(tmp_path, telemetry=True)

    outcome = RemoteWorker(config, cast(ScoutWorkerClient, fake)).run_once()

    assert outcome.state == "failed"
    assert fake.failed == [("atlas-research-fixture-v1-job", "RESULT_MISSING", False)]
    telemetry = json.loads(cast(Path, config.telemetry_file).read_text(encoding="utf-8"))
    assert telemetry["state"] == "idle"


@pytest.mark.parametrize(
    "telemetry_error",
    [
        RemoteWorkerError("WORKER_CONTROLLER_RETRY", "Fixture telemetry unavailable"),
        ValidationError("WORKER_TELEMETRY_INVALID", "Fixture telemetry response invalid"),
    ],
)
def test_failed_telemetry_fetch_or_validation_never_refreshes_existing_projection(
    tmp_path: Path, telemetry_error: Exception
) -> None:
    config = _config(tmp_path, telemetry=True)
    fake = FakeClient(None, {})
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))
    assert worker.run_once().state == "idle"
    destination = cast(Path, config.telemetry_file)
    original = destination.read_bytes()
    original_mtime = destination.stat().st_mtime_ns
    fake.telemetry_error = telemetry_error

    assert worker.run_once().state == "idle"

    assert destination.read_bytes() == original
    assert destination.stat().st_mtime_ns == original_mtime


def test_older_scout_snapshot_cannot_overwrite_newer_projection(tmp_path: Path) -> None:
    config = _config(tmp_path, telemetry=True)
    fake = FakeClient(None, {})
    now = datetime.now(UTC).replace(microsecond=123_000)
    fake.telemetry_responses = [
        _scout_telemetry(now, pending=9),
        _scout_telemetry(now - timedelta(seconds=1), pending=1),
    ]
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))
    assert worker.run_once().state == "idle"
    destination = cast(Path, config.telemetry_file)
    first = destination.read_bytes()

    assert worker.run_once().state == "idle"

    assert destination.read_bytes() == first
    assert json.loads(first)["queue"]["pending"] == 9


def test_post_rename_ambiguity_advances_watermark_and_prevents_visible_regression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path, telemetry=True)
    fake = FakeClient(None, {})
    now = datetime.now(UTC).replace(microsecond=123_000)
    fake.telemetry_responses = [
        _scout_telemetry(now, pending=9),
        _scout_telemetry(now - timedelta(seconds=1), pending=1),
    ]
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))
    worker._session = _session()
    real_write = remote_worker_module.write_worker_telemetry
    writes = 0

    def ambiguous_write(
        path: Path,
        value: Mapping[str, object],
        *,
        watermark: datetime,
    ) -> None:
        nonlocal writes
        writes += 1
        real_write(path, value, watermark=watermark)
        raise TelemetryCommitAmbiguousError(watermark)

    monkeypatch.setattr(remote_worker_module, "write_worker_telemetry", ambiguous_write)

    assert worker._publish_telemetry_once() is False
    destination = cast(Path, config.telemetry_file)
    visible = destination.read_bytes()
    assert json.loads(visible)["queue"]["pending"] == 9
    assert worker._last_telemetry_at == now

    assert worker._publish_telemetry_once() is False
    assert destination.read_bytes() == visible
    assert writes == 1


def test_restart_cannot_regress_persisted_future_telemetry_watermark(tmp_path: Path) -> None:
    config = _config(tmp_path, telemetry=True)
    now = datetime.now(UTC).replace(microsecond=123_000)
    future = now + timedelta(seconds=29)
    first_client = FakeClient(None, {})
    first_client.telemetry_responses = [_scout_telemetry(future, pending=9)]
    first_worker = RemoteWorker(config, cast(ScoutWorkerClient, first_client))
    first_worker._session = _session()
    assert first_worker._publish_telemetry_once() is True
    destination = cast(Path, config.telemetry_file)
    visible = destination.read_bytes()

    restarted_client = FakeClient(None, {})
    restarted_client.telemetry_responses = [_scout_telemetry(now, pending=1)]
    restarted_worker = RemoteWorker(config, cast(ScoutWorkerClient, restarted_client))
    restarted_worker._session = _session()

    assert restarted_worker._publish_telemetry_once() is False
    assert destination.read_bytes() == visible
    assert restarted_worker._last_telemetry_at == future


def test_telemetry_publication_is_serialized(tmp_path: Path) -> None:
    config = _config(tmp_path, telemetry=True)
    fake = FakeClient(None, {})
    fake.telemetry_delay = 0.05
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))
    worker._session = _session()
    active = 0
    maximum_active = 0
    guard = threading.Lock()
    original = fake.telemetry

    def observed(
        session: WorkerSession,
        *,
        cancel_event: threading.Event | None = None,
    ) -> ScoutTelemetry:
        nonlocal active, maximum_active
        with guard:
            active += 1
            maximum_active = max(maximum_active, active)
        try:
            return original(session, cancel_event=cancel_event)
        finally:
            with guard:
                active -= 1

    fake.telemetry = observed  # type: ignore[method-assign]
    threads = [threading.Thread(target=worker._publish_telemetry_once) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert maximum_active == 1
    assert len(fake.telemetry_calls) == 2


def test_telemetry_failure_never_changes_successful_terminal_path(tmp_path: Path) -> None:
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    fake = FakeClient(_claim((artifact,)), {"job.json": job})
    fake.telemetry_error = RuntimeError("fixture publisher failure")
    config = _config(
        tmp_path,
        executor=_executor(tmp_path / "executor", result=_fixture_result(job)),
        telemetry=True,
    )

    outcome = RemoteWorker(config, cast(ScoutWorkerClient, fake)).run_once()

    assert outcome.state == "completed"
    assert len(fake.completed) == 1
    assert not cast(Path, config.telemetry_file).exists()


def test_serve_refreshes_telemetry_during_long_job_without_delaying_heartbeat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(remote_worker_module, "_TELEMETRY_PUBLISH_INTERVAL_SECONDS", 0.05)
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    fake = FakeClient(
        _claim((artifact,)),
        {"job.json": job},
        session=_session(heartbeat_interval_seconds=1),
    )
    fake.telemetry_delay = 0.01
    config = _config(
        tmp_path,
        executor=_executor(tmp_path / "executor", result=_fixture_result(job), sleep=1.15),
        telemetry=True,
    )
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))
    fake.complete_hook = worker.stop_event.set
    states: list[str] = []
    real_write = remote_worker_module.write_worker_telemetry

    def record_write(
        path: Path,
        value: Mapping[str, object],
        *,
        watermark: datetime,
    ) -> None:
        states.append(cast(str, value["state"]))
        real_write(path, value, watermark=watermark)

    monkeypatch.setattr(remote_worker_module, "write_worker_telemetry", record_write)

    outcome = worker.serve()

    assert outcome.state == "completed"
    assert "running" in states
    assert states[-1] == "offline"
    assert len(fake.telemetry_calls) >= 10
    assert fake.heartbeats[:2] == [1, 2]
    intervals = [
        right - left
        for left, right in zip(fake.telemetry_calls, fake.telemetry_calls[1:], strict=False)
    ]
    assert max(intervals[:-1]) < 0.2


def test_worker_honors_server_cancellation(tmp_path: Path) -> None:
    job = _fixture_job()
    job_artifact, job = _artifact("job.json", job)
    claim = _claim((job_artifact,))
    fake = FakeClient(claim, {"job.json": job}, cancel=True)
    config = _config(tmp_path, executor=_executor(tmp_path / "executor", sleep=5))
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))

    outcome = worker.run_once()

    assert outcome.state == "cancelled"
    assert fake.heartbeats == [1]
    assert fake.completed == []
    assert fake.failed == []
    assert list((config.state_root / "runs").iterdir()) == []


def test_session_must_cover_job_lease_and_request_margin(tmp_path: Path) -> None:
    fake = FakeClient(None, {}, session=_session(expires_in=133))
    worker = RemoteWorker(_config(tmp_path), cast(ScoutWorkerClient, fake))

    with pytest.raises(RemoteWorkerError, match="cannot cover"):
        worker.run_once()

    assert fake.claim_calls == 0
    assert fake.failed == []


def test_slow_staging_is_heartbeated_and_cancellation_prevents_execution(tmp_path: Path) -> None:
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    fake = FakeClient(
        _claim((artifact,)),
        {"job.json": job},
        cancel_after=2,
        download_delay=1.1,
    )
    capture = tmp_path / "executor-args"
    config = _config(
        tmp_path,
        executor=_executor(
            tmp_path / "executor",
            result=_fixture_result(job),
            capture_path=capture,
        ),
    )

    outcome = RemoteWorker(config, cast(ScoutWorkerClient, fake)).run_once()

    assert outcome.state == "cancelled"
    assert fake.heartbeats[:2] == [1, 2]
    assert not capture.exists()
    assert fake.completed == []
    assert fake.failed == []
    assert list((config.state_root / "runs").iterdir()) == []


def test_heartbeat_fence_loss_and_controller_errors_never_send_fail(tmp_path: Path) -> None:
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    fake = FakeClient(
        _claim((artifact,)),
        {"job.json": job},
        heartbeat_error_at=2,
        download_delay=1.1,
    )

    with pytest.raises(RemoteWorkerError, match="heartbeat failed"):
        RemoteWorker(_config(tmp_path), cast(ScoutWorkerClient, fake)).run_once()

    assert fake.failed == []
    assert fake.completed == []


def test_total_claim_deadline_includes_staging_and_never_sends_fail(tmp_path: Path) -> None:
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    fake = FakeClient(
        _claim((artifact,)),
        {"job.json": job},
        download_delay=1.1,
    )
    config = _config(tmp_path)
    config = WorkerConfig(
        controller_url=config.controller_url,
        worker_id=config.worker_id,
        enrollment_token_file=config.enrollment_token_file,
        state_root=config.state_root,
        executor_path=config.executor_path,
        poll_seconds=config.poll_seconds,
        request_timeout_seconds=config.request_timeout_seconds,
        max_job_seconds=1,
        max_bundle_bytes=config.max_bundle_bytes,
    )

    with pytest.raises(RemoteWorkerError, match="deadline"):
        RemoteWorker(config, cast(ScoutWorkerClient, fake)).run_once()

    assert fake.failed == []
    assert fake.completed == []


def test_transient_download_and_ambiguous_complete_never_send_fail(tmp_path: Path) -> None:
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    download_failure = FakeClient(
        _claim((artifact,)),
        {"job.json": job},
        download_error=RemoteWorkerError(
            "WORKER_CONTROLLER_UNAVAILABLE", "Fixture controller unavailable"
        ),
    )
    download_root = tmp_path / "download"
    download_root.mkdir()
    download_config = _config(download_root)
    with pytest.raises(RemoteWorkerError, match="controller unavailable"):
        RemoteWorker(download_config, cast(ScoutWorkerClient, download_failure)).run_once()
    assert download_failure.failed == []
    assert (
        download_config.state_root / "runs/atlas-research-fixture-v1-job-1-7-0/atlas-research-run"
    ).is_dir()

    completion_failure = FakeClient(
        _claim((artifact,)),
        {"job.json": job},
        complete_error=RemoteWorkerError(
            "WORKER_CONTROLLER_UNAVAILABLE", "Fixture completion ambiguous"
        ),
    )
    completion_root = tmp_path / "complete"
    completion_root.mkdir()
    config = _config(
        completion_root,
        executor=_executor(
            completion_root / "executor",
            result=_fixture_result(job),
        ),
    )
    with pytest.raises(RemoteWorkerError, match="completion ambiguous"):
        RemoteWorker(config, cast(ScoutWorkerClient, completion_failure)).run_once()
    assert completion_failure.failed == []
    assignment_root = config.state_root / "runs/atlas-research-fixture-v1-job-1-7-0"
    assert (assignment_root / "atlas-research-run/output/result.json").is_file()

    replay_capture = completion_root / "replay-executor-args"
    _executor(config.executor_path, capture_path=replay_capture)
    replay = FakeClient(_claim((artifact,)), {"job.json": job})
    outcome = RemoteWorker(config, cast(ScoutWorkerClient, replay)).run_once()

    assert outcome.state == "completed"
    assert len(replay.completed) == 1
    assert not replay_capture.exists()
    assert not assignment_root.exists()


def test_new_claim_prunes_prior_ambiguous_run_before_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first_job = _fixture_job(attempt=1)
    second_job = _fixture_job(attempt=2)
    first_artifact, first_job = _artifact("job.json", first_job)
    second_artifact, second_job = _artifact("job.json", second_job)
    ambiguous = RemoteWorkerError(
        "WORKER_TERMINAL_AMBIGUOUS", "Fixture accepted completion with malformed ACK"
    )
    first = FakeClient(
        _claim((first_artifact,), attempt=1),
        {"job.json": first_job},
        complete_error=ambiguous,
        complete_error_after_accept=True,
    )
    config = _config(
        tmp_path,
        executor=_executor(tmp_path / "executor", result=_fixture_result(first_job)),
    )

    with pytest.raises(RemoteWorkerError, match="malformed ACK"):
        RemoteWorker(config, cast(ScoutWorkerClient, first)).run_once()

    first_parent = config.state_root / "runs/atlas-research-fixture-v1-job-1-7-0"
    second_parent = config.state_root / "runs/atlas-research-fixture-v1-job-2-8-0"
    assert first_parent.is_dir()
    assert len(first.completed) == 1
    _executor(config.executor_path, result=_fixture_result(second_job))

    def verify_pruned_before_download() -> None:
        assert not first_parent.exists()
        assert (second_parent / "atlas-research-run").is_dir()

    second = FakeClient(
        _claim((second_artifact,), attempt=2, fence=8),
        {"job.json": second_job},
        download_hook=verify_pruned_before_download,
        complete_error=ambiguous,
        complete_error_after_accept=True,
    )
    second_worker = RemoteWorker(config, cast(ScoutWorkerClient, second))
    remove = second_worker._remove_direct_run_entry

    def slow_remove(entry: Path) -> None:
        time.sleep(1.1)
        remove(entry)

    monkeypatch.setattr(second_worker, "_remove_direct_run_entry", slow_remove)
    with pytest.raises(RemoteWorkerError, match="malformed ACK"):
        second_worker.run_once()

    assert len(second.heartbeats) >= 2
    assert len(second.completed) == 1
    assert second.failed == []
    assert not first_parent.exists()
    assert second_parent.is_dir()
    assert list((config.state_root / "runs").iterdir()) == [second_parent]


def test_idle_prune_removes_tampered_entries_without_following_symlinks(tmp_path: Path) -> None:
    config = _config(tmp_path)
    fake = FakeClient(None, {})
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel"
    sentinel.write_text("preserve", encoding="utf-8")
    (worker._runs_root / "tampered-link").symlink_to(outside, target_is_directory=True)
    (worker._runs_root / "tampered-file").write_text("remove", encoding="utf-8")
    stale = worker._runs_root / "atlas-research-fixture-v1-job-1-6-0"
    nested = stale / "nested"
    nested.mkdir(parents=True)
    (stale / "outside-link").symlink_to(outside, target_is_directory=True)
    nested.chmod(0o555)

    outcome = worker.run_once()

    assert outcome.state == "idle"
    assert list(worker._runs_root.iterdir()) == []
    assert sentinel.read_text(encoding="utf-8") == "preserve"


def test_low_disk_gate_is_operational_and_never_sends_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    fake = FakeClient(_claim((artifact,)), {"job.json": job})
    config = _config(tmp_path)
    usage = shutil.disk_usage(tmp_path)
    monkeypatch.setattr("atlas_research.remote_worker._FREE_SPACE_RESERVE_BYTES", 100)
    monkeypatch.setattr(
        "atlas_research.remote_worker.shutil.disk_usage",
        lambda _path: usage._replace(free=len(job) + 99),
    )

    with pytest.raises(RemoteWorkerError) as captured:
        RemoteWorker(config, cast(ScoutWorkerClient, fake)).run_once()

    assert captured.value.code == "WORKER_STORAGE_LOW"
    assert fake.heartbeats == [1]
    assert fake.completed == []
    assert fake.failed == []
    assert list((config.state_root / "runs").iterdir()) == []
    status = json.loads((config.state_root / "status.json").read_text(encoding="utf-8"))
    assert status["state"] == "backoff"
    assert status["last_error_code"] == "WORKER_STORAGE_LOW"


@pytest.mark.parametrize("storage_errno", STORAGE_EXHAUSTION_ERRNOS)
@pytest.mark.parametrize(
    ("operation", "attribute", "fail_after"),
    [
        ("write", "write", 2),
        ("file_fsync", "fsync", 3),
        ("directory_fsync", "fsync", 4),
        ("link", "link", 1),
        ("rename", "replace", 2),
    ],
)
def test_storage_exhaustion_during_atomic_commit_never_sends_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    storage_errno: int,
    operation: str,
    attribute: str,
    fail_after: int,
) -> None:
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    armed = False
    calls = 0

    def arm_storage_fault(_sequence: int) -> None:
        nonlocal armed
        armed = True

    fake = FakeClient(
        _claim((artifact,)),
        {"job.json": job},
        heartbeat_hook=arm_storage_fault,
    )
    config = _config(tmp_path)
    original = getattr(os, attribute)

    def inject_storage_fault(*args: object, **kwargs: object) -> object:
        nonlocal calls
        if armed:
            calls += 1
            if calls >= fail_after:
                raise OSError(storage_errno, f"fixture {operation} storage exhaustion")
        return original(*args, **kwargs)

    monkeypatch.setattr(f"atlas_research.artifacts.os.{attribute}", inject_storage_fault)

    with pytest.raises(RemoteWorkerError) as captured:
        RemoteWorker(config, cast(ScoutWorkerClient, fake)).run_once()

    assert captured.value.code == "WORKER_STORAGE_EXHAUSTED"
    assert calls >= fail_after
    assert fake.completed == []
    assert fake.failed == []
    assert (
        config.state_root / "runs/atlas-research-fixture-v1-job-1-7-0/atlas-research-run"
    ).is_dir()


@pytest.mark.parametrize("status_errno", [errno.ENOSPC, errno.EIO])
def test_operational_status_write_error_does_not_mask_original_remote_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status_errno: int,
) -> None:
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    fake = FakeClient(
        _claim((artifact,)),
        {"job.json": job},
        download_error=RemoteWorkerError(
            "WORKER_CONTROLLER_UNAVAILABLE", "Fixture download unavailable"
        ),
    )
    config = _config(tmp_path)
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))
    status = worker._status

    def status_with_operational_error(
        state: str,
        *,
        claim: RemoteClaim | None = None,
        error_code: str | None = None,
    ) -> None:
        if state == "backoff":
            try:
                raise OSError(status_errno, "fixture status write error")
            except OSError as cause:
                raise ValidationError(
                    "OUTPUT_WRITE_FAILED", "Fixture status write failed"
                ) from cause
        status(state, claim=claim, error_code=error_code)

    monkeypatch.setattr(worker, "_status", status_with_operational_error)

    with pytest.raises(RemoteWorkerError) as captured:
        worker.run_once()

    assert captured.value.code == "WORKER_CONTROLLER_UNAVAILABLE"
    assert fake.completed == []
    assert fake.failed == []
    assert (
        config.state_root / "runs/atlas-research-fixture-v1-job-1-7-0/atlas-research-run"
    ).is_dir()


def test_best_effort_status_does_not_hide_pure_unsafe_state_error(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = RemoteWorker(config)
    real_state = tmp_path / "state-real"
    outside = tmp_path / "outside"
    outside.mkdir()
    config.state_root.rename(real_state)
    config.state_root.symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValidationError) as captured:
        worker._status_best_effort("backoff")

    assert captured.value.code == "OUTPUT_ROOT_INVALID"
    assert list(outside.iterdir()) == []


def test_wrapped_run_directory_io_error_is_operational(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    claim = _claim((artifact,))
    fake = FakeClient(claim, {"job.json": job})
    config = _config(tmp_path)
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))
    run_parent = worker._run_parent(claim)
    run_parent.mkdir(mode=0o700)
    mkdir = Path.mkdir

    def mkdir_with_io_error(path: Path, *args: object, **kwargs: object) -> None:
        if path == run_parent:
            raise OSError(errno.EIO, "fixture run directory I/O failure")
        mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", mkdir_with_io_error)

    with pytest.raises(RemoteWorkerError) as captured:
        worker.run_once()

    assert captured.value.code == "WORKER_STORAGE_UNAVAILABLE"
    assert fake.completed == []
    assert fake.failed == []
    assert run_parent.is_dir()


def test_consumed_reserve_after_missing_result_is_operational(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    fake = FakeClient(_claim((artifact,)), {"job.json": job})
    config = _config(tmp_path)
    config.executor_path.write_text("#!/bin/sh\nexit 9\n", encoding="utf-8")
    usage = shutil.disk_usage(tmp_path)
    checks = 0
    monkeypatch.setattr("atlas_research.remote_worker._FREE_SPACE_RESERVE_BYTES", 100)

    def disk_usage(_path: Path) -> object:
        nonlocal checks
        checks += 1
        free = len(job) + 100 if checks == 1 else 99
        return usage._replace(free=free)

    monkeypatch.setattr("atlas_research.remote_worker.shutil.disk_usage", disk_usage)

    with pytest.raises(RemoteWorkerError) as captured:
        RemoteWorker(config, cast(ScoutWorkerClient, fake)).run_once()

    assert captured.value.code == "WORKER_STORAGE_LOW"
    assert checks == 2
    assert fake.completed == []
    assert fake.failed == []
    assert (
        config.state_root / "runs/atlas-research-fixture-v1-job-1-7-0/atlas-research-run"
    ).is_dir()


def test_attempt_binding_and_result_binding_fail_terminally(tmp_path: Path) -> None:
    job = _fixture_job(attempt=1)
    artifact, job = _artifact("job.json", job)
    attempt_mismatch = FakeClient(_claim((artifact,), attempt=2), {"job.json": job})
    first_root = tmp_path / "attempt"
    first_root.mkdir()
    attempt_config = _config(first_root)

    outcome = RemoteWorker(attempt_config, cast(ScoutWorkerClient, attempt_mismatch)).run_once()

    assert outcome.error_code == "WORKER_JOB_IDENTITY_INVALID"
    assert attempt_mismatch.failed == [
        ("atlas-research-fixture-v1-job", "WORKER_JOB_IDENTITY_INVALID", False)
    ]
    assert attempt_mismatch.completed == []
    assert list((attempt_config.state_root / "runs").iterdir()) == []

    bad_result = _fixture_result(job, session_id="wrong-session")
    result_mismatch = FakeClient(_claim((artifact,)), {"job.json": job})
    second_root = tmp_path / "result"
    second_root.mkdir()
    config = _config(
        second_root,
        executor=_executor(second_root / "executor", result=bad_result),
    )
    outcome = RemoteWorker(config, cast(ScoutWorkerClient, result_mismatch)).run_once()
    assert outcome.error_code == "WORKER_RESULT_INVALID"
    assert result_mismatch.failed[0][1:] == ("WORKER_RESULT_INVALID", False)
    assert result_mismatch.completed == []
    assert list((config.state_root / "runs").iterdir()) == []

    spec_result = _fixture_result(job)
    spec_result["job_spec_sha256"] = "0" * 64
    spec_mismatch = FakeClient(_claim((artifact,)), {"job.json": job})
    third_root = tmp_path / "spec"
    third_root.mkdir()
    config = _config(
        third_root,
        executor=_executor(third_root / "executor", result=spec_result),
    )
    outcome = RemoteWorker(config, cast(ScoutWorkerClient, spec_mismatch)).run_once()
    assert outcome.error_code == "WORKER_RESULT_INVALID"
    assert spec_mismatch.failed[0][1:] == ("WORKER_RESULT_INVALID", False)
    assert spec_mismatch.completed == []
    assert list((config.state_root / "runs").iterdir()) == []


def test_attempt_specific_job_envelopes_do_not_replay_prior_attempt(tmp_path: Path) -> None:
    first_job = _fixture_job(attempt=1)
    second_job = _fixture_job(attempt=2)
    assert canonical_sha256(strict_json_loads(first_job)) != canonical_sha256(
        strict_json_loads(second_job)
    )
    first_artifact, _ = _artifact("job.json", first_job)
    second_artifact, _ = _artifact("job.json", second_job)
    first_capture = tmp_path / "first-executor-args"
    second_capture = tmp_path / "second-executor-args"
    executor = _executor(
        tmp_path / "executor",
        result=_fixture_result(first_job),
        capture_path=first_capture,
    )
    config = _config(tmp_path, executor=executor)
    first = FakeClient(_claim((first_artifact,), attempt=1), {"job.json": first_job})
    assert RemoteWorker(config, cast(ScoutWorkerClient, first)).run_once().state == "completed"

    _executor(executor, result=_fixture_result(second_job), capture_path=second_capture)
    second = FakeClient(
        _claim((second_artifact,), attempt=2, fence=8),
        {"job.json": second_job},
    )
    assert RemoteWorker(config, cast(ScoutWorkerClient, second)).run_once().state == "completed"

    first_arguments = first_capture.read_text(encoding="utf-8").splitlines()
    second_arguments = second_capture.read_text(encoding="utf-8").splitlines()
    assert Path(first_arguments[first_arguments.index("--run-root") + 1]).parent.name == (
        "atlas-research-fixture-v1-job-1-7-0"
    )
    assert Path(second_arguments[second_arguments.index("--run-root") + 1]).parent.name == (
        "atlas-research-fixture-v1-job-2-8-0"
    )
    assert list((config.state_root / "runs").iterdir()) == []
    assert first.completed[0][1] != second.completed[0][1]


def test_canonical_result_limit_and_cancellation_generation_replay_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _fixture_job()
    result = _fixture_result(job)
    artifact, job = _artifact("job.json", job)
    too_large = FakeClient(_claim((artifact,)), {"job.json": job})
    monkeypatch.setattr("atlas_research.remote_worker._MAX_RESULT_BYTES", 32)
    limit_root = tmp_path / "limit"
    limit_root.mkdir()
    config = _config(
        limit_root,
        executor=_executor(limit_root / "executor", result=result),
    )
    outcome = RemoteWorker(config, cast(ScoutWorkerClient, too_large)).run_once()
    assert outcome.error_code == "WORKER_RESULT_EXCEEDED"
    assert too_large.failed[0][1:] == ("WORKER_RESULT_EXCEEDED", False)
    assert list((config.state_root / "runs").iterdir()) == []

    monkeypatch.undo()
    replay_root = tmp_path / "generation"
    replay_root.mkdir()
    first_capture = replay_root / "first-executor-args"
    second_capture = replay_root / "second-executor-args"
    config = _config(
        replay_root,
        executor=_executor(
            replay_root / "executor",
            result=result,
            capture_path=first_capture,
        ),
    )
    first = FakeClient(_claim((artifact,), cancellation_generation=0), {"job.json": job})
    second = FakeClient(_claim((artifact,), cancellation_generation=1), {"job.json": job})
    assert RemoteWorker(config, cast(ScoutWorkerClient, first)).run_once().state == "completed"
    _executor(config.executor_path, result=result, capture_path=second_capture)
    assert RemoteWorker(config, cast(ScoutWorkerClient, second)).run_once().state == "completed"
    first_arguments = first_capture.read_text(encoding="utf-8").splitlines()
    second_arguments = second_capture.read_text(encoding="utf-8").splitlines()
    assert Path(first_arguments[first_arguments.index("--run-root") + 1]).parent.name == (
        "atlas-research-fixture-v1-job-1-7-0"
    )
    assert Path(second_arguments[second_arguments.index("--run-root") + 1]).parent.name == (
        "atlas-research-fixture-v1-job-1-7-1"
    )
    assert list((config.state_root / "runs").iterdir()) == []
    assert len(first.completed) == len(second.completed) == 1


def test_cleanup_is_bounded_and_does_not_follow_nested_symlinks(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = RemoteWorker(config)
    job = _fixture_job()
    claim = _claim((_artifact("job.json", job)[0],))
    run_root = worker._run_root(claim)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel"
    sentinel.write_text("preserve", encoding="utf-8")
    (run_root / "output/outside-link").symlink_to(outside, target_is_directory=True)

    worker._cleanup_run_root(run_root, claim)

    assert sentinel.read_text(encoding="utf-8") == "preserve"
    assert not run_root.parent.exists()
    with pytest.raises(ValidationError, match="outside the active claim"):
        worker._cleanup_run_root(outside, claim)
    assert sentinel.is_file()


def test_cleanup_does_not_require_chmod_nofollow_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    worker = RemoteWorker(config)
    job = _fixture_job()
    claim = _claim((_artifact("job.json", job)[0],))
    run_root = worker._run_root(claim)
    nested = run_root / "artifacts/nested"
    nested.mkdir()
    nested.chmod(0o555)
    (run_root / "artifacts").chmod(0o555)
    linux_capabilities = set(os.supports_follow_symlinks)
    linux_capabilities.discard(os.chmod)
    monkeypatch.setattr(os, "supports_follow_symlinks", linux_capabilities)

    worker._cleanup_run_root(run_root, claim)

    assert not run_root.parent.exists()


@pytest.mark.skipif(os.geteuid() == 0, reason="root can open mode-000 directories")
def test_prune_fails_closed_for_unopenable_nested_directory(tmp_path: Path) -> None:
    config = _config(tmp_path)
    fake = FakeClient(None, {})
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel"
    sentinel.write_text("preserve", encoding="utf-8")
    stale = worker._runs_root / "atlas-research-fixture-v1-job-1-6-0"
    nested = stale / "nested"
    nested.mkdir(parents=True)
    (nested / "outside-link").symlink_to(outside, target_is_directory=True)
    nested.chmod(0o000)

    try:
        with pytest.raises(RemoteWorkerError) as captured:
            worker.run_once()
        assert captured.value.code == "WORKER_RUN_PRUNE_FAILED"
        assert nested.is_dir()
        assert sentinel.read_text(encoding="utf-8") == "preserve"
        assert fake.failed == []
    finally:
        nested.chmod(0o700)


@pytest.mark.parametrize("terminal", ["complete", "fail"])
def test_cleanup_error_after_terminal_does_not_send_another_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
) -> None:
    job = _fixture_job()
    artifact, job = _artifact("job.json", job)
    result = _fixture_result(job) if terminal == "complete" else None
    config = _config(
        tmp_path,
        executor=_executor(tmp_path / "executor", result=result),
    )
    fake = FakeClient(_claim((artifact,)), {"job.json": job})
    worker = RemoteWorker(config, cast(ScoutWorkerClient, fake))

    def fail_cleanup(_run_root: Path, _claim_value: RemoteClaim) -> None:
        raise ValidationError("WORKER_CLEANUP_FAILED", "Fixture cleanup failure")

    monkeypatch.setattr(worker, "_cleanup_run_root", fail_cleanup)
    outcome = worker.run_once()

    assert outcome.state == ("completed" if terminal == "complete" else "failed")
    assert len(fake.completed) == (1 if terminal == "complete" else 0)
    assert len(fake.failed) == (0 if terminal == "complete" else 1)
    status = json.loads((config.state_root / "status.json").read_text(encoding="utf-8"))
    assert status["state"] == "cleanup_failed"
    assert status["last_error_code"] == "WORKER_CLEANUP_FAILED"
    assert (
        config.state_root / "runs/atlas-research-fixture-v1-job-1-7-0/atlas-research-run"
    ).is_dir()


def test_executor_process_group_is_cleaned_on_base_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _fixture_job()
    artifact, _job = _artifact("job.json", job)
    claim = _claim((artifact,))
    config = _config(tmp_path, executor=_executor(tmp_path / "executor", sleep=5))
    worker = RemoteWorker(config)
    run_root = worker._run_root(claim)

    class InterruptSupervisor:
        calls = 0

        def check(self) -> None:
            self.calls += 1
            if self.calls >= 2:
                raise KeyboardInterrupt

    terminated: list[object] = []
    original = worker._terminate

    def terminate(process: object) -> None:
        terminated.append(process)
        original(cast(Any, process))

    monkeypatch.setattr(worker, "_terminate", terminate)
    with pytest.raises(KeyboardInterrupt):
        worker._execute(_session(), claim, run_root, cast(Any, InterruptSupervisor()))

    assert len(terminated) == 1
    assert cast(Any, terminated[0]).poll() is not None


class _ProtocolHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    requests: ClassVar[list[tuple[str, str, str, object | None]]] = []
    request_headers: ClassVar[list[dict[str, str]]] = []
    artifact = b"fixture"
    artifact_sha256_override: ClassVar[str | None] = None
    claim_response: ClassVar[Mapping[str, object] | None] = None

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def _body(self) -> object | None:
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return None
        return json.loads(self.rfile.read(length))

    def _send(
        self, status: int, value: object | None = None, *, content_type: str = "application/json"
    ) -> None:
        body = b"" if value is None else canonical_json_bytes(value)
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Encoding", "identity")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_POST(self) -> None:
        body = self._body()
        authorization = self.headers.get("Authorization", "")
        self.requests.append(("POST", self.path, authorization, body))
        self.request_headers.append({key.lower(): value for key, value in self.headers.items()})
        if self.path == "/api/worker/v1/session":
            self._send(
                200,
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "session_id": SESSION_ID,
                    "session_token": SESSION_TOKEN,
                    "expires_at": _timestamp(),
                    "lease_seconds": 60,
                    "heartbeat_interval_seconds": 10,
                },
            )
        elif self.path == "/api/worker/v1/claim":
            if self.claim_response is None:
                self._send(204)
            else:
                self._send(200, self.claim_response)
        elif self.path == "/api/worker/v1/telemetry":
            now = datetime.now(UTC).replace(microsecond=123_000)
            self._send(
                200,
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "collected_at": _telemetry_timestamp(now),
                    "queue": {"pending": 7, "in_flight": 1, "failed": 2},
                    "totals": {"processed": 3, "failed": 1},
                    "history": [
                        {
                            "at": _telemetry_timestamp(now.replace(second=0, microsecond=0)),
                            "processed": 3,
                            "failed": 1,
                        }
                    ],
                },
                content_type="application/json; charset=utf-8",
            )
        elif self.path == "/api/worker/v1/heartbeat":
            self._send(200, {"cancelled": False, "lease_expires_at": _timestamp()})
        elif self.path in {"/api/worker/v1/complete", "/api/worker/v1/fail"}:
            self._send(200, {"accepted": True, "replayed": False})
        elif self.path == "/api/worker/v1/redirect":
            self.send_response(302)
            self.send_header("Location", "https://attacker.invalid/")
            self.send_header("Content-Length", "0")
            self.end_headers()
        else:
            self._send(404)

    def do_GET(self) -> None:
        authorization = self.headers.get("Authorization", "")
        self.requests.append(("GET", self.path, authorization, None))
        self.request_headers.append({key.lower(): value for key, value in self.headers.items()})
        if self.path != f"/api/worker/v1/objects/{JOB_OBJECT_ID}":
            self._send(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(self.artifact)))
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Encoding", "identity")
        self.send_header(
            "X-Content-SHA256",
            self.artifact_sha256_override or hashlib.sha256(self.artifact).hexdigest(),
        )
        self.end_headers()
        self.wfile.write(self.artifact)


class _SlowTelemetryHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    started = threading.Event()
    finished = threading.Event()
    delay_seconds: ClassVar[float] = 0.02
    stall_after_first_seconds: ClassVar[float] = 0.0

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        now = datetime.now(UTC).replace(microsecond=123_000)
        body = canonical_json_bytes(
            {
                "protocol_version": PROTOCOL_VERSION,
                "collected_at": _telemetry_timestamp(now),
                "queue": {"pending": 0, "in_flight": 0, "failed": 0},
                "totals": {"processed": 0, "failed": 0},
                "history": [],
            }
        )
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Encoding", "identity")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            for index, byte in enumerate(body):
                self.wfile.write(bytes((byte,)))
                self.wfile.flush()
                if index == 0:
                    self.started.set()
                    time.sleep(self.stall_after_first_seconds or self.delay_seconds)
                else:
                    time.sleep(self.delay_seconds)
        except OSError:
            return
        finally:
            self.finished.set()


def _open_descriptor_count() -> int | None:
    for directory in ("/proc/self/fd", "/dev/fd"):
        try:
            return len(os.listdir(directory))
        except OSError:
            continue
    return None


def test_interrupt_socket_duplication_supports_tls_wrappers() -> None:
    local, peer = socket.socketpair()
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    wrapped = context.wrap_socket(
        local,
        server_hostname="localhost",
        do_handshake_on_connect=False,
    )
    interrupt: socket.socket | None = None
    try:
        interrupt = _duplicate_interrupt_socket(wrapped)
        peer.settimeout(0.2)
        interrupt.shutdown(socket.SHUT_RDWR)
        assert peer.recv(1) == b""
    finally:
        if interrupt is not None:
            interrupt.close()
        wrapped.close()
        peer.close()


def test_resolver_output_is_closed_bounded_and_ip_only() -> None:
    valid = _resolved_addresses(
        [
            [socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "127.0.0.1", 443],
            [socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "::1", 443, 0, 0],
        ],
        443,
    )
    assert [item.sockaddr for item in valid] == [("127.0.0.1", 443), ("::1", 443, 0, 0)]

    invalid_values: list[object] = [
        [],
        [[socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "hostname", 443]],
        [[socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "127.0.0.1", 443.0]],
        [[socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_TCP, "127.0.0.1", 443]],
        [[socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "127.0.0.1", 443, 0]],
        [[socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "127.0.0.1", 443]] * 17,
    ]
    for value in invalid_values:
        with pytest.raises(ValidationError, match="resolver"):
            _resolved_addresses(value, 443)


@pytest.mark.parametrize("failure_mode", ["stall", "fast-fail"])
def test_multiaddress_connect_preserves_budget_for_live_second_address(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    total_timeout = 0.8
    monkeypatch.setattr(
        remote_worker_module,
        "_TELEMETRY_REQUEST_TIMEOUT_SECONDS",
        total_timeout,
    )

    class CloseProtocolHandler(_ProtocolHandler):
        protocol_version = "HTTP/1.0"

    server = ThreadingHTTPServer(("127.0.0.1", 0), CloseProtocolHandler)
    descriptor_count = _open_descriptor_count()
    _ProtocolHandler.requests = []
    _ProtocolHandler.request_headers = []
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    addresses = _resolved_addresses(
        [
            [
                socket.AF_INET,
                socket.SOCK_STREAM,
                socket.IPPROTO_TCP,
                "192.0.2.1",
                server.server_port,
            ],
            [
                socket.AF_INET,
                socket.SOCK_STREAM,
                socket.IPPROTO_TCP,
                "127.0.0.1",
                server.server_port,
            ],
        ],
        server.server_port,
    )
    monkeypatch.setattr(
        remote_worker_module,
        "_resolve_controller_addresses",
        lambda *_args, **_kwargs: addresses,
    )
    real_socket = socket.socket
    attempt_timeouts: list[float] = []
    failed_candidate_closed = threading.Event()

    class FailedCandidate:
        def settimeout(self, value: float) -> None:
            attempt_timeouts.append(value)

        def bind(self, _source_address: tuple[str, int]) -> None:
            return

        def connect(self, _address: object) -> None:
            if failure_mode == "stall":
                time.sleep(attempt_timeouts[-1])
                raise TimeoutError("simulated blackhole")
            raise ConnectionRefusedError("simulated fast failure")

        def shutdown(self, _how: int) -> None:
            return

        def close(self) -> None:
            failed_candidate_closed.set()

    failed_candidate_returned = False

    def socket_factory(*args: object, **kwargs: object) -> Any:
        nonlocal failed_candidate_returned
        if not failed_candidate_returned and "fileno" not in kwargs:
            failed_candidate_returned = True
            return FailedCandidate()
        return real_socket(*args, **kwargs)

    monkeypatch.setattr(remote_worker_module.socket, "socket", socket_factory)
    try:
        base = _config(tmp_path)
        config = WorkerConfig(
            controller_url=f"http://127.0.0.1:{server.server_port}",
            worker_id=base.worker_id,
            enrollment_token_file=base.enrollment_token_file,
            state_root=base.state_root,
            executor_path=base.executor_path,
            poll_seconds=base.poll_seconds,
            request_timeout_seconds=base.request_timeout_seconds,
            max_job_seconds=base.max_job_seconds,
            max_bundle_bytes=base.max_bundle_bytes,
            telemetry_file=base.telemetry_file,
        )
        started_at = time.monotonic()
        assert ScoutWorkerClient(config).telemetry(_session()).pending == 7
        elapsed = time.monotonic() - started_at
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)

    assert elapsed < total_timeout
    assert len(attempt_timeouts) == 1
    assert 0 < attempt_timeouts[0] <= total_timeout / 2
    assert failed_candidate_closed.is_set()
    assert [request[1] for request in _ProtocolHandler.requests] == ["/api/worker/v1/telemetry"]
    if descriptor_count is not None:
        assert cast(int, _open_descriptor_count()) <= descriptor_count
    assert not any(
        thread.name == "atlas-research-worker-telemetry-request-watchdog"
        for thread in threading.enumerate()
    )


def test_resolver_stall_obeys_hard_deadline_and_reaps_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(remote_worker_module, "_TELEMETRY_REQUEST_TIMEOUT_SECONDS", 0.25)
    monkeypatch.setattr(
        remote_worker_module,
        "_RESOLVER_PROGRAM",
        "import time\ntime.sleep(2)\n",
    )
    real_popen = remote_worker_module.subprocess.Popen
    processes: list[Any] = []
    popen_kwargs: list[Mapping[str, object]] = []

    def observed_popen(*args: object, **kwargs: object) -> Any:
        process = real_popen(*args, **kwargs)
        processes.append(process)
        popen_kwargs.append(kwargs)
        return process

    monkeypatch.setattr(remote_worker_module.subprocess, "Popen", observed_popen)
    descriptor_count = _open_descriptor_count()
    client = ScoutWorkerClient(_config(tmp_path))

    started_at = time.monotonic()
    with pytest.raises(RemoteWorkerError) as captured:
        client.telemetry(_session())
    elapsed = time.monotonic() - started_at

    assert captured.value.code == "WORKER_CONTROLLER_UNAVAILABLE"
    assert elapsed < 0.4
    assert len(processes) == 1
    assert processes[0].poll() is not None
    assert popen_kwargs[0]["env"] == {}
    assert SESSION_TOKEN not in repr(processes[0].args)
    if descriptor_count is not None:
        assert cast(int, _open_descriptor_count()) <= descriptor_count
    assert not any(
        thread.name == "atlas-research-worker-telemetry-request-watchdog"
        for thread in threading.enumerate()
    )


def test_publisher_cancel_terminates_stalled_resolver_without_leaks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(remote_worker_module, "_TELEMETRY_REQUEST_TIMEOUT_SECONDS", 1.0)
    monkeypatch.setattr(
        remote_worker_module,
        "_RESOLVER_PROGRAM",
        "import time\ntime.sleep(2)\n",
    )
    real_popen = remote_worker_module.subprocess.Popen
    resolver_started = threading.Event()
    processes: list[Any] = []

    def observed_popen(*args: object, **kwargs: object) -> Any:
        process = real_popen(*args, **kwargs)
        processes.append(process)
        resolver_started.set()
        return process

    monkeypatch.setattr(remote_worker_module.subprocess, "Popen", observed_popen)
    descriptor_count = _open_descriptor_count()
    config = _config(tmp_path, telemetry=True)
    worker = RemoteWorker(config)
    worker._session = _session()
    publisher = _TelemetryPublisher(worker._publish_telemetry_once, 60.0)
    publisher.start()
    assert resolver_started.wait(timeout=0.5)

    started_at = time.monotonic()
    assert publisher.stop() is True
    elapsed = time.monotonic() - started_at

    assert elapsed < 0.25
    assert len(processes) == 1
    assert processes[0].poll() is not None
    assert SESSION_TOKEN not in repr(processes[0].args)
    if descriptor_count is not None:
        assert cast(int, _open_descriptor_count()) <= descriptor_count
    assert not any(
        thread.name
        in {
            "atlas-research-worker-telemetry",
            "atlas-research-worker-telemetry-request-watchdog",
        }
        for thread in threading.enumerate()
    )


def test_resolved_https_connection_preserves_host_header_and_sni(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ProtocolHandler)
    _ProtocolHandler.requests = []
    _ProtocolHandler.request_headers = []
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    server_names: list[str | None] = []

    class PlaintextTLSContext:
        verify_mode = ssl.CERT_REQUIRED
        check_hostname = True

        def wrap_socket(
            self,
            active_socket: socket.socket,
            *,
            server_hostname: str | None,
        ) -> socket.socket:
            server_names.append(server_hostname)
            return active_socket

    monkeypatch.setattr(
        remote_worker_module.ssl,
        "create_default_context",
        lambda: PlaintextTLSContext(),
    )
    try:
        base = _config(tmp_path)
        config = WorkerConfig(
            controller_url=f"https://localhost:{server.server_port}",
            worker_id=base.worker_id,
            enrollment_token_file=base.enrollment_token_file,
            state_root=base.state_root,
            executor_path=base.executor_path,
            poll_seconds=base.poll_seconds,
            request_timeout_seconds=base.request_timeout_seconds,
            max_job_seconds=base.max_job_seconds,
            max_bundle_bytes=base.max_bundle_bytes,
        )

        assert ScoutWorkerClient(config).telemetry(_session()).pending == 7
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)

    assert server_names == ["localhost"]
    assert _ProtocolHandler.request_headers[0]["host"] == f"localhost:{server.server_port}"


def test_telemetry_request_has_hard_wall_clock_deadline_against_slow_drip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(remote_worker_module, "_TELEMETRY_REQUEST_TIMEOUT_SECONDS", 0.25)
    _SlowTelemetryHandler.started.clear()
    _SlowTelemetryHandler.finished.clear()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _SlowTelemetryHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        descriptor_count = _open_descriptor_count()
        base = _config(tmp_path)
        config = WorkerConfig(
            controller_url=f"http://127.0.0.1:{server.server_port}",
            worker_id=base.worker_id,
            enrollment_token_file=base.enrollment_token_file,
            state_root=base.state_root,
            executor_path=base.executor_path,
            poll_seconds=base.poll_seconds,
            request_timeout_seconds=base.request_timeout_seconds,
            max_job_seconds=base.max_job_seconds,
            max_bundle_bytes=base.max_bundle_bytes,
        )
        client = ScoutWorkerClient(config)
        started_at = time.monotonic()
        with pytest.raises(RemoteWorkerError) as captured:
            client.telemetry(_session())
        elapsed = time.monotonic() - started_at

        assert captured.value.code == "WORKER_CONTROLLER_UNAVAILABLE"
        assert elapsed < 0.4
        assert SESSION_TOKEN not in repr(captured.value)
        assert _SlowTelemetryHandler.finished.wait(timeout=0.2)
        time.sleep(0.02)
        if descriptor_count is not None:
            assert cast(int, _open_descriptor_count()) <= descriptor_count + 1
        assert not any(
            thread.name == "atlas-research-worker-telemetry-request-watchdog"
            for thread in threading.enumerate()
        )
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)


def test_telemetry_publisher_shutdown_interrupts_active_request_without_thread_leak(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(remote_worker_module, "_TELEMETRY_REQUEST_TIMEOUT_SECONDS", 1.0)
    _SlowTelemetryHandler.started.clear()
    _SlowTelemetryHandler.finished.clear()
    _SlowTelemetryHandler.stall_after_first_seconds = 1.0
    server = ThreadingHTTPServer(("127.0.0.1", 0), _SlowTelemetryHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        descriptor_count = _open_descriptor_count()
        base = _config(tmp_path, telemetry=True)
        config = WorkerConfig(
            controller_url=f"http://127.0.0.1:{server.server_port}",
            worker_id=base.worker_id,
            enrollment_token_file=base.enrollment_token_file,
            state_root=base.state_root,
            executor_path=base.executor_path,
            poll_seconds=base.poll_seconds,
            request_timeout_seconds=base.request_timeout_seconds,
            max_job_seconds=base.max_job_seconds,
            max_bundle_bytes=base.max_bundle_bytes,
            telemetry_file=base.telemetry_file,
        )
        worker = RemoteWorker(config)
        worker._session = _session()
        publisher = _TelemetryPublisher(worker._publish_telemetry_once, 60.0)
        publisher.start()
        assert _SlowTelemetryHandler.started.wait(timeout=1)
        time.sleep(0.05)

        started_at = time.monotonic()
        assert publisher.stop() is True
        elapsed = time.monotonic() - started_at

        assert elapsed < 0.25
        if descriptor_count is not None:
            assert cast(int, _open_descriptor_count()) <= descriptor_count + 1
        assert not any(
            thread.name
            in {
                "atlas-research-worker-telemetry",
                "atlas-research-worker-telemetry-request-watchdog",
            }
            for thread in threading.enumerate()
        )
    finally:
        _SlowTelemetryHandler.stall_after_first_seconds = 0.0
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)


@pytest.mark.parametrize("terminal", ["complete", "fail"])
@pytest.mark.parametrize(
    ("body", "ambiguous"),
    [
        (b'{"accepted":true,"replayed":true}', False),
        (b"", True),
        (b"{", True),
        (b'{"accepted":false,"replayed":false}', True),
        (b'{"accepted":true,"replayed":"false"}', True),
        (b'{"accepted":true,"extra":1,"replayed":false}', True),
    ],
)
def test_terminal_ack_is_closed_and_ambiguous_responses_never_succeed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
    body: bytes,
    ambiguous: bool,
) -> None:
    client = ScoutWorkerClient(_config(tmp_path))
    job = _fixture_job()
    claim = _claim((_artifact("job.json", job)[0],))
    result = _fixture_result(job)
    digest = hashlib.sha256(canonical_json_bytes(result)).hexdigest()
    monkeypatch.setattr(client, "_request", lambda *_args, **_kwargs: (200, body, {}))

    def invoke() -> None:
        if terminal == "complete":
            client.complete(_session(), claim, result, digest)
        else:
            client.fail(_session(), claim, code="FIXTURE_FAILURE", retryable=False)

    if ambiguous:
        with pytest.raises(RemoteWorkerError) as captured:
            invoke()
        assert captured.value.code == "WORKER_TERMINAL_AMBIGUOUS"
    else:
        invoke()


def test_client_telemetry_requires_closed_body_and_exact_response_headers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = ScoutWorkerClient(_config(tmp_path))
    now = datetime.now(UTC).replace(microsecond=123_000)
    value: dict[str, object] = {
        "protocol_version": PROTOCOL_VERSION,
        "collected_at": _telemetry_timestamp(now),
        "queue": {"pending": 0, "in_flight": 0, "failed": 0},
        "totals": {"processed": 0, "failed": 0},
        "history": [],
    }
    headers = {
        "content-type": "application/json; charset=utf-8",
        "content-encoding": "identity",
        "cache-control": "no-store",
    }
    monkeypatch.setattr(
        client,
        "_request",
        lambda *_args, **_kwargs: (200, canonical_json_bytes(value), headers),
    )
    assert client.telemetry(_session()).processed == 0

    extended = dict(value)
    extended["job_id"] = "secret-job"
    monkeypatch.setattr(
        client,
        "_request",
        lambda *_args, **_kwargs: (200, canonical_json_bytes(extended), headers),
    )
    with pytest.raises(ValidationError, match="fields"):
        client.telemetry(_session())

    monkeypatch.setattr(
        client,
        "_request",
        lambda *_args, **_kwargs: (
            200,
            canonical_json_bytes(value),
            {"content-type": "application/json; charset=utf-8"},
        ),
    )
    with pytest.raises(ValidationError, match="headers"):
        client.telemetry(_session())


def test_http_client_uses_bearer_tokens_and_never_follows_redirects(tmp_path: Path) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ProtocolHandler)
    _ProtocolHandler.requests = []
    _ProtocolHandler.request_headers = []
    _ProtocolHandler.claim_response = None
    _ProtocolHandler.artifact_sha256_override = None
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        config = _config(tmp_path)
        config = WorkerConfig(
            controller_url=f"http://127.0.0.1:{server.server_port}",
            worker_id=config.worker_id,
            enrollment_token_file=config.enrollment_token_file,
            state_root=config.state_root,
            executor_path=config.executor_path,
            poll_seconds=config.poll_seconds,
            request_timeout_seconds=config.request_timeout_seconds,
            max_job_seconds=config.max_job_seconds,
            max_bundle_bytes=config.max_bundle_bytes,
        )
        client = ScoutWorkerClient(config)
        session = client.exchange_session()
        assert client.claim(session) is None
        telemetry = client.telemetry(session)
        assert telemetry.pending == 7
        artifact = RemoteArtifact(
            path="fixture.json",
            sha256=hashlib.sha256(_ProtocolHandler.artifact).hexdigest(),
            size_bytes=len(_ProtocolHandler.artifact),
            download_path=f"/api/worker/v1/objects/{JOB_OBJECT_ID}",
        )
        assert client.download(session, artifact) == _ProtocolHandler.artifact
        job = _fixture_job()
        claim = _claim((_artifact("job.json", job)[0],))
        heartbeat = client.heartbeat(session, claim, 1)
        assert heartbeat.cancelled is False
        result = _fixture_result(job)
        digest = hashlib.sha256(canonical_json_bytes(result)).hexdigest()
        client.complete(session, claim, result, digest)
        client.fail(session, claim, code="FIXTURE_FAILURE", retryable=False)
        with pytest.raises(RemoteWorkerError, match="rejected"):
            client._request(
                "POST",
                "/api/worker/v1/redirect",
                token=session.token,
                payload={"protocol_version": PROTOCOL_VERSION},
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        _ProtocolHandler.claim_response = None
        _ProtocolHandler.artifact_sha256_override = None

    assert _ProtocolHandler.requests[0][2] == f"Bearer {ENROLLMENT_TOKEN}"
    assert _ProtocolHandler.requests[1][2] == f"Bearer {SESSION_TOKEN}"
    assert _ProtocolHandler.requests[0][3] == {
        "protocol_version": PROTOCOL_VERSION,
        "worker_id": "mac-mini-test",
    }
    assert [request[1] for request in _ProtocolHandler.requests[:7]] == [
        "/api/worker/v1/session",
        "/api/worker/v1/claim",
        "/api/worker/v1/telemetry",
        f"/api/worker/v1/objects/{JOB_OBJECT_ID}",
        "/api/worker/v1/heartbeat",
        "/api/worker/v1/complete",
        "/api/worker/v1/fail",
    ]
    assert _ProtocolHandler.requests[2][3] == {
        "protocol_version": PROTOCOL_VERSION,
        "worker_id": "mac-mini-test",
        "session_id": SESSION_ID,
    }
    heartbeat_body = cast(dict[str, object], _ProtocolHandler.requests[4][3])
    assert heartbeat_body["heartbeat_sequence"] == 1
    fail_body = cast(dict[str, object], _ProtocolHandler.requests[6][3])
    assert fail_body["code"] == "FIXTURE_FAILURE"
    assert "error_code" not in fail_body
    object_headers = _ProtocolHandler.request_headers[3]
    assert "x-atlas-worker-id" not in object_headers
    assert "x-atlas-worker-session-id" not in object_headers
    assert all("attacker.invalid" not in request[1] for request in _ProtocolHandler.requests)


def test_malformed_authenticated_artifact_response_backs_off_without_fail(tmp_path: Path) -> None:
    job = _fixture_job()
    digest = hashlib.sha256(job).hexdigest()
    _ProtocolHandler.requests = []
    _ProtocolHandler.request_headers = []
    _ProtocolHandler.artifact = job
    _ProtocolHandler.artifact_sha256_override = "0" * 64
    _ProtocolHandler.claim_response = {
        "protocol_version": PROTOCOL_VERSION,
        "job_id": "atlas-research-fixture-v1-job",
        "workload_type": "research.experiment",
        "attempt": 1,
        "fence": 7,
        "cancellation_generation": 0,
        "lease_expires_at": _timestamp(),
        "artifacts": [
            {
                "path": "job.json",
                "sha256": digest,
                "size_bytes": len(job),
                "download_path": f"/api/worker/v1/objects/{JOB_OBJECT_ID}",
            }
        ],
        "job_path": "job.json",
    }
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ProtocolHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = _config(tmp_path)
        config = WorkerConfig(
            controller_url=f"http://127.0.0.1:{server.server_port}",
            worker_id=base.worker_id,
            enrollment_token_file=base.enrollment_token_file,
            state_root=base.state_root,
            executor_path=base.executor_path,
            poll_seconds=base.poll_seconds,
            request_timeout_seconds=base.request_timeout_seconds,
            max_job_seconds=base.max_job_seconds,
            max_bundle_bytes=base.max_bundle_bytes,
        )
        worker = RemoteWorker(config)

        with pytest.raises(RemoteWorkerError) as captured:
            worker.run_once()

        assert captured.value.code == "WORKER_ARTIFACT_TRANSFER_INVALID"
        paths = [request[1] for request in _ProtocolHandler.requests]
        assert paths[:4] == [
            "/api/worker/v1/session",
            "/api/worker/v1/claim",
            "/api/worker/v1/heartbeat",
            f"/api/worker/v1/objects/{JOB_OBJECT_ID}",
        ]
        assert "/api/worker/v1/fail" not in paths
        status = json.loads((config.state_root / "status.json").read_text(encoding="utf-8"))
        assert status["state"] == "backoff"
        assert status["last_error_code"] == "WORKER_ARTIFACT_TRANSFER_INVALID"
        assert (
            config.state_root / "runs/atlas-research-fixture-v1-job-1-7-0/atlas-research-run"
        ).is_dir()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        _ProtocolHandler.claim_response = None
        _ProtocolHandler.artifact_sha256_override = None
        _ProtocolHandler.artifact = b"fixture"


def test_executor_symlink_and_world_writable_file_are_rejected(tmp_path: Path) -> None:
    executable = _executor(tmp_path / "executor")
    link = tmp_path / "executor-link"
    link.symlink_to(executable)
    linked_root = tmp_path / "linked"
    linked_root.mkdir()
    with pytest.raises(ValidationError, match="unsafe"):
        _config(linked_root, executor=link)

    executable.chmod(0o722)
    token = _write_private(tmp_path / "token", f"{ENROLLMENT_TOKEN}\n")
    state = tmp_path / "state-extra"
    state.mkdir(mode=0o700)
    with pytest.raises(ValidationError, match="unsafe"):
        WorkerConfig.from_mapping(
            {
                "protocol_version": PROTOCOL_VERSION,
                "controller_url": "http://127.0.0.1:8123",
                "worker_id": "mac-mini-test",
                "enrollment_token_file": str(token),
                "state_root": str(state),
                "executor_path": str(executable),
            }
        )

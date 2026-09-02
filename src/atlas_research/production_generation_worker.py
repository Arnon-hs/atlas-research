# SPDX-License-Identifier: MIT
"""Outbound Mac/Linux worker for Scout-owned production generation leases."""

from __future__ import annotations

import hashlib
import os
import re
import signal
import stat
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final, cast

from .artifacts import atomic_write_private, ensure_private_directory, read_private_bytes
from .canonical import canonical_json_bytes, strict_json_loads
from .errors import AtlasResearchError, ValidationError
from .production_generation import (
    ProductionGenerationJob,
    build_production_generation_result,
    execute_production_generation,
    parse_production_generation_job,
    validate_production_generation_result,
)
from .qwen import QwenError, QwenTransport
from .remote_worker import (
    RemoteWorkerError,
    ScoutWorkerClient,
    WorkerConfig,
    _controller_origin,
    _exact_fields,
    _mapping,
    _read_bounded_private_file,
    _read_secret_file,
    _terminal_ack_from_mapping,
    _timestamp,
)

PROTOCOL_VERSION: Final = "1"
API_PREFIX: Final = "/api/production-generation-worker/v1"
STATUS_SCHEMA_VERSION: Final = "atlas-production-generation-worker-status.v1"
RECEIPT_CONTRACT: Final = "atlas-production-generation-terminal-receipt.v1"
_MAX_CONTROL_BYTES: Final = 512 << 10
_MAX_RESULT_BYTES: Final = 256 << 10
_IDENTIFIER: Final = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,127}$", re.ASCII)
_RELEASE_ID: Final = re.compile(r"^pgr_release_[0-9a-f]{32}$", re.ASCII)
_TOKEN: Final = re.compile(r"^[A-Za-z0-9._~-]{32,512}$", re.ASCII)
_SHA256: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_RESULT_CACHE_NAME: Final = re.compile(
    r"^(?P<job>[a-z0-9][a-z0-9._:-]{0,127})-(?P<attempt>1)-"
    r"(?P<fence>[1-9][0-9]{0,15})-(?P<cancellation>[0-9]{1,16})-"
    r"(?P<assignment>[0-9a-f]{64})\.json$",
    re.ASCII,
)


@dataclass(frozen=True, slots=True)
class ProductionGenerationWorkerConfig:
    controller_url: str
    worker_id: str
    release_id: str
    model_revision: str
    enrollment_token_file: Path
    state_root: Path
    poll_seconds: float = 5.0
    request_timeout_seconds: float = 30.0

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> ProductionGenerationWorkerConfig:
        required = {
            "protocol_version",
            "controller_url",
            "worker_id",
            "release_id",
            "model_revision",
            "enrollment_token_file",
            "state_root",
        }
        optional = {"poll_seconds", "request_timeout_seconds"}
        if not required.issubset(value) or not set(value).issubset(required | optional):
            raise ValidationError(
                "GENERATION_WORKER_CONFIG_INVALID", "Generation worker config fields are invalid"
            )
        if value.get("protocol_version") != PROTOCOL_VERSION:
            raise ValidationError(
                "GENERATION_WORKER_CONFIG_INVALID", "Generation worker protocol is invalid"
            )
        controller = _required_string(value, "controller_url")
        _controller_origin(controller)
        worker_id = _required_string(value, "worker_id")
        release_id = _required_string(value, "release_id")
        model_revision = _required_string(value, "model_revision")
        if _IDENTIFIER.fullmatch(worker_id) is None or _RELEASE_ID.fullmatch(release_id) is None:
            raise ValidationError(
                "GENERATION_WORKER_CONFIG_INVALID", "Generation worker identity is invalid"
            )
        if _SHA256.fullmatch(model_revision) is None:
            raise ValidationError(
                "GENERATION_WORKER_CONFIG_INVALID", "Generation worker model revision is invalid"
            )
        poll_seconds = _number(value.get("poll_seconds", 5.0), 0.1, 300.0)
        request_timeout = _number(value.get("request_timeout_seconds", 30.0), 1.0, 60.0)
        config = cls(
            controller_url=controller,
            worker_id=worker_id,
            release_id=release_id,
            model_revision=model_revision,
            enrollment_token_file=_absolute_path(value, "enrollment_token_file"),
            state_root=_absolute_path(value, "state_root"),
            poll_seconds=poll_seconds,
            request_timeout_seconds=request_timeout,
        )
        config.validate_local_paths()
        return config

    def validate_local_paths(self) -> None:
        _read_secret_file(self.enrollment_token_file)
        ensure_private_directory(self.state_root)


@dataclass(frozen=True, slots=True)
class ProductionGenerationSession:
    session_id: str
    token: str
    expires_at: datetime
    lease_seconds: int
    heartbeat_interval_seconds: int


@dataclass(frozen=True, slots=True)
class ProductionGenerationClaim:
    job: ProductionGenerationJob
    attempt: int
    fence: int
    cancellation_generation: int
    lease_expires_at: datetime
    assignment_sha256: str

    def identity(
        self, config: ProductionGenerationWorkerConfig, session_id: str
    ) -> dict[str, object]:
        return {
            "protocol_version": PROTOCOL_VERSION,
            "worker_id": config.worker_id,
            "release_id": config.release_id,
            "session_id": session_id,
            "job_id": self.job.job_id,
            "attempt": self.attempt,
            "fence": self.fence,
            "cancellation_generation": self.cancellation_generation,
            "assignment_sha256": self.assignment_sha256,
        }


@dataclass(frozen=True, slots=True)
class ProductionGenerationHeartbeat:
    cancelled: bool
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class ProductionGenerationRunOutcome:
    state: str
    job_id: str | None = None
    result_sha256: str | None = None
    error_code: str | None = None

    def to_mapping(self) -> dict[str, object]:
        return {
            "state": self.state,
            "job_id": self.job_id,
            "result_sha256": self.result_sha256,
            "error_code": self.error_code,
        }


@dataclass(frozen=True, slots=True)
class ProductionGenerationCacheIdentity:
    name: str
    job_id: str
    workload_type: str
    target: Mapping[str, object]
    attempt: int
    fence: int
    cancellation_generation: int
    assignment_sha256: str
    result_sha256: str


def _required_string(value: Mapping[str, object], key: str) -> str:
    selected = value.get(key)
    if not isinstance(selected, str) or not selected:
        raise ValidationError(
            "GENERATION_WORKER_CONFIG_INVALID", "Generation worker string is invalid"
        )
    return selected


def _absolute_path(value: Mapping[str, object], key: str) -> Path:
    selected = _required_string(value, key)
    path = Path(selected)
    if not path.is_absolute() or "\x00" in selected:
        raise ValidationError(
            "GENERATION_WORKER_CONFIG_INVALID", "Generation worker path is invalid"
        )
    return path


def _number(value: object, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(
            "GENERATION_WORKER_CONFIG_INVALID", "Generation worker number is invalid"
        )
    result = float(value)
    if not minimum <= result <= maximum:
        raise ValidationError(
            "GENERATION_WORKER_CONFIG_INVALID", "Generation worker number is invalid"
        )
    return result


def _integer(value: object, minimum: int, maximum: int, code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValidationError(code, "Generation worker integer is invalid")
    return value


def _parse_session(
    value: object, config: ProductionGenerationWorkerConfig
) -> ProductionGenerationSession:
    session = _mapping(value, "GENERATION_WORKER_SESSION_INVALID")
    _exact_fields(
        session,
        {
            "protocol_version",
            "session_id",
            "session_token",
            "worker_id",
            "release_id",
            "expires_at",
            "lease_seconds",
            "heartbeat_interval_seconds",
            "max_concurrency",
        },
        "GENERATION_WORKER_SESSION_INVALID",
    )
    session_id = session.get("session_id")
    token = session.get("session_token")
    if (
        session.get("protocol_version") != PROTOCOL_VERSION
        or session.get("worker_id") != config.worker_id
        or session.get("release_id") != config.release_id
        or session.get("max_concurrency") != 1
        or not isinstance(session_id, str)
        or _IDENTIFIER.fullmatch(session_id) is None
        or not isinstance(token, str)
        or _TOKEN.fullmatch(token) is None
    ):
        raise ValidationError(
            "GENERATION_WORKER_SESSION_INVALID", "Generation worker session is invalid"
        )
    lease_seconds = _integer(
        session.get("lease_seconds"), 30, 900, "GENERATION_WORKER_SESSION_INVALID"
    )
    heartbeat = _integer(
        session.get("heartbeat_interval_seconds"),
        10,
        lease_seconds,
        "GENERATION_WORKER_SESSION_INVALID",
    )
    return ProductionGenerationSession(
        session_id=session_id,
        token=token,
        expires_at=_timestamp(session.get("expires_at"), "GENERATION_WORKER_SESSION_INVALID"),
        lease_seconds=lease_seconds,
        heartbeat_interval_seconds=heartbeat,
    )


def _assignment_sha256(
    raw_job: Mapping[str, object],
    config: ProductionGenerationWorkerConfig,
    *,
    attempt: int,
    fence: int,
    cancellation_generation: int,
) -> str:
    payload = {
        "protocol_version": PROTOCOL_VERSION,
        "job": dict(raw_job),
        "worker_id": config.worker_id,
        "release_id": config.release_id,
        "attempt": attempt,
        "fence": fence,
        "cancellation_generation": cancellation_generation,
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _parse_claim(
    value: object, config: ProductionGenerationWorkerConfig
) -> ProductionGenerationClaim:
    claim = _mapping(value, "GENERATION_WORKER_CLAIM_INVALID")
    _exact_fields(
        claim,
        {
            "protocol_version",
            "job",
            "worker_id",
            "release_id",
            "session_id",
            "attempt",
            "fence",
            "cancellation_generation",
            "lease_expires_at",
            "assignment_sha256",
        },
        "GENERATION_WORKER_CLAIM_INVALID",
    )
    raw_job = _mapping(claim.get("job"), "GENERATION_WORKER_CLAIM_INVALID")
    job = parse_production_generation_job(raw_job)
    if canonical_json_bytes(raw_job) != canonical_json_bytes(job.to_mapping()):
        raise ValidationError(
            "GENERATION_WORKER_CLAIM_INVALID", "Generation job normalization drifted"
        )
    attempt = _integer(claim.get("attempt"), 1, 1, "GENERATION_WORKER_CLAIM_INVALID")
    fence = _integer(claim.get("fence"), 1, (1 << 53) - 1, "GENERATION_WORKER_CLAIM_INVALID")
    cancellation = _integer(
        claim.get("cancellation_generation"),
        0,
        (1 << 53) - 1,
        "GENERATION_WORKER_CLAIM_INVALID",
    )
    assignment = claim.get("assignment_sha256")
    if (
        claim.get("protocol_version") != PROTOCOL_VERSION
        or claim.get("worker_id") != config.worker_id
        or claim.get("release_id") != config.release_id
        or not isinstance(claim.get("session_id"), str)
        or _IDENTIFIER.fullmatch(cast(str, claim.get("session_id"))) is None
        or not isinstance(assignment, str)
        or _SHA256.fullmatch(assignment) is None
        or assignment
        != _assignment_sha256(
            raw_job,
            config,
            attempt=attempt,
            fence=fence,
            cancellation_generation=cancellation,
        )
    ):
        raise ValidationError(
            "GENERATION_WORKER_CLAIM_INVALID", "Generation assignment identity is invalid"
        )
    return ProductionGenerationClaim(
        job=job,
        attempt=attempt,
        fence=fence,
        cancellation_generation=cancellation,
        lease_expires_at=_timestamp(
            claim.get("lease_expires_at"), "GENERATION_WORKER_CLAIM_INVALID"
        ),
        assignment_sha256=assignment,
    )


def _parse_receipt(
    value: object,
    config: ProductionGenerationWorkerConfig,
    identity: ProductionGenerationCacheIdentity,
) -> Mapping[str, object]:
    receipt = _mapping(value, "GENERATION_WORKER_RECEIPT_INVALID")
    fields = {
        "contract_version",
        "receipt_sha256",
        "job_id",
        "workload_type",
        "target",
        "terminal_kind",
        "worker_id",
        "release_id",
        "session_id",
        "attempt",
        "fence",
        "cancellation_generation",
        "assignment_sha256",
        "request_sha256",
        "result_sha256",
        "failure_code",
        "failure_retryable",
        "terminal_at",
    }
    _exact_fields(receipt, fields, "GENERATION_WORKER_RECEIPT_INVALID")
    receipt_sha256 = receipt.get("receipt_sha256")
    request_sha256 = receipt.get("request_sha256")
    session_id = receipt.get("session_id")
    terminal_kind = receipt.get("terminal_kind")
    if (
        receipt.get("contract_version") != RECEIPT_CONTRACT
        or receipt.get("job_id") != identity.job_id
        or receipt.get("workload_type") != identity.workload_type
        or receipt.get("target") != identity.target
        or terminal_kind not in {"completed", "failed", "cancelled"}
        or receipt.get("worker_id") != config.worker_id
        or receipt.get("release_id") != config.release_id
        or not isinstance(session_id, str)
        or _IDENTIFIER.fullmatch(session_id) is None
        or receipt.get("attempt") != identity.attempt
        or receipt.get("fence") != identity.fence
        or (
            receipt.get("cancellation_generation")
            != identity.cancellation_generation + (1 if terminal_kind == "cancelled" else 0)
        )
        or receipt.get("assignment_sha256") != identity.assignment_sha256
        or not isinstance(request_sha256, str)
        or _SHA256.fullmatch(request_sha256) is None
        or not isinstance(receipt_sha256, str)
        or _SHA256.fullmatch(receipt_sha256) is None
    ):
        raise ValidationError(
            "GENERATION_WORKER_RECEIPT_INVALID", "Generation terminal receipt is invalid"
        )
    _timestamp(receipt.get("terminal_at"), "GENERATION_WORKER_RECEIPT_INVALID")
    result_sha256 = receipt.get("result_sha256")
    failure_code = receipt.get("failure_code")
    failure_retryable = receipt.get("failure_retryable")
    if terminal_kind == "completed":
        if (
            result_sha256 != identity.result_sha256
            or failure_code is not None
            or failure_retryable is not None
        ):
            raise ValidationError(
                "GENERATION_WORKER_RECEIPT_INVALID",
                "Completed generation receipt does not match the cached result",
            )
    elif terminal_kind == "failed" and (
        result_sha256 is not None
        or not isinstance(failure_code, str)
        or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", failure_code) is None
        or not isinstance(failure_retryable, bool)
    ):
        raise ValidationError(
            "GENERATION_WORKER_RECEIPT_INVALID", "Failed generation receipt is invalid"
        )
    elif terminal_kind == "cancelled" and (
        result_sha256 is not None or failure_code is not None or failure_retryable is not None
    ):
        raise ValidationError(
            "GENERATION_WORKER_RECEIPT_INVALID", "Cancelled generation receipt is invalid"
        )
    without_digest = {key: receipt[key] for key in fields if key != "receipt_sha256"}
    if hashlib.sha256(canonical_json_bytes(without_digest)).hexdigest() != receipt_sha256:
        raise ValidationError(
            "GENERATION_WORKER_RECEIPT_INVALID", "Generation receipt digest is invalid"
        )
    return dict(receipt)


class ProductionGenerationClient:
    """Exact worker-side projection of Scout's generation control plane."""

    def __init__(self, config: ProductionGenerationWorkerConfig) -> None:
        self.config = config
        self._http = ScoutWorkerClient(cast(WorkerConfig, config))

    def exchange_session(self) -> ProductionGenerationSession:
        token = _read_secret_file(self.config.enrollment_token_file)
        _status, body, _headers = self._http._request(
            "POST",
            f"{API_PREFIX}/session",
            token=token,
            payload={
                "protocol_version": PROTOCOL_VERSION,
                "worker_id": self.config.worker_id,
                "release_id": self.config.release_id,
            },
            maximum=_MAX_CONTROL_BYTES,
            hard_deadline_seconds=self.config.request_timeout_seconds,
        )
        parsed = strict_json_loads(body, max_bytes=_MAX_CONTROL_BYTES)
        return _parse_session(parsed, self.config)

    def claim(self, session: ProductionGenerationSession) -> ProductionGenerationClaim | None:
        status, body, _headers = self._http._request(
            "POST",
            f"{API_PREFIX}/claim",
            token=session.token,
            payload={
                "protocol_version": PROTOCOL_VERSION,
                "worker_id": self.config.worker_id,
                "release_id": self.config.release_id,
                "session_id": session.session_id,
            },
            expected=(200, 204),
            maximum=_MAX_CONTROL_BYTES,
            hard_deadline_seconds=self.config.request_timeout_seconds,
        )
        if status == 204:
            if body:
                raise ValidationError(
                    "GENERATION_WORKER_PROTOCOL_INVALID", "Empty generation claim contained a body"
                )
            return None
        value = _mapping(
            strict_json_loads(body, max_bytes=_MAX_CONTROL_BYTES),
            "GENERATION_WORKER_CLAIM_INVALID",
        )
        if value.get("session_id") != session.session_id:
            raise ValidationError(
                "GENERATION_WORKER_CLAIM_INVALID", "Generation claim session is invalid"
            )
        return _parse_claim(value, self.config)

    def heartbeat(
        self,
        session: ProductionGenerationSession,
        claim: ProductionGenerationClaim,
        sequence: int,
    ) -> ProductionGenerationHeartbeat:
        payload = claim.identity(self.config, session.session_id)
        payload["heartbeat_sequence"] = sequence
        _status, body, _headers = self._http._request(
            "POST",
            f"{API_PREFIX}/heartbeat",
            token=session.token,
            payload=payload,
            maximum=_MAX_CONTROL_BYTES,
            hard_deadline_seconds=self.config.request_timeout_seconds,
        )
        response = _mapping(
            strict_json_loads(body, max_bytes=_MAX_CONTROL_BYTES),
            "GENERATION_WORKER_HEARTBEAT_INVALID",
        )
        _exact_fields(
            response,
            {"cancelled", "lease_expires_at"},
            "GENERATION_WORKER_HEARTBEAT_INVALID",
        )
        if not isinstance(response.get("cancelled"), bool):
            raise ValidationError(
                "GENERATION_WORKER_HEARTBEAT_INVALID", "Generation heartbeat is invalid"
            )
        return ProductionGenerationHeartbeat(
            cancelled=cast(bool, response["cancelled"]),
            lease_expires_at=_timestamp(
                response.get("lease_expires_at"), "GENERATION_WORKER_HEARTBEAT_INVALID"
            ),
        )

    def complete(
        self,
        session: ProductionGenerationSession,
        claim: ProductionGenerationClaim,
        result: Mapping[str, object],
        result_sha256: str,
    ) -> None:
        payload = claim.identity(self.config, session.session_id)
        payload.update({"result_sha256": result_sha256, "result": dict(result)})
        self._terminal("complete", session.token, payload)

    def fail(
        self,
        session: ProductionGenerationSession,
        claim: ProductionGenerationClaim,
        *,
        code: str,
        retryable: bool,
    ) -> None:
        if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", code):
            code = "GENERATION_FAILED"
        payload = claim.identity(self.config, session.session_id)
        payload.update({"code": code, "retryable": retryable})
        self._terminal("fail", session.token, payload)

    def receipt(
        self,
        session: ProductionGenerationSession,
        identity: ProductionGenerationCacheIdentity,
    ) -> Mapping[str, object] | None:
        status, body, _headers = self._http._request(
            "POST",
            f"{API_PREFIX}/receipt",
            token=session.token,
            payload={
                "protocol_version": PROTOCOL_VERSION,
                "worker_id": self.config.worker_id,
                "release_id": self.config.release_id,
                "session_id": session.session_id,
                "job_id": identity.job_id,
                "attempt": identity.attempt,
                "fence": identity.fence,
                "assignment_sha256": identity.assignment_sha256,
            },
            expected=(200, 204),
            maximum=_MAX_CONTROL_BYTES,
            hard_deadline_seconds=self.config.request_timeout_seconds,
        )
        if status == 204:
            if body:
                raise ValidationError(
                    "GENERATION_WORKER_PROTOCOL_INVALID",
                    "Empty generation receipt response contained a body",
                )
            return None
        parsed = strict_json_loads(body, max_bytes=_MAX_CONTROL_BYTES)
        return _parse_receipt(parsed, self.config, identity)

    def _terminal(self, suffix: str, token: str, payload: Mapping[str, object]) -> None:
        try:
            _status, body, _headers = self._http._request(
                "POST",
                f"{API_PREFIX}/{suffix}",
                token=token,
                payload=payload,
                maximum=_MAX_CONTROL_BYTES,
                hard_deadline_seconds=self.config.request_timeout_seconds,
            )
            _terminal_ack_from_mapping(
                _mapping(
                    strict_json_loads(body, max_bytes=_MAX_CONTROL_BYTES),
                    "GENERATION_WORKER_TERMINAL_INVALID",
                )
            )
        except RemoteWorkerError:
            raise
        except AtlasResearchError as error:
            raise RemoteWorkerError(
                "GENERATION_WORKER_TERMINAL_AMBIGUOUS",
                "Scout generation terminal acknowledgement is ambiguous",
            ) from error


class _GenerationSupervisor:
    def __init__(
        self,
        client: ProductionGenerationClient,
        session: ProductionGenerationSession,
        claim: ProductionGenerationClaim,
        stop_event: threading.Event,
    ) -> None:
        self.client = client
        self.session = session
        self.claim = claim
        self.stop_event = stop_event
        self.done = threading.Event()
        self.cancelled = False
        self.failure: BaseException | None = None
        self.sequence = 0
        self.lease_expires_at = claim.lease_expires_at
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        self._heartbeat()
        self.thread = threading.Thread(
            target=self._run,
            name="atlas-production-generation-heartbeat",
            daemon=True,
        )
        self.thread.start()

    def _heartbeat(self) -> None:
        self.sequence += 1
        reply = self.client.heartbeat(self.session, self.claim, self.sequence)
        self.cancelled = reply.cancelled
        self.lease_expires_at = reply.lease_expires_at

    def _run(self) -> None:
        try:
            while not self.done.wait(self.session.heartbeat_interval_seconds):
                if self.stop_event.is_set() or self.cancelled:
                    return
                self._heartbeat()
        except BaseException as error:
            self.failure = error

    def check(self) -> None:
        if self.stop_event.is_set() or self.cancelled:
            raise RemoteWorkerError("GENERATION_WORKER_CANCELLED", "Generation claim was cancelled")
        if self.failure is not None:
            raise self.failure
        if self.lease_expires_at <= datetime.now(UTC):
            raise RemoteWorkerError("GENERATION_WORKER_LEASE_EXPIRED", "Generation lease expired")

    def stop_and_check(self) -> None:
        self.done.set()
        if self.thread is not None:
            self.thread.join(timeout=self.client.config.request_timeout_seconds + 1)
            if self.thread.is_alive():
                raise RemoteWorkerError(
                    "GENERATION_WORKER_HEARTBEAT_STUCK", "Generation heartbeat did not stop"
                )
        self.check()


class ProductionGenerationWorker:
    """Single-concurrency production inference worker with fenced result replay."""

    def __init__(
        self,
        config: ProductionGenerationWorkerConfig,
        *,
        client: ProductionGenerationClient | None = None,
        qwen_transport: QwenTransport | None = None,
    ) -> None:
        self.config = config
        self.client = client or ProductionGenerationClient(config)
        self.qwen_transport = qwen_transport
        self.stop_event = threading.Event()
        self.session: ProductionGenerationSession | None = None
        self.results_root = ensure_private_directory(config.state_root / "generation-results")

    def install_signal_handlers(self) -> None:
        def stop(_signum: int, _frame: object) -> None:
            self.stop_event.set()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)

    def _status(
        self,
        state: str,
        *,
        claim: ProductionGenerationClaim | None = None,
        error_code: str | None = None,
    ) -> None:
        atomic_write_private(
            self.config.state_root,
            "generation-status.json",
            canonical_json_bytes(
                {
                    "schema_version": STATUS_SCHEMA_VERSION,
                    "worker_id": self.config.worker_id,
                    "release_id": self.config.release_id,
                    "state": state,
                    "updated_at": _now(),
                    "job_id": claim.job.job_id if claim else None,
                    "attempt": claim.attempt if claim else None,
                    "fence": claim.fence if claim else None,
                    "last_error_code": error_code,
                }
            )
            + b"\n",
            overwrite=True,
            max_bytes=_MAX_CONTROL_BYTES,
        )

    def _session_or_exchange(self) -> ProductionGenerationSession:
        def has_margin(candidate: ProductionGenerationSession) -> bool:
            minimum = timedelta(
                seconds=60
                + (2 * candidate.lease_seconds)
                + (2 * self.config.request_timeout_seconds)
            )
            return candidate.expires_at - datetime.now(UTC) >= minimum

        session = self.session
        if session is None or not has_margin(session):
            self._status("authenticating")
            session = self.client.exchange_session()
            self.session = session
        if not has_margin(session):
            self.session = None
            raise RemoteWorkerError(
                "GENERATION_WORKER_SESSION_TOO_SHORT",
                "Generation worker session cannot cover one bounded claim",
            )
        return session

    def _result_name(self, claim: ProductionGenerationClaim) -> str:
        return (
            f"{claim.job.job_id}-{claim.attempt}-{claim.fence}-"
            f"{claim.cancellation_generation}-{claim.assignment_sha256}.json"
        )

    def _cache_identity(self, name: str, result_value: object) -> ProductionGenerationCacheIdentity:
        match = _RESULT_CACHE_NAME.fullmatch(name)
        result = _mapping(result_value, "GENERATION_WORKER_STATE_INVALID")
        if match is None:
            raise ValidationError(
                "GENERATION_WORKER_STATE_INVALID", "Generation result cache name is invalid"
            )
        attempt = int(match.group("attempt"))
        fence = int(match.group("fence"))
        cancellation = int(match.group("cancellation"))
        target = _mapping(result.get("target"), "GENERATION_WORKER_STATE_INVALID")
        _exact_fields(
            target,
            {"entity_type", "entity_id", "expected_version"},
            "GENERATION_WORKER_STATE_INVALID",
        )
        provenance = _mapping(result.get("provenance"), "GENERATION_WORKER_STATE_INVALID")
        workload_type = result.get("workload_type")
        if (
            result.get("contract_version") != "atlas-production-generation-result.v1"
            or result.get("job_id") != match.group("job")
            or workload_type not in {"content.description.regenerate", "atlas.score.generate"}
            or result.get("attempt") != attempt
            or provenance.get("worker_release_id") != self.config.release_id
            or not 1 <= fence <= (1 << 53) - 1
            or not 0 <= cancellation <= (1 << 53) - 1
        ):
            raise ValidationError(
                "GENERATION_WORKER_STATE_INVALID", "Generation result cache identity is invalid"
            )
        result_sha256 = hashlib.sha256(canonical_json_bytes(result)).hexdigest()
        return ProductionGenerationCacheIdentity(
            name=name,
            job_id=match.group("job"),
            workload_type=workload_type,
            target=dict(target),
            attempt=attempt,
            fence=fence,
            cancellation_generation=cancellation,
            assignment_sha256=match.group("assignment"),
            result_sha256=result_sha256,
        )

    def _reconcile_cached_results(self, session: ProductionGenerationSession) -> None:
        names = sorted(os.listdir(self.results_root))
        if len(names) > 8:
            raise ValidationError(
                "GENERATION_WORKER_STATE_INVALID", "Too many generation result cache entries"
            )
        for name in names:
            resolved = read_private_bytes(self.results_root, name, max_bytes=_MAX_RESULT_BYTES + 1)
            if resolved is None:
                continue
            _path, data = resolved
            result_value = strict_json_loads(data, max_bytes=_MAX_RESULT_BYTES + 1)
            identity = self._cache_identity(name, result_value)
            receipt = self.client.receipt(session, identity)
            if receipt is not None:
                self._remove_result_name(name)

    def _cached_result(
        self, claim: ProductionGenerationClaim
    ) -> tuple[Mapping[str, object], str] | None:
        resolved = read_private_bytes(
            self.results_root, self._result_name(claim), max_bytes=_MAX_RESULT_BYTES + 1
        )
        if resolved is None:
            return None
        _path, data = resolved
        value = strict_json_loads(data, max_bytes=_MAX_RESULT_BYTES + 1)
        result = validate_production_generation_result(
            claim.job, value, release_id=self.config.release_id
        )
        canonical = canonical_json_bytes(result)
        return result, hashlib.sha256(canonical).hexdigest()

    def _store_result(self, claim: ProductionGenerationClaim, result: Mapping[str, object]) -> str:
        canonical = canonical_json_bytes(result)
        if len(canonical) > _MAX_RESULT_BYTES:
            raise ValidationError(
                "GENERATION_WORKER_RESULT_INVALID", "Generation result is too large"
            )
        atomic_write_private(
            self.results_root,
            self._result_name(claim),
            canonical + b"\n",
            overwrite=False,
            max_bytes=_MAX_RESULT_BYTES + 1,
        )
        return hashlib.sha256(canonical).hexdigest()

    def _remove_result(self, claim: ProductionGenerationClaim) -> None:
        self._remove_result_name(self._result_name(claim))

    def _remove_result_name(self, name: str) -> None:
        if _RESULT_CACHE_NAME.fullmatch(name) is None:
            raise ValidationError(
                "GENERATION_WORKER_STATE_INVALID", "Generation result cache name is invalid"
            )
        path = self.results_root / name
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) & 0o077
        ):
            raise ValidationError(
                "GENERATION_WORKER_STATE_INVALID", "Generation result cache is unsafe"
            )
        path.unlink()

    def run_once(self) -> ProductionGenerationRunOutcome:
        session = self._session_or_exchange()
        self._reconcile_cached_results(session)
        claim = self.client.claim(session)
        if claim is None:
            self._status("idle")
            return ProductionGenerationRunOutcome("idle")
        supervisor = _GenerationSupervisor(self.client, session, claim, self.stop_event)
        self._status("leased", claim=claim)
        try:
            supervisor.start()
            cached = self._cached_result(claim)
            if cached is None:
                started = _now()
                self._status("running", claim=claim)
                execution = execute_production_generation(
                    claim.job,
                    model_revision=self.config.model_revision,
                    transport=self.qwen_transport,
                )
                finished = _now()
                result = build_production_generation_result(
                    claim.job,
                    execution,
                    release_id=self.config.release_id,
                    started_at=started,
                    finished_at=finished,
                )
                digest = self._store_result(claim, result)
            else:
                result, digest = cached
            supervisor.stop_and_check()
            self.client.complete(session, claim, result, digest)
            self._remove_result(claim)
            self._status("completed", claim=claim)
            return ProductionGenerationRunOutcome(
                "completed", job_id=claim.job.job_id, result_sha256=digest
            )
        except RemoteWorkerError as error:
            self._status("backoff", claim=claim, error_code=error.code)
            raise
        except QwenError as error:
            supervisor.stop_and_check()
            retryable = error.code in {
                "QWEN_TIMEOUT",
                "QWEN_CONNECTION_FAILED",
                "QWEN_HTTP_ERROR",
                "QWEN_MODEL_UNAVAILABLE",
            }
            self.client.fail(session, claim, code=error.code, retryable=retryable)
            self._remove_result(claim)
            self._status("failed", claim=claim, error_code=error.code)
            return ProductionGenerationRunOutcome(
                "failed", job_id=claim.job.job_id, error_code=error.code
            )
        except AtlasResearchError as error:
            supervisor.stop_and_check()
            self.client.fail(session, claim, code=error.code, retryable=False)
            self._remove_result(claim)
            self._status("failed", claim=claim, error_code=error.code)
            return ProductionGenerationRunOutcome(
                "failed", job_id=claim.job.job_id, error_code=error.code
            )

    def serve(self) -> ProductionGenerationRunOutcome:
        last = ProductionGenerationRunOutcome("starting")
        delay = self.config.poll_seconds
        self._status("starting")
        while not self.stop_event.is_set():
            try:
                last = self.run_once()
                delay = self.config.poll_seconds
            except RemoteWorkerError as error:
                if error.code == "WORKER_AUTH_REJECTED":
                    self.session = None
                self._status("backoff", error_code=error.code)
                last = ProductionGenerationRunOutcome("backoff", error_code=error.code)
                delay = min(max(self.config.poll_seconds, delay * 2), 60.0)
            if not self.stop_event.is_set():
                self.stop_event.wait(delay)
        self._status("stopped")
        return last


def load_production_generation_worker_config(
    path: Path,
) -> ProductionGenerationWorkerConfig:
    data = _read_bounded_private_file(path, maximum=_MAX_CONTROL_BYTES)
    value = strict_json_loads(data, max_bytes=_MAX_CONTROL_BYTES)
    return ProductionGenerationWorkerConfig.from_mapping(
        _mapping(value, "GENERATION_WORKER_CONFIG_INVALID")
    )


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

# SPDX-License-Identifier: MIT
"""Least-privilege outbound client for Scout-owned research worker leases."""

from __future__ import annotations

import errno
import hashlib
import http.client
import ipaddress
import os
import re
import shutil
import signal
import socket
import ssl
import stat
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final, Protocol, cast
from urllib.parse import urlsplit

from .artifacts import atomic_write_private, ensure_private_directory, read_private_bytes
from .canonical import canonical_json_bytes, strict_json_loads
from .constants import MAX_ARTIFACT_BYTES, MAX_ARTIFACTS, MAX_JOB_BYTES, MAX_TOTAL_INPUT_BYTES
from .errors import AtlasResearchError, ResourceLimitError, ValidationError
from .job import ResearchJob, load_job
from .operations_telemetry import (
    MAX_SCOUT_TELEMETRY_BYTES,
    ScoutTelemetry,
    TelemetryCommitAmbiguousError,
    TelemetrySnapshotStaleError,
    parse_scout_telemetry,
    validate_telemetry_destination,
    write_worker_telemetry,
)
from .worker import validate_result_document

PROTOCOL_VERSION: Final = "1"
WORKLOAD_TYPE: Final = "research.experiment"
STATUS_SCHEMA_VERSION: Final = "atlas-research-worker-status.v1"
_MAX_CONTROL_BYTES: Final = 512 << 10
_MAX_RESULT_BYTES: Final = 256 << 10
_MAX_TOKEN_BYTES: Final = 512
_MAX_ERROR_CODE_CHARS: Final = 64
_FREE_SPACE_RESERVE_BYTES: Final = 1 << 30
_TELEMETRY_PUBLISH_INTERVAL_SECONDS: Final = 20.0
_TELEMETRY_REQUEST_TIMEOUT_SECONDS: Final = 5.0
_MAX_RESOLVER_BYTES: Final = 16 << 10
_MAX_RESOLVED_ADDRESSES: Final = 16
_RESOLVER_PROGRAM: Final = """\
import json
import socket
import sys

results = []
for family, socktype, proto, _canonname, sockaddr in socket.getaddrinfo(
    sys.argv[1], int(sys.argv[2]), 0, socket.SOCK_STREAM, 0
):
    if family == socket.AF_INET:
        item = [family, socktype, proto, sockaddr[0], sockaddr[1]]
    elif family == socket.AF_INET6:
        item = [family, socktype, proto, sockaddr[0], sockaddr[1], sockaddr[2], sockaddr[3]]
    else:
        continue
    if item not in results:
        results.append(item)
    if len(results) >= 16:
        break
sys.stdout.write(json.dumps(results, separators=(",", ":")))
"""
_STORAGE_EXHAUSTION_ERRNOS: Final = frozenset(
    {errno.ENOSPC, getattr(errno, "EDQUOT", errno.ENOSPC)}
)
_IDENTIFIER: Final = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$", re.ASCII)
_TOKEN: Final = re.compile(r"^[A-Za-z0-9._~-]{32,512}$", re.ASCII)
_SHA256: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_ERROR_CODE: Final = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$", re.ASCII)
_SEGMENT: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$", re.ASCII)
_DOWNLOAD_PATH: Final = re.compile(
    r"^/api/worker/v1/objects/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.ASCII,
)
_ARCHIVE_SUFFIXES: Final = (
    ".zip",
    ".tar",
    ".tgz",
    ".tar.gz",
    ".gz",
    ".bz2",
    ".xz",
    ".7z",
    ".rar",
    ".whl",
    ".jar",
    ".zst",
    ".tar.zst",
    ".cab",
    ".iso",
    ".dmg",
)


class RemoteWorkerError(AtlasResearchError):
    """Bounded operational failure while speaking to Scout."""


def _first_os_error(error: BaseException) -> OSError | None:
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen and len(seen) < 32:
        seen.add(id(current))
        if isinstance(current, OSError):
            return current
        current = current.__cause__ or current.__context__
    return None


def _local_operational_error(error: BaseException) -> RemoteWorkerError | None:
    os_error = _first_os_error(error)
    if os_error is None:
        return None
    if os_error.errno in _STORAGE_EXHAUSTION_ERRNOS:
        return RemoteWorkerError(
            "WORKER_STORAGE_EXHAUSTED", "Worker storage was exhausted during the claim"
        )
    return RemoteWorkerError(
        "WORKER_STORAGE_UNAVAILABLE", "Worker local storage operation is unavailable"
    )


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    """Operator-owned configuration; no server response can change the executor."""

    controller_url: str
    worker_id: str
    enrollment_token_file: Path
    state_root: Path
    executor_path: Path
    poll_seconds: float = 5.0
    request_timeout_seconds: float = 30.0
    max_job_seconds: int = 3_000
    max_bundle_bytes: int = MAX_TOTAL_INPUT_BYTES
    telemetry_file: Path | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> WorkerConfig:
        required = {
            "protocol_version",
            "controller_url",
            "worker_id",
            "enrollment_token_file",
            "state_root",
            "executor_path",
        }
        optional = {
            "poll_seconds",
            "request_timeout_seconds",
            "max_job_seconds",
            "max_bundle_bytes",
            "telemetry_file",
        }
        if not required.issubset(value) or not set(value).issubset(required | optional):
            raise ValidationError("WORKER_CONFIG_INVALID", "Worker config fields are invalid")
        if value.get("protocol_version") != PROTOCOL_VERSION:
            raise ValidationError("WORKER_CONFIG_INVALID", "Worker protocol version is invalid")
        controller_url = _required_string(value, "controller_url", "WORKER_CONFIG_INVALID")
        _controller_origin(controller_url)
        worker_id = _required_string(value, "worker_id", "WORKER_CONFIG_INVALID")
        if _IDENTIFIER.fullmatch(worker_id) is None:
            raise ValidationError("WORKER_CONFIG_INVALID", "Worker identity is invalid")
        enrollment_token_file = _absolute_path(value, "enrollment_token_file")
        state_root = _absolute_path(value, "state_root")
        executor_path = _absolute_path(value, "executor_path")
        poll_seconds = _bounded_number(value.get("poll_seconds", 5.0), 0.1, 300.0)
        request_timeout_seconds = _bounded_number(
            value.get("request_timeout_seconds", 30.0), 1.0, 120.0
        )
        max_job_seconds = _bounded_integer(value.get("max_job_seconds", 3_000), 1, 3_600)
        max_bundle_bytes = _bounded_integer(
            value.get("max_bundle_bytes", MAX_TOTAL_INPUT_BYTES),
            MAX_JOB_BYTES,
            MAX_TOTAL_INPUT_BYTES,
        )
        telemetry_file = (
            _absolute_path(value, "telemetry_file") if "telemetry_file" in value else None
        )
        config = cls(
            controller_url=controller_url,
            worker_id=worker_id,
            enrollment_token_file=enrollment_token_file,
            state_root=state_root,
            executor_path=executor_path,
            poll_seconds=poll_seconds,
            request_timeout_seconds=request_timeout_seconds,
            max_job_seconds=max_job_seconds,
            max_bundle_bytes=max_bundle_bytes,
            telemetry_file=telemetry_file,
        )
        config.validate_local_paths()
        return config

    def validate_local_paths(self) -> None:
        _read_secret_file(self.enrollment_token_file)
        ensure_private_directory(self.state_root)
        _validate_executor(self.executor_path)
        if self.telemetry_file is not None:
            validate_telemetry_destination(self.telemetry_file)


@dataclass(frozen=True, slots=True)
class WorkerSession:
    session_id: str
    token: str
    expires_at: datetime
    lease_seconds: int
    heartbeat_interval_seconds: int


@dataclass(frozen=True, slots=True)
class RemoteArtifact:
    path: str
    sha256: str
    size_bytes: int
    download_path: str


@dataclass(frozen=True, slots=True)
class RemoteClaim:
    job_id: str
    workload_type: str
    attempt: int
    fence: int
    cancellation_generation: int
    lease_expires_at: datetime
    artifacts: tuple[RemoteArtifact, ...]
    job_path: str

    def identity_body(self) -> dict[str, object]:
        return {
            "protocol_version": PROTOCOL_VERSION,
            "job_id": self.job_id,
            "attempt": self.attempt,
            "fence": self.fence,
            "cancellation_generation": self.cancellation_generation,
        }


@dataclass(frozen=True, slots=True)
class HeartbeatReply:
    cancelled: bool
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class _ResolvedAddress:
    family: int
    socktype: int
    proto: int
    sockaddr: tuple[str, int] | tuple[str, int, int, int]


class _ConnectionWithFactory(Protocol):
    _create_connection: Callable[[tuple[str, int], object, tuple[str, int] | None], socket.socket]


@dataclass(frozen=True, slots=True)
class RunOutcome:
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


class _ClaimCancelled(Exception):
    """Internal control flow for a Scout cancellation or local shutdown."""

    def __init__(self, *, confirmed: bool) -> None:
        self.confirmed = confirmed
        super().__init__("claim cancelled")


class _ClaimSupervisor:
    """Maintain one lease from claim return until immediately before terminal CAS."""

    def __init__(
        self,
        client: ScoutWorkerClient,
        session: WorkerSession,
        claim: RemoteClaim,
        worker_stop_event: threading.Event,
        deadline: float,
        request_timeout_seconds: float,
    ) -> None:
        self._client = client
        self._session = session
        self._claim = claim
        self._worker_stop_event = worker_stop_event
        self._deadline = deadline
        self._request_timeout_seconds = request_timeout_seconds
        self._done = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._sequence = 0
        self._lease_expires_at = claim.lease_expires_at
        self._cancelled = False
        self._failure: BaseException | None = None

    def _record_failure(self, error: BaseException) -> None:
        with self._lock:
            if self._failure is None:
                self._failure = error

    def _heartbeat(self) -> None:
        self._sequence += 1
        try:
            reply = self._client.heartbeat(self._session, self._claim, self._sequence)
        except RemoteWorkerError:
            raise
        except AtlasResearchError as error:
            raise RemoteWorkerError(
                "WORKER_HEARTBEAT_INVALID", "Scout returned an invalid heartbeat response"
            ) from error
        with self._lock:
            self._lease_expires_at = reply.lease_expires_at
            self._cancelled = reply.cancelled
        if reply.cancelled:
            return
        if reply.lease_expires_at <= datetime.now(UTC):
            raise RemoteWorkerError("WORKER_LEASE_EXPIRED", "Worker lease expired")

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("claim supervisor already started")
        self._heartbeat()
        self._thread = threading.Thread(
            target=self._run,
            name="atlas-research-worker-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        try:
            while not self._done.is_set() and not self._worker_stop_event.is_set():
                with self._lock:
                    if self._cancelled:
                        return
                    lease_remaining = (self._lease_expires_at - datetime.now(UTC)).total_seconds()
                deadline_remaining = self._deadline - time.monotonic()
                wait_seconds = min(
                    float(self._session.heartbeat_interval_seconds),
                    max(0.1, lease_remaining / 2),
                    max(0.1, deadline_remaining),
                )
                if self._done.wait(wait_seconds):
                    return
                if time.monotonic() >= self._deadline:
                    raise RemoteWorkerError("WORKER_TIMEOUT", "Worker claim exceeded its deadline")
                self._heartbeat()
        except BaseException as error:
            self._record_failure(error)

    def check(self) -> None:
        if self._worker_stop_event.is_set():
            raise _ClaimCancelled(confirmed=False)
        with self._lock:
            cancelled = self._cancelled
            failure = self._failure
            lease_expires_at = self._lease_expires_at
        if cancelled:
            raise _ClaimCancelled(confirmed=True)
        if failure is not None:
            raise failure
        if time.monotonic() >= self._deadline:
            raise RemoteWorkerError("WORKER_TIMEOUT", "Worker claim exceeded its deadline")
        if lease_expires_at <= datetime.now(UTC):
            raise RemoteWorkerError("WORKER_LEASE_EXPIRED", "Worker lease expired")

    def stop(self) -> None:
        self._done.set()
        thread = self._thread
        if thread is None:
            return
        thread.join(timeout=self._request_timeout_seconds + 1.0)
        if thread.is_alive():
            self._record_failure(
                RemoteWorkerError("WORKER_HEARTBEAT_STUCK", "Worker heartbeat did not stop")
            )

    def stop_and_check(self) -> None:
        self.stop()
        self.check()


def _required_string(value: Mapping[str, object], key: str, code: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValidationError(code, "Remote worker field is invalid")
    return item


def _absolute_path(value: Mapping[str, object], key: str) -> Path:
    item = _required_string(value, key, "WORKER_CONFIG_INVALID")
    path = Path(item)
    if not path.is_absolute() or "\x00" in item:
        raise ValidationError("WORKER_CONFIG_INVALID", "Worker path must be absolute")
    return path


def _bounded_number(value: object, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError("WORKER_CONFIG_INVALID", "Worker numeric field is invalid")
    result = float(value)
    if not minimum <= result <= maximum:
        raise ValidationError("WORKER_CONFIG_INVALID", "Worker numeric field is invalid")
    return result


def _bounded_integer(value: object, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValidationError("WORKER_CONFIG_INVALID", "Worker integer field is invalid")
    return value


def _controller_origin(raw: str) -> tuple[str, str, int]:
    parsed = urlsplit(raw)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValidationError("WORKER_CONFIG_INVALID", "Controller URL must be one origin")
    if parsed.path not in {"", "/"} or parsed.hostname is None:
        raise ValidationError("WORKER_CONFIG_INVALID", "Controller URL must be one origin")
    try:
        host = parsed.hostname.encode("idna").decode("ascii")
    except UnicodeError as error:
        raise ValidationError("WORKER_CONFIG_INVALID", "Controller hostname is invalid") from error
    if not 1 <= len(host) <= 253 or any(ord(character) <= 0x20 for character in host):
        raise ValidationError("WORKER_CONFIG_INVALID", "Controller hostname is invalid")
    try:
        parsed_port = parsed.port
    except ValueError as error:
        raise ValidationError("WORKER_CONFIG_INVALID", "Controller URL port is invalid") from error
    if parsed.scheme == "https":
        port = parsed_port or 443
    elif parsed.scheme == "http" and host in {"127.0.0.1", "::1"}:
        port = parsed_port or 80
    else:
        raise ValidationError(
            "WORKER_CONFIG_INVALID", "Controller URL must use HTTPS or loopback HTTP"
        )
    return parsed.scheme, host, port


def _read_secret_file(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    try:
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) & 0o077
            or not 16 <= before.st_size <= _MAX_TOKEN_BYTES + 1
        ):
            raise ValidationError("WORKER_CREDENTIAL_INVALID", "Worker credential file is unsafe")
        data = os.read(descriptor, _MAX_TOKEN_BYTES + 2)
        after = os.fstat(descriptor)
        if len(data) > _MAX_TOKEN_BYTES + 1 or (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValidationError("WORKER_CREDENTIAL_INVALID", "Worker credential file is unsafe")
    except OSError as error:
        raise ValidationError(
            "WORKER_CREDENTIAL_INVALID", "Worker credential file cannot be read safely"
        ) from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        token = data.decode("ascii").rstrip("\n")
    except UnicodeDecodeError as error:
        raise ValidationError(
            "WORKER_CREDENTIAL_INVALID", "Worker credential is invalid"
        ) from error
    if "\n" in token or "\r" in token or _TOKEN.fullmatch(token) is None:
        raise ValidationError("WORKER_CREDENTIAL_INVALID", "Worker credential is invalid")
    return token


def _validate_executor(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise ValidationError(
            "WORKER_EXECUTOR_INVALID", "Worker executor is unavailable"
        ) from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid not in {0, os.getuid()}
        or stat.S_IMODE(metadata.st_mode) & 0o022
        or not metadata.st_mode & stat.S_IXUSR
    ):
        raise ValidationError("WORKER_EXECUTOR_INVALID", "Worker executor is unsafe")


def _mapping(value: object, code: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ValidationError(code, "Remote worker response must be an object")
    return cast(Mapping[str, object], value)


def _exact_fields(value: Mapping[str, object], fields: set[str], code: str) -> None:
    if set(value) != fields:
        raise ValidationError(code, "Remote worker response fields are invalid")


def _terminal_ack_from_mapping(value: Mapping[str, object]) -> bool:
    _exact_fields(value, {"accepted", "replayed"}, "WORKER_TERMINAL_ACK_INVALID")
    replayed = value.get("replayed")
    if value.get("accepted") is not True or not isinstance(replayed, bool):
        raise ValidationError(
            "WORKER_TERMINAL_ACK_INVALID", "Worker terminal acknowledgement is invalid"
        )
    return replayed


def _timestamp(value: object, code: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValidationError(code, "Remote worker timestamp is invalid")
    try:
        result = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValidationError(code, "Remote worker timestamp is invalid") from error
    if result.tzinfo is None:
        raise ValidationError(code, "Remote worker timestamp is invalid")
    return result.astimezone(UTC)


def _safe_relative(value: object, code: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ValidationError(code, "Remote artifact path is invalid")
    if any(marker in value for marker in ("\\", "%", "?", "#", "\x00", ":")):
        raise ValidationError(code, "Remote artifact path is invalid")
    parts = value.split("/")
    if len(parts) > 32 or any(
        part in {"", ".", ".."} or _SEGMENT.fullmatch(part) is None for part in parts
    ):
        raise ValidationError(code, "Remote artifact path is invalid")
    if any(part.lower().endswith(_ARCHIVE_SUFFIXES) for part in parts):
        raise ValidationError(code, "Archive artifacts are not accepted")
    return value


def _session_from_mapping(value: Mapping[str, object]) -> WorkerSession:
    _exact_fields(
        value,
        {
            "protocol_version",
            "session_id",
            "session_token",
            "expires_at",
            "lease_seconds",
            "heartbeat_interval_seconds",
        },
        "WORKER_SESSION_INVALID",
    )
    if value.get("protocol_version") != PROTOCOL_VERSION:
        raise ValidationError("WORKER_SESSION_INVALID", "Worker protocol version is invalid")
    session_id = _required_string(value, "session_id", "WORKER_SESSION_INVALID")
    token = _required_string(value, "session_token", "WORKER_SESSION_INVALID")
    if (
        _IDENTIFIER.fullmatch(session_id) is None
        or _TOKEN.fullmatch(token) is None
        or len(token) > 256
    ):
        raise ValidationError("WORKER_SESSION_INVALID", "Worker session identity is invalid")
    expires_at = _timestamp(value.get("expires_at"), "WORKER_SESSION_INVALID")
    lease_seconds = _bounded_protocol_integer(
        value.get("lease_seconds"), 30, 900, "WORKER_SESSION_INVALID"
    )
    heartbeat = _bounded_protocol_integer(
        value.get("heartbeat_interval_seconds"),
        10,
        min(300, lease_seconds),
        "WORKER_SESSION_INVALID",
    )
    return WorkerSession(session_id, token, expires_at, lease_seconds, heartbeat)


def _bounded_protocol_integer(value: object, minimum: int, maximum: int, code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValidationError(code, "Remote worker integer field is invalid")
    return value


def _claim_from_mapping(value: Mapping[str, object], max_bundle_bytes: int) -> RemoteClaim:
    _exact_fields(
        value,
        {
            "protocol_version",
            "job_id",
            "workload_type",
            "attempt",
            "fence",
            "cancellation_generation",
            "lease_expires_at",
            "artifacts",
            "job_path",
        },
        "WORKER_CLAIM_INVALID",
    )
    if value.get("protocol_version") != PROTOCOL_VERSION:
        raise ValidationError("WORKER_CLAIM_INVALID", "Worker protocol version is invalid")
    job_id = _required_string(value, "job_id", "WORKER_CLAIM_INVALID")
    if _IDENTIFIER.fullmatch(job_id) is None or value.get("workload_type") != WORKLOAD_TYPE:
        raise ValidationError("WORKER_CLAIM_INVALID", "Remote worker job identity is invalid")
    attempt = _bounded_protocol_integer(value.get("attempt"), 1, 1_000, "WORKER_CLAIM_INVALID")
    fence = _bounded_protocol_integer(value.get("fence"), 1, (1 << 53) - 1, "WORKER_CLAIM_INVALID")
    cancellation_generation = _bounded_protocol_integer(
        value.get("cancellation_generation"), 0, (1 << 53) - 1, "WORKER_CLAIM_INVALID"
    )
    lease_expires_at = _timestamp(value.get("lease_expires_at"), "WORKER_CLAIM_INVALID")
    raw_artifacts = value.get("artifacts")
    if not isinstance(raw_artifacts, list) or not 1 <= len(raw_artifacts) <= MAX_ARTIFACTS:
        raise ValidationError("WORKER_CLAIM_INVALID", "Remote artifact count is invalid")
    artifacts: list[RemoteArtifact] = []
    seen: set[str] = set()
    total = 0
    for raw in raw_artifacts:
        artifact = _mapping(raw, "WORKER_CLAIM_INVALID")
        _exact_fields(
            artifact,
            {"path", "sha256", "size_bytes", "download_path"},
            "WORKER_CLAIM_INVALID",
        )
        path = _safe_relative(artifact.get("path"), "WORKER_CLAIM_INVALID")
        digest = artifact.get("sha256")
        size = artifact.get("size_bytes")
        download_path = artifact.get("download_path")
        if path in seen or not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
            raise ValidationError("WORKER_CLAIM_INVALID", "Remote artifact identity is invalid")
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or not 0 <= size <= MAX_ARTIFACT_BYTES
        ):
            raise ValidationError("WORKER_CLAIM_INVALID", "Remote artifact size is invalid")
        if not isinstance(download_path, str) or _DOWNLOAD_PATH.fullmatch(download_path) is None:
            raise ValidationError("WORKER_CLAIM_INVALID", "Remote download path is invalid")
        total += size
        if total > max_bundle_bytes:
            raise ResourceLimitError(
                "WORKER_BUNDLE_EXCEEDED", "Remote artifact bundle is too large"
            )
        seen.add(path)
        artifacts.append(RemoteArtifact(path, digest, size, download_path))
    job_path = _safe_relative(value.get("job_path"), "WORKER_CLAIM_INVALID")
    if job_path != "job.json" or job_path not in seen:
        raise ValidationError("WORKER_CLAIM_INVALID", "Remote worker job path is invalid")
    return RemoteClaim(
        job_id=job_id,
        workload_type=WORKLOAD_TYPE,
        attempt=attempt,
        fence=fence,
        cancellation_generation=cancellation_generation,
        lease_expires_at=lease_expires_at,
        artifacts=tuple(artifacts),
        job_path=job_path,
    )


def _terminate_resolver(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    with suppress(OSError):
        process.terminate()
    try:
        process.wait(timeout=0.1)
        return
    except subprocess.TimeoutExpired:
        pass
    with suppress(OSError):
        process.kill()
    process.wait()


def _resolved_addresses(value: object, port: int) -> tuple[_ResolvedAddress, ...]:
    if not isinstance(value, list) or not 1 <= len(value) <= _MAX_RESOLVED_ADDRESSES:
        raise ValidationError("WORKER_RESOLVER_INVALID", "Worker resolver response is invalid")
    resolved: list[_ResolvedAddress] = []
    for raw_item in value:
        if not isinstance(raw_item, list) or len(raw_item) not in {5, 7}:
            raise ValidationError("WORKER_RESOLVER_INVALID", "Worker resolver response is invalid")
        family, socktype, proto, address, resolved_port, *ipv6_values = raw_item
        if (
            isinstance(family, bool)
            or not isinstance(family, int)
            or isinstance(socktype, bool)
            or socktype != socket.SOCK_STREAM
            or isinstance(proto, bool)
            or not isinstance(proto, int)
            or proto not in {0, socket.IPPROTO_TCP}
            or not isinstance(address, str)
            or "%" in address
            or isinstance(resolved_port, bool)
            or not isinstance(resolved_port, int)
            or resolved_port != port
        ):
            raise ValidationError("WORKER_RESOLVER_INVALID", "Worker resolver response is invalid")
        try:
            parsed_address = ipaddress.ip_address(address)
        except ValueError as error:
            raise ValidationError(
                "WORKER_RESOLVER_INVALID", "Worker resolver response is invalid"
            ) from error
        if family == socket.AF_INET and len(raw_item) == 5 and parsed_address.version == 4:
            sockaddr: tuple[str, int] | tuple[str, int, int, int] = (address, resolved_port)
        elif family == socket.AF_INET6 and len(raw_item) == 7 and parsed_address.version == 6:
            flowinfo, scope_id = ipv6_values
            if (
                isinstance(flowinfo, bool)
                or not isinstance(flowinfo, int)
                or not 0 <= flowinfo <= 0xFFFFFFFF
                or isinstance(scope_id, bool)
                or not isinstance(scope_id, int)
                or not 0 <= scope_id <= 0xFFFFFFFF
            ):
                raise ValidationError(
                    "WORKER_RESOLVER_INVALID", "Worker resolver response is invalid"
                )
            sockaddr = (address, resolved_port, flowinfo, scope_id)
        else:
            raise ValidationError("WORKER_RESOLVER_INVALID", "Worker resolver response is invalid")
        item = _ResolvedAddress(family, socktype, proto, sockaddr)
        if item not in resolved:
            resolved.append(item)
    if not resolved:
        raise ValidationError("WORKER_RESOLVER_INVALID", "Worker resolver response is invalid")
    return tuple(resolved)


def _resolve_controller_addresses(
    host: str,
    port: int,
    *,
    deadline: float,
    cancel_event: threading.Event | None,
) -> tuple[_ResolvedAddress, ...]:
    process = subprocess.Popen(
        [sys.executable, "-I", "-c", _RESOLVER_PROGRAM, host, str(port)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env={},
        close_fds=True,
        start_new_session=True,
    )
    try:
        while process.poll() is None:
            if (cancel_event is not None and cancel_event.is_set()) or time.monotonic() >= deadline:
                raise TimeoutError("worker resolver deadline expired")
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
        if (cancel_event is not None and cancel_event.is_set()) or time.monotonic() >= deadline:
            raise TimeoutError("worker resolver deadline expired")
        stdout, _stderr = process.communicate()
        if process.returncode != 0 or len(stdout) > _MAX_RESOLVER_BYTES:
            raise OSError("worker resolver failed")
        return _resolved_addresses(
            strict_json_loads(stdout, max_bytes=_MAX_RESOLVER_BYTES),
            port,
        )
    finally:
        _terminate_resolver(process)
        if process.stdout is not None:
            process.stdout.close()


def _duplicate_interrupt_socket(active_socket: socket.socket) -> socket.socket:
    """Duplicate the transport FD as a raw socket, including for TLS wrappers."""

    duplicate_fd = os.dup(active_socket.fileno())
    try:
        return socket.socket(
            family=active_socket.family,
            type=active_socket.type,
            proto=active_socket.proto,
            fileno=duplicate_fd,
        )
    except BaseException:
        os.close(duplicate_fd)
        raise


class ScoutWorkerClient:
    """Small HTTP client that rejects redirects and cross-origin object URLs."""

    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.scheme, self.host, self.port = _controller_origin(config.controller_url)

    def _connection(self, timeout_seconds: float | None = None) -> http.client.HTTPConnection:
        timeout = timeout_seconds or self.config.request_timeout_seconds
        if self.scheme == "https":
            return http.client.HTTPSConnection(
                self.host,
                self.port,
                timeout=timeout,
                context=ssl.create_default_context(),
            )
        return http.client.HTTPConnection(self.host, self.port, timeout=timeout)

    def _request(
        self,
        method: str,
        path: str,
        *,
        token: str,
        payload: Mapping[str, object] | None = None,
        expected: Sequence[int] = (200,),
        maximum: int = _MAX_CONTROL_BYTES,
        timeout_seconds: float | None = None,
        hard_deadline_seconds: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> tuple[int, bytes, Mapping[str, str]]:
        if not path.startswith("/") or "//" in path or any(item in path for item in ("?", "#")):
            raise ValidationError("WORKER_PROTOCOL_INVALID", "Worker request path is invalid")
        data = canonical_json_bytes(payload) if payload is not None else None
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "atlas-research-worker/1",
        }
        if data is not None:
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(data))
        connection = self._connection(timeout_seconds)
        if hard_deadline_seconds is None and cancel_event is not None:
            hard_deadline_seconds = timeout_seconds or self.config.request_timeout_seconds
        deadline = (
            time.monotonic() + hard_deadline_seconds if hard_deadline_seconds is not None else None
        )
        aborted = threading.Event()
        finished = threading.Event()
        watchdog: threading.Thread | None = None
        connecting_socket: socket.socket | None = None
        interrupt_socket: socket.socket | None = None
        response: http.client.HTTPResponse | None = None
        resolved_addresses: tuple[_ResolvedAddress, ...] = ()

        def check_active() -> None:
            if (
                aborted.is_set()
                or (cancel_event is not None and cancel_event.is_set())
                or (deadline is not None and time.monotonic() >= deadline)
            ):
                aborted.set()
                raise TimeoutError("worker request deadline expired")

        def abort_socket() -> None:
            for active_socket in (connecting_socket, connection.sock, interrupt_socket):
                if active_socket is None:
                    continue
                with suppress(OSError):
                    active_socket.shutdown(socket.SHUT_RDWR)

        def connect_resolved(
            _address: tuple[str, int],
            _timeout: object,
            source_address: tuple[str, int] | None,
        ) -> socket.socket:
            nonlocal connecting_socket, interrupt_socket
            last_error: OSError | None = None
            for index, resolved in enumerate(resolved_addresses):
                check_active()
                assert deadline is not None
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0:
                    check_active()
                remaining_candidates = len(resolved_addresses) - index
                attempt_seconds = remaining_seconds / remaining_candidates
                candidate: socket.socket | None = None
                connected = False
                try:
                    candidate = socket.socket(resolved.family, resolved.socktype, resolved.proto)
                    connecting_socket = candidate
                    candidate.settimeout(max(0.001, attempt_seconds))
                    if source_address is not None:
                        candidate.bind(source_address)
                    candidate.connect(resolved.sockaddr)
                    connected = True
                    check_active()
                    interrupt_socket = _duplicate_interrupt_socket(candidate)
                    return candidate
                except OSError as error:
                    last_error = error
                    if candidate is not None:
                        candidate.close()
                    connecting_socket = None
                    if connected or aborted.is_set() or time.monotonic() >= deadline:
                        raise
            if last_error is not None:
                raise last_error
            raise OSError("worker resolver returned no usable addresses")

        def watch_request() -> None:
            while not finished.is_set():
                if cancel_event is not None and cancel_event.is_set():
                    aborted.set()
                if deadline is not None and time.monotonic() >= deadline:
                    aborted.set()
                if aborted.is_set():
                    abort_socket()
                    if finished.wait(0.01):
                        return
                    continue
                wait_seconds = 0.01
                if deadline is not None:
                    wait_seconds = min(wait_seconds, max(0.0, deadline - time.monotonic()))
                finished.wait(wait_seconds)

        if deadline is not None or cancel_event is not None:
            watchdog = threading.Thread(
                target=watch_request,
                name="atlas-research-worker-telemetry-request-watchdog",
                daemon=True,
            )
            watchdog.start()

        def read_response(response: http.client.HTTPResponse, limit: int) -> bytes:
            if deadline is None and cancel_event is None:
                return response.read(limit)
            chunks: list[bytes] = []
            remaining = limit
            while remaining > 0:
                check_active()
                if deadline is not None and connection.sock is not None:
                    connection.sock.settimeout(max(0.001, deadline - time.monotonic()))
                chunk = response.read1(min(65_536, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            check_active()
            return b"".join(chunks)

        try:
            check_active()
            if deadline is not None:
                resolved_addresses = _resolve_controller_addresses(
                    self.host,
                    self.port,
                    deadline=deadline,
                    cancel_event=cancel_event,
                )
                cast(_ConnectionWithFactory, connection)._create_connection = connect_resolved
            check_active()
            connection.request(method, path, body=data, headers=headers)
            if deadline is not None or cancel_event is not None:
                active_socket = connection.sock
                if active_socket is None:
                    raise OSError("worker request socket is unavailable")
                if interrupt_socket is None:
                    interrupt_socket = _duplicate_interrupt_socket(active_socket)
            check_active()
            response = connection.getresponse()
            check_active()
            if response.status not in expected:
                read_response(response, min(maximum, 4_096))
                if response.status in {401, 403}:
                    raise RemoteWorkerError(
                        "WORKER_AUTH_REJECTED", "Scout rejected worker authentication"
                    )
                if response.status in {408, 409, 425, 429} or response.status >= 500:
                    raise RemoteWorkerError(
                        "WORKER_CONTROLLER_RETRY", "Scout controller is temporarily unavailable"
                    )
                raise RemoteWorkerError(
                    "WORKER_CONTROLLER_REJECTED", "Scout rejected worker request"
                )
            length = response.getheader("Content-Length")
            if length is not None:
                try:
                    declared = int(length)
                except ValueError as error:
                    raise ValidationError(
                        "WORKER_PROTOCOL_INVALID", "Scout response length is invalid"
                    ) from error
                if declared > maximum:
                    raise ResourceLimitError(
                        "WORKER_RESPONSE_EXCEEDED", "Scout response is too large"
                    )
            body = read_response(response, maximum + 1)
            if len(body) > maximum:
                raise ResourceLimitError("WORKER_RESPONSE_EXCEEDED", "Scout response is too large")
            normalized_headers = {key.lower(): value for key, value in response.getheaders()}
            return response.status, body, normalized_headers
        except (TimeoutError, OSError, http.client.HTTPException) as error:
            raise RemoteWorkerError(
                "WORKER_CONTROLLER_UNAVAILABLE", "Scout controller is unavailable"
            ) from error
        finally:
            finished.set()
            if watchdog is not None:
                watchdog.join()
            if response is not None:
                response.close()
            connection.close()
            if interrupt_socket is not None:
                interrupt_socket.close()

    def exchange_session(self) -> WorkerSession:
        enrollment = _read_secret_file(self.config.enrollment_token_file)
        _status, body, _headers = self._request(
            "POST",
            "/api/worker/v1/session",
            token=enrollment,
            payload={
                "protocol_version": PROTOCOL_VERSION,
                "worker_id": self.config.worker_id,
            },
        )
        return _session_from_mapping(
            _mapping(
                strict_json_loads(body, max_bytes=_MAX_CONTROL_BYTES), "WORKER_SESSION_INVALID"
            )
        )

    def claim(self, session: WorkerSession) -> RemoteClaim | None:
        status, body, _headers = self._request(
            "POST",
            "/api/worker/v1/claim",
            token=session.token,
            payload={
                "protocol_version": PROTOCOL_VERSION,
                "session_id": session.session_id,
                "worker_id": self.config.worker_id,
            },
            expected=(200, 204),
        )
        if status == 204:
            if body:
                raise ValidationError("WORKER_PROTOCOL_INVALID", "Empty claim response had a body")
            return None
        return _claim_from_mapping(
            _mapping(strict_json_loads(body, max_bytes=_MAX_CONTROL_BYTES), "WORKER_CLAIM_INVALID"),
            self.config.max_bundle_bytes,
        )

    def telemetry(
        self,
        session: WorkerSession,
        *,
        cancel_event: threading.Event | None = None,
    ) -> ScoutTelemetry:
        timeout_seconds = min(
            self.config.request_timeout_seconds, _TELEMETRY_REQUEST_TIMEOUT_SECONDS
        )
        _status, body, headers = self._request(
            "POST",
            "/api/worker/v1/telemetry",
            token=session.token,
            payload={
                "protocol_version": PROTOCOL_VERSION,
                "worker_id": self.config.worker_id,
                "session_id": session.session_id,
            },
            maximum=MAX_SCOUT_TELEMETRY_BYTES,
            timeout_seconds=timeout_seconds,
            hard_deadline_seconds=timeout_seconds,
            cancel_event=cancel_event,
        )
        if (
            headers.get("content-type", "").lower() != "application/json; charset=utf-8"
            or headers.get("content-encoding", "identity").lower() != "identity"
            or headers.get("cache-control", "").lower() != "no-store"
            or (
                headers.get("content-length") is not None
                and headers.get("content-length") != str(len(body))
            )
        ):
            raise ValidationError(
                "WORKER_TELEMETRY_INVALID", "Scout telemetry response headers are invalid"
            )
        return parse_scout_telemetry(strict_json_loads(body, max_bytes=MAX_SCOUT_TELEMETRY_BYTES))

    def download(self, session: WorkerSession, artifact: RemoteArtifact) -> bytes:
        try:
            _status, body, headers = self._request(
                "GET",
                artifact.download_path,
                token=session.token,
                expected=(200,),
                maximum=artifact.size_bytes,
            )
            if headers.get("content-encoding", "identity").lower() != "identity":
                raise ValidationError(
                    "WORKER_ARTIFACT_INVALID", "Remote artifact encoding is invalid"
                )
            if (
                headers.get("content-length") != str(artifact.size_bytes)
                or headers.get("x-content-sha256") != artifact.sha256
                or len(body) != artifact.size_bytes
            ):
                raise ValidationError(
                    "WORKER_ARTIFACT_INVALID", "Remote artifact metadata is invalid"
                )
            if hashlib.sha256(body).hexdigest() != artifact.sha256:
                raise ValidationError(
                    "WORKER_ARTIFACT_INVALID", "Remote artifact digest is invalid"
                )
            return body
        except RemoteWorkerError:
            raise
        except AtlasResearchError as error:
            raise RemoteWorkerError(
                "WORKER_ARTIFACT_TRANSFER_INVALID",
                "Scout returned an invalid artifact transfer",
            ) from error

    def heartbeat(
        self,
        session: WorkerSession,
        claim: RemoteClaim,
        sequence: int,
    ) -> HeartbeatReply:
        payload = claim.identity_body()
        payload.update(
            {
                "worker_id": self.config.worker_id,
                "session_id": session.session_id,
                "heartbeat_sequence": sequence,
            }
        )
        _status, body, _headers = self._request(
            "POST",
            "/api/worker/v1/heartbeat",
            token=session.token,
            payload=payload,
        )
        value = _mapping(
            strict_json_loads(body, max_bytes=_MAX_CONTROL_BYTES), "WORKER_HEARTBEAT_INVALID"
        )
        _exact_fields(
            value,
            {"cancelled", "lease_expires_at"},
            "WORKER_HEARTBEAT_INVALID",
        )
        if not isinstance(value.get("cancelled"), bool):
            raise ValidationError(
                "WORKER_HEARTBEAT_INVALID", "Worker heartbeat response is invalid"
            )
        return HeartbeatReply(
            cancelled=cast(bool, value["cancelled"]),
            lease_expires_at=_timestamp(value.get("lease_expires_at"), "WORKER_HEARTBEAT_INVALID"),
        )

    def complete(
        self,
        session: WorkerSession,
        claim: RemoteClaim,
        result: Mapping[str, object],
        result_sha256: str,
    ) -> None:
        payload = claim.identity_body()
        payload.update(
            {
                "worker_id": self.config.worker_id,
                "session_id": session.session_id,
                "result_sha256": result_sha256,
                "result": dict(result),
            }
        )
        try:
            _status, body, _headers = self._request(
                "POST",
                "/api/worker/v1/complete",
                token=session.token,
                payload=payload,
                expected=(200,),
            )
            _terminal_ack_from_mapping(
                _mapping(
                    strict_json_loads(body, max_bytes=_MAX_CONTROL_BYTES),
                    "WORKER_TERMINAL_ACK_INVALID",
                )
            )
        except RemoteWorkerError:
            raise
        except AtlasResearchError as error:
            raise RemoteWorkerError(
                "WORKER_TERMINAL_AMBIGUOUS", "Scout completion acknowledgement is ambiguous"
            ) from error

    def fail(
        self,
        session: WorkerSession,
        claim: RemoteClaim,
        *,
        code: str,
        retryable: bool,
    ) -> None:
        if len(code) > _MAX_ERROR_CODE_CHARS or _ERROR_CODE.fullmatch(code) is None:
            code = "WORKER_FAILED"
        payload = claim.identity_body()
        payload.update(
            {
                "worker_id": self.config.worker_id,
                "session_id": session.session_id,
                "code": code,
                "retryable": retryable,
            }
        )
        try:
            _status, body, _headers = self._request(
                "POST",
                "/api/worker/v1/fail",
                token=session.token,
                payload=payload,
                expected=(200,),
            )
            _terminal_ack_from_mapping(
                _mapping(
                    strict_json_loads(body, max_bytes=_MAX_CONTROL_BYTES),
                    "WORKER_TERMINAL_ACK_INVALID",
                )
            )
        except RemoteWorkerError:
            raise
        except AtlasResearchError as error:
            raise RemoteWorkerError(
                "WORKER_TERMINAL_AMBIGUOUS", "Scout failure acknowledgement is ambiguous"
            ) from error


def _mkdir_private(path: Path) -> Path:
    try:
        path.mkdir(mode=0o700, exist_ok=True)
        metadata = path.lstat()
    except OSError as error:
        raise ValidationError(
            "WORKER_STATE_INVALID", "Worker state directory is unavailable"
        ) from error
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise ValidationError("WORKER_STATE_INVALID", "Worker state directory is unsafe")
    return path


def _mkdir_artifact_parents(root: Path, relative: str) -> None:
    current = root
    for part in relative.split("/")[:-1]:
        current = current / part
        _mkdir_private(current)


def _read_exact_artifact(root: Path, artifact: RemoteArtifact) -> bool:
    path = root.joinpath(*artifact.path.split("/"))
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as error:
        raise ValidationError(
            "WORKER_ARTIFACT_INVALID", "Staged artifact is unavailable"
        ) from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
    ):
        raise ValidationError("WORKER_ARTIFACT_INVALID", "Staged artifact is unsafe")
    if metadata.st_size != artifact.size_bytes:
        raise ValidationError("WORKER_ARTIFACT_INVALID", "Staged artifact size is invalid")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != artifact.sha256:
        raise ValidationError("WORKER_ARTIFACT_INVALID", "Staged artifact digest is invalid")
    return True


def _seal_artifacts(root: Path) -> None:
    for path in sorted(root.rglob("*"), reverse=True):
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise ValidationError("WORKER_ARTIFACT_INVALID", "Staged artifact link is forbidden")
        if stat.S_ISDIR(metadata.st_mode):
            path.chmod(0o555)
        elif stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1:
            path.chmod(0o444)
        else:
            raise ValidationError("WORKER_ARTIFACT_INVALID", "Staged artifact is unsafe")
    root.chmod(0o555)


def _unseal_artifact_directories(root: Path) -> None:
    if not root.exists():
        return
    for path in [root, *root.rglob("*")]:
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise ValidationError("WORKER_ARTIFACT_INVALID", "Staged artifact link is forbidden")
        if stat.S_ISDIR(metadata.st_mode):
            path.chmod(0o700)
        elif not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise ValidationError("WORKER_ARTIFACT_INVALID", "Staged artifact is unsafe")


class _TelemetryPublisher:
    """Publish on a fixed cadence without sharing the claim execution thread."""

    def __init__(
        self,
        publish: Callable[[threading.Event], bool],
        interval_seconds: float,
    ) -> None:
        self._publish = publish
        self._interval_seconds = interval_seconds
        self._done = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("telemetry publisher already started")
        self._thread = threading.Thread(
            target=self._run,
            name="atlas-research-worker-telemetry",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        next_publish = time.monotonic()
        while not self._done.is_set():
            wait_seconds = max(0.0, next_publish - time.monotonic())
            if self._done.wait(wait_seconds):
                return
            with suppress(Exception):
                self._publish(self._done)
            next_publish = max(next_publish + self._interval_seconds, time.monotonic())

    def stop(self) -> bool:
        self._done.set()
        thread = self._thread
        if thread is None:
            return True
        thread.join(timeout=_TELEMETRY_REQUEST_TIMEOUT_SECONDS + 1.0)
        return not thread.is_alive()


class RemoteWorker:
    """Persistent single-concurrency worker supervised by launchd or systemd."""

    def __init__(self, config: WorkerConfig, client: ScoutWorkerClient | None = None) -> None:
        self.config = config
        self.client = client or ScoutWorkerClient(config)
        self.stop_event = threading.Event()
        self._session: WorkerSession | None = None
        self._runs_root = _mkdir_private(config.state_root / "runs")
        self._telemetry_state = "idle"
        self._telemetry_state_lock = threading.Lock()
        self._telemetry_publish_lock = threading.Lock()
        self._last_telemetry_at: datetime | None = None
        self._telemetry_background = False

    def install_signal_handlers(self) -> None:
        def stop(_signum: int, _frame: object) -> None:
            self.stop_event.set()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)

    def _set_telemetry_state(self, state: str) -> None:
        with self._telemetry_state_lock:
            self._telemetry_state = state

    def _publish_telemetry_once(
        self,
        cancel_event: threading.Event | None = None,
    ) -> bool:
        destination = self.config.telemetry_file
        if destination is None or (cancel_event is not None and cancel_event.is_set()):
            return False
        if not self._telemetry_publish_lock.acquire(
            timeout=_TELEMETRY_REQUEST_TIMEOUT_SECONDS + 1.0
        ):
            return False
        try:
            session = self._session
            if session is None or session.expires_at <= datetime.now(UTC):
                return False
            telemetry = self.client.telemetry(session, cancel_event=cancel_event)
            if cancel_event is not None and cancel_event.is_set():
                return False
            if (
                self._last_telemetry_at is not None
                and telemetry.collected_at_value < self._last_telemetry_at
            ):
                return False
            with self._telemetry_state_lock:
                state = self._telemetry_state
            write_worker_telemetry(
                destination,
                telemetry.projection(state),
                watermark=telemetry.collected_at_value,
            )
            self._last_telemetry_at = telemetry.collected_at_value
            return True
        except TelemetryCommitAmbiguousError as error:
            if self._last_telemetry_at is None or error.watermark > self._last_telemetry_at:
                self._last_telemetry_at = error.watermark
            return False
        except TelemetrySnapshotStaleError as error:
            if self._last_telemetry_at is None or error.watermark > self._last_telemetry_at:
                self._last_telemetry_at = error.watermark
            return False
        except Exception:
            return False
        finally:
            self._telemetry_publish_lock.release()

    def _status(
        self,
        state: str,
        *,
        claim: RemoteClaim | None = None,
        error_code: str | None = None,
    ) -> None:
        value = {
            "schema_version": STATUS_SCHEMA_VERSION,
            "worker_id": self.config.worker_id,
            "state": state,
            "updated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "job_id": claim.job_id if claim else None,
            "attempt": claim.attempt if claim else None,
            "fence": claim.fence if claim else None,
            "last_error_code": error_code,
        }
        atomic_write_private(
            self.config.state_root,
            "status.json",
            canonical_json_bytes(value) + b"\n",
            overwrite=True,
            max_bytes=_MAX_CONTROL_BYTES,
        )

    def _status_best_effort(
        self,
        state: str,
        *,
        claim: RemoteClaim | None = None,
        error_code: str | None = None,
    ) -> None:
        try:
            self._status(state, claim=claim, error_code=error_code)
        except (AtlasResearchError, OSError) as error:
            if _local_operational_error(error) is not None:
                return
            raise

    def _session_or_exchange(self) -> WorkerSession:
        def has_margin(session: WorkerSession) -> bool:
            minimum_remaining = timedelta(
                seconds=(
                    self.config.max_job_seconds
                    + (2 * session.lease_seconds)
                    + (2 * self.config.request_timeout_seconds)
                )
            )
            return session.expires_at - datetime.now(UTC) >= minimum_remaining

        session = self._session
        if session is None or not has_margin(session):
            self._status("authenticating")
            session = self.client.exchange_session()
            self._session = session
        if not has_margin(session):
            self._session = None
            raise RemoteWorkerError(
                "WORKER_SESSION_TOO_SHORT", "Worker session cannot cover one bounded claim"
            )
        return session

    def _run_root(self, claim: RemoteClaim) -> Path:
        name = f"{claim.job_id}-{claim.attempt}-{claim.fence}-{claim.cancellation_generation}"
        parent = _mkdir_private(self._runs_root / name)
        run_root = _mkdir_private(parent / "atlas-research-run")
        artifacts = run_root / "artifacts"
        output = run_root / "output"
        if artifacts.exists():
            metadata = artifacts.lstat()
            if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
                raise ValidationError("WORKER_STATE_INVALID", "Worker artifact directory is unsafe")
            _unseal_artifact_directories(artifacts)
        else:
            artifacts.mkdir(mode=0o700)
        _mkdir_private(output)
        return run_root

    def _run_parent(self, claim: RemoteClaim) -> Path:
        name = f"{claim.job_id}-{claim.attempt}-{claim.fence}-{claim.cancellation_generation}"
        return self._runs_root / name

    def _empty_directory_at(self, parent_fd: int, name: str, *, depth: int = 0) -> None:
        if depth > 64:
            raise ValidationError("WORKER_CLEANUP_INVALID", "Worker run tree nesting is unsafe")
        required_flags = ("O_DIRECTORY", "O_NOFOLLOW")
        if any(not hasattr(os, flag) for flag in required_flags):
            raise ValidationError(
                "WORKER_CLEANUP_INVALID", "Worker cleanup is unsafe on this platform"
            )
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_DIRECTORY | os.O_NOFOLLOW
        parent_metadata = os.fstat(parent_fd)
        before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if not stat.S_ISDIR(before.st_mode) or before.st_dev != parent_metadata.st_dev:
            raise ValidationError("WORKER_CLEANUP_INVALID", "Worker run directory is unsafe")
        directory_fd = os.open(name, flags, dir_fd=parent_fd)
        try:
            metadata = os.fstat(directory_fd)
            if (metadata.st_dev, metadata.st_ino) != (before.st_dev, before.st_ino):
                raise ValidationError("WORKER_CLEANUP_INVALID", "Worker run directory is unsafe")
            os.fchmod(directory_fd, 0o700)
            for child in sorted(os.listdir(directory_fd)):
                child_metadata = os.stat(child, dir_fd=directory_fd, follow_symlinks=False)
                if stat.S_ISDIR(child_metadata.st_mode):
                    self._empty_directory_at(directory_fd, child, depth=depth + 1)
                    os.rmdir(child, dir_fd=directory_fd)
                else:
                    os.unlink(child, dir_fd=directory_fd)
        finally:
            os.close(directory_fd)

    def _remove_direct_run_entry(self, entry: Path) -> None:
        if (
            entry.parent != self._runs_root
            or entry == self._runs_root
            or entry.name in {"", ".", ".."}
        ):
            raise ValidationError(
                "WORKER_CLEANUP_INVALID", "Worker cleanup target is outside the active claim"
            )
        required_flags = ("O_DIRECTORY", "O_NOFOLLOW")
        if any(not hasattr(os, flag) for flag in required_flags):
            raise ValidationError(
                "WORKER_CLEANUP_INVALID", "Worker cleanup is unsafe on this platform"
            )
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_DIRECTORY | os.O_NOFOLLOW
        runs_fd = -1
        try:
            runs_fd = os.open(self._runs_root, flags)
            metadata = os.stat(entry.name, dir_fd=runs_fd, follow_symlinks=False)
            if stat.S_ISDIR(metadata.st_mode):
                self._empty_directory_at(runs_fd, entry.name)
                os.rmdir(entry.name, dir_fd=runs_fd)
            else:
                os.unlink(entry.name, dir_fd=runs_fd)
        except FileNotFoundError:
            return
        except OSError as error:
            raise ValidationError(
                "WORKER_CLEANUP_FAILED", "Worker run entry cannot be removed safely"
            ) from error
        finally:
            if runs_fd >= 0:
                os.close(runs_fd)

    def _cleanup_run_root(self, run_root: Path, claim: RemoteClaim) -> None:
        expected_parent = self._run_parent(claim)
        expected_run_root = expected_parent / "atlas-research-run"
        if run_root != expected_run_root:
            raise ValidationError(
                "WORKER_CLEANUP_INVALID", "Worker cleanup target is outside the active claim"
            )
        self._remove_direct_run_entry(expected_parent)

    def _prune_runs(self, keep: RemoteClaim | None) -> None:
        keep_parent = self._run_parent(keep) if keep is not None else None
        try:
            entries = sorted(self._runs_root.iterdir(), key=lambda path: path.name)
            for entry in entries:
                if entry == keep_parent:
                    continue
                self._remove_direct_run_entry(entry)
        except RemoteWorkerError:
            raise
        except (AtlasResearchError, OSError) as error:
            raise RemoteWorkerError(
                "WORKER_RUN_PRUNE_FAILED", "Worker stale-run pruning failed"
            ) from error

    def _free_space(self) -> int:
        try:
            return shutil.disk_usage(self._runs_root).free
        except OSError as error:
            raise RemoteWorkerError(
                "WORKER_STORAGE_UNAVAILABLE", "Worker free space cannot be measured"
            ) from error

    def _require_free_space(self, claim: RemoteClaim) -> None:
        required = sum(artifact.size_bytes for artifact in claim.artifacts)
        required += _FREE_SPACE_RESERVE_BYTES
        if self._free_space() < required:
            raise RemoteWorkerError(
                "WORKER_STORAGE_LOW", "Worker does not have enough free space for the claim"
            )

    def _require_reserve(self) -> None:
        if self._free_space() < _FREE_SPACE_RESERVE_BYTES:
            raise RemoteWorkerError(
                "WORKER_STORAGE_LOW", "Worker safety reserve was consumed during the claim"
            )

    def _cleanup_after_terminal(self, run_root: Path | None, claim: RemoteClaim) -> bool:
        if run_root is None:
            return True
        try:
            self._cleanup_run_root(run_root, claim)
        except AtlasResearchError as error:
            self._status_best_effort("cleanup_failed", claim=claim, error_code=error.code)
            return False
        return True

    def _stage(
        self,
        session: WorkerSession,
        claim: RemoteClaim,
        run_root: Path,
        supervisor: _ClaimSupervisor,
    ) -> None:
        artifacts_root = run_root / "artifacts"
        for artifact in claim.artifacts:
            supervisor.check()
            _mkdir_artifact_parents(artifacts_root, artifact.path)
            if _read_exact_artifact(artifacts_root, artifact):
                continue
            data = self.client.download(session, artifact)
            supervisor.check()
            atomic_write_private(
                artifacts_root,
                artifact.path,
                data,
                max_bytes=MAX_ARTIFACT_BYTES,
            )
        supervisor.check()
        _seal_artifacts(artifacts_root)

    def _terminate(self, process: subprocess.Popen[bytes]) -> None:
        if process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            with suppress(OSError):
                os.killpg(process.pid, signal.SIGKILL)
            with suppress(subprocess.TimeoutExpired):
                process.wait(timeout=5)

    def _execute(
        self,
        session: WorkerSession,
        claim: RemoteClaim,
        run_root: Path,
        supervisor: _ClaimSupervisor,
    ) -> int:
        command = [
            str(self.config.executor_path),
            "--run-root",
            str(run_root),
            "--worker-id",
            self.config.worker_id,
            "--session-id",
            session.session_id,
            "--job-path",
            f"/artifacts/{claim.job_path}",
            "--result-uri",
            "result.json",
        ]
        allowed_environment = {
            key: os.environ[key]
            for key in ("HOME", "PATH", "TMPDIR", "LANG", "LC_ALL")
            if key in os.environ
        }
        supervisor.check()
        try:
            process = subprocess.Popen(
                command,
                cwd=self.config.executor_path.parent,
                env=allowed_environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except OSError as error:
            raise ValidationError(
                "WORKER_EXECUTOR_UNAVAILABLE", "Worker executor could not be started"
            ) from error
        try:
            while process.poll() is None:
                self.stop_event.wait(0.25)
                supervisor.check()
            supervisor.check()
            return process.wait()
        finally:
            if process.poll() is None:
                self._terminate(process)

    def _load_claim_job(self, run_root: Path, claim: RemoteClaim) -> ResearchJob:
        job = load_job(run_root / "artifacts" / claim.job_path)
        if job.job_id != claim.job_id or job.attempt != claim.attempt:
            raise ValidationError(
                "WORKER_JOB_IDENTITY_INVALID", "Staged job does not match the active claim"
            )
        return job

    def _result(
        self,
        run_root: Path,
        job: ResearchJob,
        session: WorkerSession,
    ) -> tuple[Mapping[str, object], str] | None:
        resolved = read_private_bytes(
            run_root / "output", "result.json", max_bytes=_MAX_CONTROL_BYTES
        )
        if resolved is None:
            return None
        _path, data = resolved
        value = _mapping(
            strict_json_loads(data, max_bytes=_MAX_CONTROL_BYTES), "WORKER_RESULT_INVALID"
        )
        try:
            validate_result_document(
                value,
                job,
                expected_worker_id=self.config.worker_id,
                expected_session_id=session.session_id,
            )
        except AtlasResearchError as error:
            raise ValidationError(
                "WORKER_RESULT_INVALID", "Executor result does not match the active claim"
            ) from error
        canonical = canonical_json_bytes(value)
        if len(canonical) > _MAX_RESULT_BYTES:
            raise ResourceLimitError(
                "WORKER_RESULT_EXCEEDED", "Canonical executor result is too large"
            )
        return value, hashlib.sha256(canonical).hexdigest()

    def _process_claim(
        self,
        session: WorkerSession,
        claim: RemoteClaim,
        claim_started_at: float,
    ) -> RunOutcome:
        supervisor = _ClaimSupervisor(
            self.client,
            session,
            claim,
            self.stop_event,
            claim_started_at + self.config.max_job_seconds,
            self.config.request_timeout_seconds,
        )
        run_root: Path | None = None
        try:
            supervisor.start()
            self._prune_runs(claim)
            supervisor.check()
            self._require_free_space(claim)
            supervisor.check()
            self._status("staging", claim=claim)
            run_root = self._run_root(claim)
            self._stage(session, claim, run_root, supervisor)
            job = self._load_claim_job(run_root, claim)
            supervisor.check()
            replayed = self._result(run_root, job, session)
            if replayed is None:
                self._status("running", claim=claim)
                return_code = self._execute(session, claim, run_root, supervisor)
                result = self._result(run_root, job, session)
                if result is None:
                    self._require_reserve()
                    code = "EXECUTOR_FAILED" if return_code else "RESULT_MISSING"
                    raise ValidationError(code, "Executor did not produce a valid result")
            else:
                result = replayed
            value, digest = result
            supervisor.stop_and_check()
            self.client.complete(session, claim, value, digest)
            if self._cleanup_after_terminal(run_root, claim):
                self._status("completed", claim=claim)
            return RunOutcome("completed", job_id=claim.job_id, result_sha256=digest)
        except _ClaimCancelled as cancellation:
            supervisor.stop()
            if not cancellation.confirmed or self._cleanup_after_terminal(run_root, claim):
                self._status("cancelled", claim=claim)
            return RunOutcome("cancelled", job_id=claim.job_id)
        except RemoteWorkerError as error:
            self._status_best_effort("backoff", claim=claim, error_code=error.code)
            raise
        except OSError as error:
            operational = _local_operational_error(error)
            assert operational is not None
            self._status_best_effort("backoff", claim=claim, error_code=operational.code)
            raise operational from error
        except AtlasResearchError as error:
            operational = _local_operational_error(error)
            if operational is not None:
                self._status_best_effort("backoff", claim=claim, error_code=operational.code)
                raise operational from error
            try:
                supervisor.stop_and_check()
            except _ClaimCancelled as cancellation:
                if not cancellation.confirmed or self._cleanup_after_terminal(run_root, claim):
                    self._status("cancelled", claim=claim)
                return RunOutcome("cancelled", job_id=claim.job_id)
            except RemoteWorkerError as supervisor_error:
                self._status_best_effort("backoff", claim=claim, error_code=supervisor_error.code)
                raise
            retryable = error.code in {
                "EXECUTOR_FAILED",
                "WORKER_EXECUTOR_UNAVAILABLE",
                "WORKER_STATE_INVALID",
            }
            try:
                self.client.fail(session, claim, code=error.code, retryable=retryable)
            except RemoteWorkerError as terminal_error:
                self._status_best_effort("backoff", claim=claim, error_code=terminal_error.code)
                raise
            if self._cleanup_after_terminal(run_root, claim):
                self._status("failed", claim=claim, error_code=error.code)
            return RunOutcome("failed", job_id=claim.job_id, error_code=error.code)
        finally:
            supervisor.stop()

    def run_once(self) -> RunOutcome:
        try:
            session = self._session_or_exchange()
            self._status("claiming")
            claim = self.client.claim(session)
            if claim is None:
                self._prune_runs(None)
                self._status("idle")
                outcome = RunOutcome("idle")
            else:
                self._set_telemetry_state("running")
                claim_started_at = time.monotonic()
                outcome = self._process_claim(session, claim, claim_started_at)
            self._set_telemetry_state("idle")
            return outcome
        except (OSError, AtlasResearchError):
            self._set_telemetry_state("degraded")
            raise
        finally:
            if not self._telemetry_background:
                self._publish_telemetry_once()

    def serve(self) -> RunOutcome:
        delay = self.config.poll_seconds
        last = RunOutcome("starting")
        self._status_best_effort("starting")
        publisher: _TelemetryPublisher | None = None
        if self.config.telemetry_file is not None:
            self._telemetry_background = True
            publisher = _TelemetryPublisher(
                self._publish_telemetry_once, _TELEMETRY_PUBLISH_INTERVAL_SECONDS
            )
            publisher.start()
        try:
            while not self.stop_event.is_set():
                try:
                    last = self.run_once()
                    delay = self.config.poll_seconds
                except RemoteWorkerError as error:
                    if error.code == "WORKER_AUTH_REJECTED":
                        self._session = None
                    self._status_best_effort("backoff", error_code=error.code)
                    last = RunOutcome("backoff", error_code=error.code)
                    delay = min(max(self.config.poll_seconds, delay * 2), 60.0)
                except OSError as error:
                    operational = _local_operational_error(error)
                    assert operational is not None
                    code = operational.code
                    self._status_best_effort("backoff", error_code=code)
                    last = RunOutcome("backoff", error_code=code)
                    delay = min(max(self.config.poll_seconds, delay * 2), 60.0)
                except AtlasResearchError as error:
                    operational = _local_operational_error(error)
                    code = operational.code if operational is not None else error.code
                    self._status_best_effort("backoff", error_code=code)
                    last = RunOutcome("backoff", error_code=code)
                    delay = min(max(self.config.poll_seconds, delay * 2), 60.0)
                if not self.stop_event.is_set():
                    self.stop_event.wait(delay)
            return last
        finally:
            self._set_telemetry_state("offline")
            publisher_stopped = publisher is None or publisher.stop()
            self._telemetry_background = False
            if publisher_stopped:
                self._publish_telemetry_once()
            self._status_best_effort("stopped")


def load_worker_config(path: Path) -> WorkerConfig:
    data = _read_bounded_private_file(path, maximum=_MAX_CONTROL_BYTES)
    return WorkerConfig.from_mapping(
        _mapping(strict_json_loads(data, max_bytes=_MAX_CONTROL_BYTES), "WORKER_CONFIG_INVALID")
    )


def _read_bounded_private_file(path: Path, *, maximum: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    try:
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) & 0o077
            or before.st_size > maximum
        ):
            raise ValidationError("WORKER_CONFIG_INVALID", "Worker config file is unsafe")
        data = os.read(descriptor, maximum + 1)
        after = os.fstat(descriptor)
        if len(data) > maximum or (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValidationError("WORKER_CONFIG_INVALID", "Worker config file is unsafe")
        return data
    except OSError as error:
        raise ValidationError(
            "WORKER_CONFIG_INVALID", "Worker config cannot be read safely"
        ) from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)


__all__ = [
    "PROTOCOL_VERSION",
    "RemoteArtifact",
    "RemoteClaim",
    "RemoteWorker",
    "RemoteWorkerError",
    "RunOutcome",
    "ScoutWorkerClient",
    "WorkerConfig",
    "WorkerSession",
    "load_worker_config",
]

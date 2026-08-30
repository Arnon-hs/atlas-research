# SPDX-License-Identifier: MIT
"""Bounded Scout telemetry parsing and sanitized local projection writes."""

from __future__ import annotations

import os
import re
import secrets
import stat
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final, cast

from .canonical import canonical_json_bytes, strict_json_loads
from .errors import ResourceLimitError, ValidationError

TELEMETRY_WORKER_ID: Final = "atlasrepo"
TELEMETRY_SCHEMA_VERSION: Final = 1
TELEMETRY_STATES: Final = frozenset({"idle", "running", "degraded", "offline"})
MAX_TELEMETRY_BYTES: Final = 256 << 10
MAX_SCOUT_TELEMETRY_BYTES: Final = 64 << 10
MAX_TELEMETRY_HISTORY: Final = 120
MAX_SAFE_INTEGER: Final = (1 << 53) - 1
TELEMETRY_FRESH_SECONDS: Final = 30

_TIMESTAMP = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$",
    re.ASCII,
)
_O_CLOEXEC: Final = getattr(os, "O_CLOEXEC", 0)
_O_DIRECTORY: Final = getattr(os, "O_DIRECTORY", 0)
_O_NOFOLLOW: Final = getattr(os, "O_NOFOLLOW", 0)


@dataclass(frozen=True, slots=True)
class TelemetryHistoryPoint:
    at: str
    at_value: datetime
    processed: int
    failed: int


@dataclass(frozen=True, slots=True)
class ScoutTelemetry:
    collected_at: str
    collected_at_value: datetime
    pending: int
    in_flight: int
    queue_failed: int
    processed: int
    failed: int
    history: tuple[TelemetryHistoryPoint, ...]

    def projection(self, state: str) -> dict[str, object]:
        if state not in TELEMETRY_STATES:
            raise ValidationError("WORKER_TELEMETRY_STATE_INVALID", "Worker state is invalid")
        return {
            "schema_version": TELEMETRY_SCHEMA_VERSION,
            "worker_id": TELEMETRY_WORKER_ID,
            "state": state,
            "updated_at": self.collected_at,
            "queue": {
                "pending": self.pending,
                "in_flight": self.in_flight,
                "failed": self.queue_failed,
            },
            "totals": {"processed": self.processed, "failed": self.failed},
            "active_model": None,
            "history": [
                {"at": item.at, "processed": item.processed, "failed": item.failed}
                for item in self.history
            ],
        }


class TelemetryCommitAmbiguousError(ValidationError):
    """The replacement is visible but its durability or final metadata is unknown."""

    watermark: datetime

    def __init__(self, watermark: datetime) -> None:
        self.watermark = watermark
        super().__init__(
            "WORKER_TELEMETRY_COMMIT_AMBIGUOUS",
            "Worker telemetry replacement is visible but its commit is ambiguous",
        )


class TelemetrySnapshotStaleError(ValidationError):
    """A persisted projection is newer than the attempted Scout snapshot."""

    watermark: datetime

    def __init__(self, watermark: datetime) -> None:
        self.watermark = watermark
        super().__init__(
            "WORKER_TELEMETRY_SNAPSHOT_STALE",
            "Worker telemetry snapshot is older than the persisted projection",
        )


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ValidationError(
            "WORKER_TELEMETRY_INVALID", "Scout telemetry response must be an object"
        )
    return cast(Mapping[str, object], value)


def _exact_fields(value: Mapping[str, object], fields: set[str]) -> None:
    if set(value) != fields:
        raise ValidationError(
            "WORKER_TELEMETRY_INVALID", "Scout telemetry response fields are invalid"
        )


def _counter(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_SAFE_INTEGER:
        raise ValidationError("WORKER_TELEMETRY_INVALID", "Scout telemetry counter is invalid")
    return value


def _timestamp(value: object, *, minute: bool = False) -> tuple[str, datetime]:
    if not isinstance(value, str) or _TIMESTAMP.fullmatch(value) is None:
        raise ValidationError("WORKER_TELEMETRY_INVALID", "Scout telemetry timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00").astimezone(UTC)
    except ValueError as error:
        raise ValidationError(
            "WORKER_TELEMETRY_INVALID", "Scout telemetry timestamp is invalid"
        ) from error
    if minute and (parsed.second != 0 or parsed.microsecond != 0):
        raise ValidationError(
            "WORKER_TELEMETRY_INVALID", "Scout telemetry history timestamp is invalid"
        )
    return value, parsed


def parse_scout_telemetry(
    value: object,
    *,
    now: datetime | None = None,
) -> ScoutTelemetry:
    """Parse the exact bounded response exposed by Scout protocol v1."""

    telemetry = _mapping(value)
    _exact_fields(telemetry, {"protocol_version", "collected_at", "queue", "totals", "history"})
    if telemetry.get("protocol_version") != "1":
        raise ValidationError(
            "WORKER_TELEMETRY_INVALID", "Scout telemetry protocol version is invalid"
        )
    collected_at, collected_at_value = _timestamp(telemetry.get("collected_at"))
    current = (now or datetime.now(UTC)).astimezone(UTC)
    freshness = timedelta(seconds=TELEMETRY_FRESH_SECONDS)
    if collected_at_value < current - freshness or collected_at_value > current + freshness:
        raise ValidationError("WORKER_TELEMETRY_INVALID", "Scout telemetry snapshot is not fresh")

    queue = _mapping(telemetry.get("queue"))
    _exact_fields(queue, {"pending", "in_flight", "failed"})
    pending = _counter(queue.get("pending"))
    in_flight = _counter(queue.get("in_flight"))
    queue_failed = _counter(queue.get("failed"))

    totals = _mapping(telemetry.get("totals"))
    _exact_fields(totals, {"processed", "failed"})
    processed = _counter(totals.get("processed"))
    failed = _counter(totals.get("failed"))
    if failed > processed:
        raise ValidationError("WORKER_TELEMETRY_INVALID", "Scout telemetry totals are invalid")

    raw_history = telemetry.get("history")
    if not isinstance(raw_history, list) or len(raw_history) > MAX_TELEMETRY_HISTORY:
        raise ValidationError("WORKER_TELEMETRY_INVALID", "Scout telemetry history is invalid")
    collected_minute = collected_at_value.replace(second=0, microsecond=0)
    history: list[TelemetryHistoryPoint] = []
    previous_at: datetime | None = None
    previous_processed = 0
    previous_failed = 0
    for raw_item in raw_history:
        item = _mapping(raw_item)
        _exact_fields(item, {"at", "processed", "failed"})
        at, at_value = _timestamp(item.get("at"), minute=True)
        item_processed = _counter(item.get("processed"))
        item_failed = _counter(item.get("failed"))
        if (
            (previous_at is not None and at_value <= previous_at)
            or at_value > collected_minute
            or item_processed < previous_processed
            or item_failed < previous_failed
            or item_failed > item_processed
            or item_processed > processed
            or item_failed > failed
        ):
            raise ValidationError(
                "WORKER_TELEMETRY_INVALID", "Scout telemetry history is not monotonic"
            )
        history.append(TelemetryHistoryPoint(at, at_value, item_processed, item_failed))
        previous_at = at_value
        previous_processed = item_processed
        previous_failed = item_failed
    history_required = processed != 0 or failed != 0
    if bool(history) != history_required or (
        history and (previous_processed != processed or previous_failed != failed)
    ):
        raise ValidationError(
            "WORKER_TELEMETRY_INVALID", "Scout telemetry history does not match totals"
        )
    return ScoutTelemetry(
        collected_at=collected_at,
        collected_at_value=collected_at_value,
        pending=pending,
        in_flight=in_flight,
        queue_failed=queue_failed,
        processed=processed,
        failed=failed,
        history=tuple(history),
    )


def _open_parent(path: Path) -> int:
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise ValidationError(
            "WORKER_TELEMETRY_PATH_INVALID", "Worker telemetry path must be absolute"
        )
    if (
        _O_DIRECTORY == 0
        or _O_NOFOLLOW == 0
        or os.open not in os.supports_dir_fd
        or os.stat not in os.supports_dir_fd
        or os.stat not in os.supports_follow_symlinks
        or os.unlink not in os.supports_dir_fd
    ):
        raise ValidationError(
            "WORKER_TELEMETRY_PATH_INVALID", "Worker telemetry path is unsafe on this platform"
        )
    flags = os.O_RDONLY | _O_CLOEXEC | _O_DIRECTORY | _O_NOFOLLOW
    descriptor = os.open(path.anchor, flags)
    try:
        for part in path.parent.parts[1:]:
            if part in {"", ".", ".."}:
                raise ValidationError(
                    "WORKER_TELEMETRY_PATH_INVALID", "Worker telemetry parent is unsafe"
                )
            next_descriptor = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) & 0o077
        ):
            raise ValidationError(
                "WORKER_TELEMETRY_PATH_INVALID", "Worker telemetry parent is unsafe"
            )
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _validate_existing_target(parent_fd: int, name: str) -> os.stat_result | None:
    try:
        metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_size > MAX_TELEMETRY_BYTES
    ):
        raise ValidationError(
            "WORKER_TELEMETRY_PATH_INVALID", "Existing worker telemetry file is unsafe"
        )
    return metadata


def _read_existing_watermark(parent_fd: int, name: str) -> datetime | None:
    path_metadata = _validate_existing_target(parent_fd, name)
    if path_metadata is None:
        return None
    descriptor = -1
    try:
        descriptor = os.open(name, os.O_RDONLY | _O_CLOEXEC | _O_NOFOLLOW, dir_fd=parent_fd)
        before = os.fstat(descriptor)
        if (
            before.st_dev != path_metadata.st_dev
            or before.st_ino != path_metadata.st_ino
            or before.st_uid != path_metadata.st_uid
            or before.st_nlink != path_metadata.st_nlink
            or stat.S_IMODE(before.st_mode) != stat.S_IMODE(path_metadata.st_mode)
            or before.st_size != path_metadata.st_size
        ):
            raise ValidationError(
                "WORKER_TELEMETRY_WRITE_FAILED",
                "Existing worker telemetry file changed during validation",
            )
        chunks: list[bytes] = []
        remaining = MAX_TELEMETRY_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(descriptor)
        if (
            len(data) > MAX_TELEMETRY_BYTES
            or len(data) != before.st_size
            or after.st_dev != before.st_dev
            or after.st_ino != before.st_ino
            or after.st_nlink != before.st_nlink
            or after.st_size != before.st_size
            or after.st_mtime_ns != before.st_mtime_ns
            or after.st_ctime_ns != before.st_ctime_ns
        ):
            raise ValidationError(
                "WORKER_TELEMETRY_WRITE_FAILED",
                "Existing worker telemetry file changed during validation",
            )
        value = strict_json_loads(data, max_bytes=MAX_TELEMETRY_BYTES)
        projection = _mapping(value)
        _exact_fields(
            projection,
            {
                "schema_version",
                "worker_id",
                "state",
                "updated_at",
                "queue",
                "totals",
                "active_model",
                "history",
            },
        )
        if (
            projection.get("schema_version") != TELEMETRY_SCHEMA_VERSION
            or projection.get("worker_id") != TELEMETRY_WORKER_ID
            or projection.get("state") not in TELEMETRY_STATES
            or projection.get("active_model") is not None
            or canonical_json_bytes(projection) != data
        ):
            raise ValidationError(
                "WORKER_TELEMETRY_WRITE_FAILED",
                "Existing worker telemetry projection is invalid",
            )
        _updated_at, watermark = _timestamp(projection.get("updated_at"))
        return watermark
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def validate_telemetry_destination(path: Path) -> None:
    """Reject unsafe parents and pre-existing telemetry targets."""

    descriptor = -1
    try:
        descriptor = _open_parent(path)
        _validate_existing_target(descriptor, path.name)
    except OSError as error:
        raise ValidationError(
            "WORKER_TELEMETRY_PATH_INVALID", "Worker telemetry path cannot be opened safely"
        ) from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def write_worker_telemetry(
    path: Path,
    projection: Mapping[str, object],
    *,
    watermark: datetime,
) -> None:
    """Atomically replace one private telemetry projection in its destination directory."""

    data = canonical_json_bytes(projection)
    if len(data) > MAX_TELEMETRY_BYTES:
        raise ResourceLimitError(
            "WORKER_TELEMETRY_EXCEEDED", "Worker telemetry projection exceeds the byte limit"
        )
    parent_fd = -1
    temporary_fd = -1
    temporary_exists = False
    renamed = False
    temporary_name = f".atlas-research-telemetry-{secrets.token_hex(16)}.tmp"
    try:
        parent_fd = _open_parent(path)
        _validate_existing_target(parent_fd, path.name)
        temporary_fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_CLOEXEC | _O_NOFOLLOW,
            0o600,
            dir_fd=parent_fd,
        )
        temporary_exists = True
        view = memoryview(data)
        offset = 0
        while offset < len(view):
            written = os.write(temporary_fd, view[offset:])
            if written <= 0:
                raise OSError("telemetry write made no progress")
            offset += written
        os.fchmod(temporary_fd, 0o600)
        os.fsync(temporary_fd)
        temporary_metadata = os.fstat(temporary_fd)
        if (
            not stat.S_ISREG(temporary_metadata.st_mode)
            or temporary_metadata.st_nlink != 1
            or stat.S_IMODE(temporary_metadata.st_mode) != 0o600
        ):
            raise ValidationError(
                "WORKER_TELEMETRY_WRITE_FAILED", "Worker telemetry temporary file is unsafe"
            )
        os.close(temporary_fd)
        temporary_fd = -1
        persisted_watermark = _read_existing_watermark(parent_fd, path.name)
        if persisted_watermark is not None and persisted_watermark > watermark:
            raise TelemetrySnapshotStaleError(persisted_watermark)
        os.replace(
            temporary_name,
            path.name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        renamed = True
        temporary_exists = False
        os.fsync(parent_fd)
        committed = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(committed.st_mode)
            or committed.st_dev != temporary_metadata.st_dev
            or committed.st_ino != temporary_metadata.st_ino
            or committed.st_nlink != 1
            or stat.S_IMODE(committed.st_mode) != 0o600
        ):
            raise ValidationError(
                "WORKER_TELEMETRY_WRITE_FAILED", "Worker telemetry commit is unsafe"
            )
    except (ValidationError, OSError) as error:
        if renamed:
            raise TelemetryCommitAmbiguousError(watermark) from error
        if isinstance(error, ValidationError):
            raise
        raise ValidationError(
            "WORKER_TELEMETRY_WRITE_FAILED", "Worker telemetry could not be committed safely"
        ) from error
    finally:
        if temporary_fd >= 0:
            os.close(temporary_fd)
        if temporary_exists and parent_fd >= 0:
            with suppress(OSError):
                os.unlink(temporary_name, dir_fd=parent_fd)
        if parent_fd >= 0:
            os.close(parent_fd)


__all__ = [
    "MAX_SAFE_INTEGER",
    "MAX_SCOUT_TELEMETRY_BYTES",
    "MAX_TELEMETRY_HISTORY",
    "TELEMETRY_FRESH_SECONDS",
    "TELEMETRY_SCHEMA_VERSION",
    "TELEMETRY_STATES",
    "TELEMETRY_WORKER_ID",
    "ScoutTelemetry",
    "TelemetryCommitAmbiguousError",
    "TelemetrySnapshotStaleError",
    "parse_scout_telemetry",
    "validate_telemetry_destination",
    "write_worker_telemetry",
]

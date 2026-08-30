# SPDX-License-Identifier: MIT

from __future__ import annotations

import copy
import errno
import json
import os
import stat
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

import atlas_research.operations_telemetry as telemetry_module
from atlas_research.errors import ValidationError
from atlas_research.operations_telemetry import (
    MAX_SAFE_INTEGER,
    MAX_TELEMETRY_HISTORY,
    TELEMETRY_FRESH_SECONDS,
    TelemetryCommitAmbiguousError,
    parse_scout_telemetry,
    validate_telemetry_destination,
    write_worker_telemetry,
)


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _response(now: datetime) -> dict[str, object]:
    minute = now.astimezone(UTC).replace(second=0, microsecond=0)
    return {
        "protocol_version": "1",
        "collected_at": _timestamp(now),
        "queue": {"pending": 4, "in_flight": 1, "failed": 2},
        "totals": {"processed": 12, "failed": 2},
        "history": [
            {"at": _timestamp(minute - timedelta(minutes=1)), "processed": 10, "failed": 1},
            {"at": _timestamp(minute), "processed": 12, "failed": 2},
        ],
    }


def test_scout_telemetry_projection_is_exact_and_sanitized() -> None:
    now = datetime.now(UTC).replace(microsecond=123_000)
    telemetry = parse_scout_telemetry(_response(now), now=now)

    assert telemetry.projection("running") == {
        "schema_version": 1,
        "worker_id": "atlasrepo",
        "state": "running",
        "updated_at": _timestamp(now),
        "queue": {"pending": 4, "in_flight": 1, "failed": 2},
        "totals": {"processed": 12, "failed": 2},
        "active_model": None,
        "history": [
            {
                "at": _timestamp(now.replace(second=0, microsecond=0) - timedelta(minutes=1)),
                "processed": 10,
                "failed": 1,
            },
            {
                "at": _timestamp(now.replace(second=0, microsecond=0)),
                "processed": 12,
                "failed": 2,
            },
        ],
    }


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(extra=True),
        lambda value: cast(dict[str, object], value["queue"]).update(extra=1),
        lambda value: cast(dict[str, object], value["totals"]).update(failed=13),
        lambda value: cast(dict[str, object], value["queue"]).update(pending=True),
        lambda value: cast(dict[str, object], value["queue"]).update(pending=-1),
        lambda value: cast(dict[str, object], value["queue"]).update(pending=MAX_SAFE_INTEGER + 1),
        lambda value: cast(dict[str, object], value["totals"]).update(
            processed=MAX_SAFE_INTEGER + 1
        ),
        lambda value: value.update(history=cast(list[object], value["history"]) * 61),
        lambda value: cast(list[dict[str, object]], value["history"])[1].update(
            at=cast(list[dict[str, object]], value["history"])[0]["at"]
        ),
        lambda value: cast(list[dict[str, object]], value["history"])[1].update(processed=9),
        lambda value: cast(list[dict[str, object]], value["history"])[1].update(failed=3),
        lambda value: cast(dict[str, object], value["totals"]).update(processed=13),
        lambda value: cast(dict[str, object], value["totals"]).update(processed=0, failed=0),
        lambda value: value.update(history=[]),
    ],
)
def test_scout_telemetry_rejects_extended_unbounded_or_inconsistent_values(
    mutate: Callable[[dict[str, object]], None],
) -> None:
    now = datetime.now(UTC).replace(microsecond=123_000)
    value = copy.deepcopy(_response(now))
    mutate(value)

    with pytest.raises(ValidationError, match="telemetry"):
        parse_scout_telemetry(value, now=now)


def test_scout_telemetry_rejects_bad_timestamps_and_clock_drift() -> None:
    now = datetime.now(UTC).replace(microsecond=123_000)
    for collected_at in [
        now + timedelta(seconds=TELEMETRY_FRESH_SECONDS + 1),
        now - timedelta(seconds=TELEMETRY_FRESH_SECONDS + 1),
    ]:
        value = _response(now)
        value["collected_at"] = _timestamp(collected_at)
        with pytest.raises(ValidationError, match="fresh"):
            parse_scout_telemetry(value, now=now)

    invalid_lexical = _response(now)
    invalid_lexical["collected_at"] = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    with pytest.raises(ValidationError, match="timestamp"):
        parse_scout_telemetry(invalid_lexical, now=now)

    non_minute = _response(now)
    cast(list[dict[str, object]], non_minute["history"])[-1]["at"] = _timestamp(
        now.replace(microsecond=0)
    )
    with pytest.raises(ValidationError, match="history timestamp"):
        parse_scout_telemetry(non_minute, now=now)

    future_history = _response(now)
    future_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    cast(list[dict[str, object]], future_history["history"])[-1]["at"] = _timestamp(future_minute)
    with pytest.raises(ValidationError, match="monotonic"):
        parse_scout_telemetry(future_history, now=now)


@pytest.mark.parametrize("offset", [-TELEMETRY_FRESH_SECONDS, TELEMETRY_FRESH_SECONDS])
def test_scout_telemetry_accepts_inclusive_thirty_second_clock_skew(offset: int) -> None:
    now = datetime.now(UTC).replace(microsecond=123_000)
    value = _response(now)
    value["collected_at"] = _timestamp(now + timedelta(seconds=offset))
    value["totals"] = {"processed": 0, "failed": 0}
    value["history"] = []

    assert parse_scout_telemetry(value, now=now).collected_at == value["collected_at"]


def test_empty_history_is_allowed_only_for_zero_totals() -> None:
    now = datetime.now(UTC).replace(microsecond=123_000)
    value = _response(now)
    value["totals"] = {"processed": 0, "failed": 0}
    value["history"] = []
    telemetry = parse_scout_telemetry(value, now=now)
    assert telemetry.history == ()


def _private_parent(tmp_path: Path) -> Path:
    parent = tmp_path / "telemetry-parent"
    parent.mkdir(mode=0o700)
    return parent


def test_worker_telemetry_writer_commits_private_atomic_file(tmp_path: Path) -> None:
    parent = _private_parent(tmp_path)
    target = parent / "worker-telemetry.json"
    now = datetime.now(UTC).replace(microsecond=123_000)
    projection = parse_scout_telemetry(_response(now), now=now).projection("idle")

    validate_telemetry_destination(target)
    write_worker_telemetry(target, projection, watermark=now)
    first_inode = target.stat().st_ino
    updated = dict(projection)
    updated["state"] = "offline"
    write_worker_telemetry(target, updated, watermark=now)

    metadata = target.lstat()
    assert stat.S_ISREG(metadata.st_mode)
    assert stat.S_IMODE(metadata.st_mode) == 0o600
    assert metadata.st_nlink == 1
    assert metadata.st_ino != first_inode
    assert json.loads(target.read_text(encoding="utf-8"))["state"] == "offline"
    assert list(parent.glob(".atlas-research-telemetry-*.tmp")) == []


def test_worker_telemetry_writer_uses_exclusive_nofollow_temp_and_fsyncs_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parent = _private_parent(tmp_path)
    target = parent / "worker-telemetry.json"
    opened: list[int] = []
    synced: list[str] = []
    replaced: list[tuple[int | None, int | None]] = []
    real_open = telemetry_module.os.open
    real_fsync = telemetry_module.os.fsync
    real_replace = telemetry_module.os.replace

    def observed_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        if flags & os.O_CREAT:
            opened.append(flags)
        return real_open(path, flags, *args, **kwargs)

    def observed_fsync(descriptor: int) -> None:
        mode = os.fstat(descriptor).st_mode
        synced.append("directory" if stat.S_ISDIR(mode) else "file")
        real_fsync(descriptor)

    def observed_replace(
        source: object,
        destination: object,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        replaced.append((src_dir_fd, dst_dir_fd))
        real_replace(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )

    monkeypatch.setattr(telemetry_module.os, "open", observed_open)
    supported_dir_fd = set(os.supports_dir_fd)
    supported_dir_fd.add(observed_open)
    monkeypatch.setattr(telemetry_module.os, "supports_dir_fd", supported_dir_fd)
    monkeypatch.setattr(telemetry_module.os, "fsync", observed_fsync)
    monkeypatch.setattr(telemetry_module.os, "replace", observed_replace)

    write_worker_telemetry(target, {"schema_version": 1}, watermark=datetime.now(UTC))

    assert len(opened) == 1
    assert opened[0] & os.O_EXCL
    assert opened[0] & os.O_NOFOLLOW
    assert synced == ["file", "directory"]
    assert len(replaced) == 1
    assert replaced[0][0] == replaced[0][1]
    assert replaced[0][0] is not None


@pytest.mark.parametrize("target_kind", ["symlink", "hardlink", "public_file", "oversized"])
def test_worker_telemetry_writer_rejects_unsafe_existing_target(
    tmp_path: Path, target_kind: str
) -> None:
    parent = _private_parent(tmp_path)
    target = parent / "worker-telemetry.json"
    sentinel = tmp_path / "sentinel"
    sentinel.write_text("preserve", encoding="utf-8")
    sentinel.chmod(0o600)
    if target_kind == "symlink":
        target.symlink_to(sentinel)
    elif target_kind == "hardlink":
        os.link(sentinel, target)
    elif target_kind == "public_file":
        target.write_text("preserve", encoding="utf-8")
        target.chmod(0o644)
    else:
        target.write_bytes(b"x" * ((256 << 10) + 1))
        target.chmod(0o600)

    with pytest.raises(ValidationError, match="unsafe"):
        write_worker_telemetry(target, {"schema_version": 1}, watermark=datetime.now(UTC))

    assert sentinel.read_text(encoding="utf-8") == "preserve"


def test_worker_telemetry_writer_rejects_unsafe_or_linked_parent(tmp_path: Path) -> None:
    public_parent = tmp_path / "public"
    public_parent.mkdir(mode=0o755)
    with pytest.raises(ValidationError, match="unsafe"):
        validate_telemetry_destination(public_parent / "worker.json")

    private_parent = _private_parent(tmp_path)
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(private_parent, target_is_directory=True)
    with pytest.raises(ValidationError, match="safely"):
        validate_telemetry_destination(linked_parent / "worker.json")


@pytest.mark.parametrize("fault", ["directory_fsync", "post_commit_stat"])
def test_worker_telemetry_writer_marks_post_rename_failures_ambiguous(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    parent = _private_parent(tmp_path)
    target = parent / "worker-telemetry.json"
    watermark = datetime.now(UTC).replace(microsecond=123_000)
    projection = {"schema_version": 1, "updated_at": _timestamp(watermark)}
    real_fsync = telemetry_module.os.fsync
    real_replace = telemetry_module.os.replace
    real_stat = telemetry_module.os.stat
    renamed = False

    def observed_replace(
        source: object,
        destination: object,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        nonlocal renamed
        real_replace(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )
        renamed = True

    def faulting_fsync(descriptor: int) -> None:
        if fault == "directory_fsync" and stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise OSError(errno.EIO, "fixture directory fsync failure")
        real_fsync(descriptor)

    def faulting_stat(path: object, *args: object, **kwargs: object) -> os.stat_result:
        if (
            fault == "post_commit_stat"
            and renamed
            and path == target.name
            and kwargs.get("dir_fd") is not None
        ):
            raise OSError(errno.EIO, "fixture post-commit stat failure")
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(telemetry_module.os, "replace", observed_replace)
    monkeypatch.setattr(telemetry_module.os, "fsync", faulting_fsync)
    monkeypatch.setattr(telemetry_module.os, "stat", faulting_stat)
    supported_dir_fd = set(os.supports_dir_fd)
    supported_dir_fd.add(faulting_stat)
    monkeypatch.setattr(telemetry_module.os, "supports_dir_fd", supported_dir_fd)
    supported_follow = set(os.supports_follow_symlinks)
    supported_follow.add(faulting_stat)
    monkeypatch.setattr(telemetry_module.os, "supports_follow_symlinks", supported_follow)

    with pytest.raises(TelemetryCommitAmbiguousError) as captured:
        write_worker_telemetry(target, projection, watermark=watermark)

    assert captured.value.watermark == watermark
    assert captured.value.code == "WORKER_TELEMETRY_COMMIT_AMBIGUOUS"
    assert json.loads(target.read_text(encoding="utf-8")) == projection


def test_history_limit_matches_neolab_agent_contract() -> None:
    assert MAX_TELEMETRY_HISTORY == 120

# SPDX-License-Identifier: MIT
"""Crash-safe, append-only experiment receipt storage."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final, cast

from .artifacts import ArtifactRef, atomic_write_private
from .canonical import canonical_json_bytes
from .constants import MAX_JOB_BYTES, SCHEMA_VERSION
from .errors import AtlasResearchError
from .job import parse_timestamp

__all__ = [
    "ReceiptChainError",
    "ReceiptCommit",
    "ReceiptConflictError",
    "ReceiptError",
    "ReceiptLog",
    "ReceiptValidationError",
    "ReceiptVerification",
    "canonical_result_sha256",
    "validate_receipt_document",
]

_ENTRY_RE: Final = re.compile(
    r"^(?P<sequence>[0-9]{16})-(?P<receipt>[a-z0-9][a-z0-9._-]{0,127})\.json$"
)
_HEX_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_IDEMPOTENCY_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}$")
_IDENTIFIER_RE: Final = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_CODE_RE: Final = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_COMMIT_RE: Final = re.compile(r"^[0-9a-f]{40}$")
_DECIMAL_RE: Final = re.compile(
    r"^(?:0|-?(?:[1-9][0-9]*(?:\.[0-9]{0,11}[1-9])?|0\.[0-9]{0,11}[1-9]))$"
)
_METRICS: Final = (
    "mae",
    "spearman",
    "pairwise_accuracy",
    "ndcg_at_10",
    "ndcg_at_50",
    "f1",
    "calibration_error",
)
_RECEIPT_FIELDS: Final = frozenset(
    {
        "schema_version",
        "receipt_id",
        "previous_receipt_sha256",
        "created_at",
        "started_at",
        "finished_at",
        "experiment_id",
        "job_id",
        "attempt",
        "idempotency_key",
        "job_spec_sha256",
        "canonical_result_sha256",
        "dataset_manifest",
        "benchmark_manifest",
        "baseline_evaluation_payload",
        "candidate",
        "evaluation_split",
        "canonical_result",
        "resource_usage",
        "provenance",
    }
)


class ReceiptError(AtlasResearchError):
    """Base class for receipt failures with a non-sensitive error code."""


class ReceiptValidationError(ReceiptError):
    """The proposed or stored receipt is invalid."""


class ReceiptConflictError(ReceiptError):
    """An idempotency key was reused for different bytes or job identity."""


class ReceiptChainError(ReceiptError):
    """The append-only chain or its head is inconsistent."""


@dataclass(frozen=True, slots=True)
class ReceiptCommit:
    """The exact committed receipt bytes and their storage identity."""

    path: Path
    data: bytes
    sha256: str
    replayed: bool


@dataclass(frozen=True, slots=True)
class ReceiptVerification:
    """A verified chain snapshot."""

    entry_count: int
    head_sha256: str | None
    recovered: bool


@dataclass(frozen=True, slots=True)
class _StoredReceipt:
    sequence: int
    path: Path
    data: bytes
    sha256: str
    document: dict[str, object]


class ReceiptLog:
    """A private directory containing an ordered, hash-chained receipt log.

    Callers provide the complete receipt, including the expected
    ``previous_receipt_sha256``. A replay is accepted only when its canonical
    bytes are identical to the originally committed bytes.
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.entries_dir = self.root / "entries"
        self._lock_path = self.root / ".lock"
        self._head_path = self.root / "HEAD"
        _ensure_private_directory(self.root)
        _ensure_private_directory(self.entries_dir)

    @property
    def head_sha256(self) -> str | None:
        """Return the verified current head digest."""

        return self.verify().head_sha256

    def commit(
        self,
        receipt: Mapping[str, object],
        *,
        not_after: datetime | None = None,
    ) -> ReceiptCommit:
        """Commit one canonical receipt or return an exact prior replay."""

        proposed = dict(receipt)
        _validate_receipt_document(proposed)
        data = canonical_json_bytes(proposed) + b"\n"
        if len(data) > MAX_JOB_BYTES:
            raise ReceiptValidationError("RECEIPT_TOO_LARGE", "receipt exceeds the size limit")

        with self._locked():
            entries = self._scan_entries()
            self._require_matching_head(entries)

            idempotency_key = cast(str, proposed["idempotency_key"])
            job_digest = cast(str, proposed["job_spec_sha256"])
            for stored in entries:
                if stored.document.get("idempotency_key") != idempotency_key:
                    continue
                if stored.document.get("job_spec_sha256") != job_digest:
                    raise ReceiptConflictError(
                        "RECEIPT_JOB_CONFLICT",
                        "idempotency key is already bound to another job digest",
                    )
                if stored.data != data:
                    raise ReceiptConflictError(
                        "RECEIPT_REPLAY_CONFLICT",
                        "idempotency key is already bound to different receipt bytes",
                    )
                return ReceiptCommit(stored.path, stored.data, stored.sha256, replayed=True)

            previous = entries[-1].sha256 if entries else None
            if proposed["previous_receipt_sha256"] != previous:
                raise ReceiptChainError(
                    "RECEIPT_STALE_HEAD",
                    "receipt does not extend the current chain head",
                )
            if not_after is not None:
                if not_after.tzinfo is None or not_after.utcoffset() != UTC.utcoffset(not_after):
                    raise ReceiptValidationError(
                        "RECEIPT_DEADLINE_INVALID", "receipt deadline must be UTC"
                    )
                if datetime.now(UTC) > not_after:
                    raise ReceiptValidationError(
                        "RECEIPT_DEADLINE_EXPIRED", "receipt deadline has expired"
                    )

            sequence = len(entries) + 1
            receipt_id = cast(str, proposed["receipt_id"])
            path = self.entries_dir / f"{sequence:016d}-{receipt_id}.json"
            digest = hashlib.sha256(data).hexdigest()
            _exclusive_write(path, data)
            _fsync_directory(self.entries_dir)
            try:
                self._write_head(sequence, path.name, digest)
            except Exception:
                # The durable entry is authoritative. Verification with recovery
                # can rebuild HEAD without rewriting any receipt.
                raise ReceiptError(
                    "RECEIPT_HEAD_WRITE_FAILED",
                    "receipt entry committed but chain head update failed",
                ) from None
            return ReceiptCommit(path, data, digest, replayed=False)

    def find(self, idempotency_key: str, job_spec_sha256: str) -> ReceiptCommit | None:
        """Find an exact job binding without requiring receipt reconstruction."""

        if (
            not isinstance(idempotency_key, str)
            or _IDEMPOTENCY_RE.fullmatch(idempotency_key) is None
        ):
            raise ReceiptValidationError("RECEIPT_INVALID", "idempotency key is invalid")
        if not isinstance(job_spec_sha256, str) or _HEX_RE.fullmatch(job_spec_sha256) is None:
            raise ReceiptValidationError("RECEIPT_INVALID", "job digest is invalid")
        with self._locked():
            entries = self._scan_entries()
            self._require_matching_head(entries)
            for stored in entries:
                if stored.document.get("idempotency_key") != idempotency_key:
                    continue
                if stored.document.get("job_spec_sha256") != job_spec_sha256:
                    raise ReceiptConflictError(
                        "RECEIPT_JOB_CONFLICT",
                        "idempotency key is already bound to another job digest",
                    )
                return ReceiptCommit(
                    stored.path,
                    stored.data,
                    stored.sha256,
                    replayed=True,
                )
            return None

    def verify(self, *, recover: bool = False) -> ReceiptVerification:
        """Verify every entry and optionally rebuild only the mutable HEAD file."""

        with self._locked():
            entries = self._scan_entries()
            try:
                self._require_matching_head(entries)
            except ReceiptChainError:
                if not recover:
                    raise
                self._recover_head(entries)
                return ReceiptVerification(
                    entry_count=len(entries),
                    head_sha256=entries[-1].sha256 if entries else None,
                    recovered=True,
                )
            return ReceiptVerification(
                entry_count=len(entries),
                head_sha256=entries[-1].sha256 if entries else None,
                recovered=False,
            )

    def verified_receipts(self) -> tuple[Mapping[str, object], ...]:
        """Return one lock-consistent, fully validated chain snapshot."""

        with self._locked():
            entries = self._scan_entries()
            self._require_matching_head(entries)
            return tuple(dict(entry.document) for entry in entries)

    @contextmanager
    def _locked(self) -> Iterator[None]:
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(self._lock_path, flags, 0o600)
        except OSError as exc:
            raise ReceiptError("RECEIPT_LOCK_FAILED", "cannot open receipt lock") from exc
        try:
            _require_private_regular_fd(fd, "RECEIPT_UNSAFE_LOCK")
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def _scan_entries(self) -> list[_StoredReceipt]:
        try:
            names = sorted(os.listdir(self.entries_dir))
        except OSError as exc:
            raise ReceiptError("RECEIPT_READ_FAILED", "cannot enumerate receipt entries") from exc

        entries: list[_StoredReceipt] = []
        seen_ids: set[str] = set()
        seen_keys: set[str] = set()
        previous: str | None = None
        for expected_sequence, name in enumerate(names, start=1):
            match = _ENTRY_RE.fullmatch(name)
            if match is None or int(match.group("sequence")) != expected_sequence:
                raise ReceiptChainError(
                    "RECEIPT_ENTRY_ORDER_INVALID",
                    "receipt entry sequence is not contiguous",
                )
            path = self.entries_dir / name
            data = _read_private_regular(path, MAX_JOB_BYTES)
            document = _decode_document(data)
            _validate_receipt_document(document)
            if canonical_json_bytes(document) + b"\n" != data:
                raise ReceiptChainError(
                    "RECEIPT_ENTRY_NOT_CANONICAL",
                    "stored receipt is not canonical JSON",
                )
            if document["previous_receipt_sha256"] != previous:
                raise ReceiptChainError(
                    "RECEIPT_CHAIN_BROKEN",
                    "stored receipt does not extend its predecessor",
                )
            receipt_id = cast(str, document["receipt_id"])
            idempotency_key = cast(str, document["idempotency_key"])
            if receipt_id in seen_ids or idempotency_key in seen_keys:
                raise ReceiptChainError(
                    "RECEIPT_DUPLICATE_IDENTITY",
                    "receipt identity is duplicated in the chain",
                )
            if receipt_id != match.group("receipt"):
                raise ReceiptChainError(
                    "RECEIPT_FILENAME_MISMATCH",
                    "receipt filename does not match its identity",
                )
            digest = hashlib.sha256(data).hexdigest()
            entries.append(_StoredReceipt(expected_sequence, path, data, digest, document))
            seen_ids.add(receipt_id)
            seen_keys.add(idempotency_key)
            previous = digest
        return entries

    def _require_matching_head(self, entries: list[_StoredReceipt]) -> None:
        if not entries:
            if self._head_path.exists() or self._head_path.is_symlink():
                raise ReceiptChainError(
                    "RECEIPT_HEAD_MISMATCH",
                    "chain head exists for an empty receipt log",
                )
            return
        try:
            data = _read_private_regular(self._head_path, 4096)
        except ReceiptError as exc:
            raise ReceiptChainError(
                "RECEIPT_HEAD_MISSING",
                "chain head is missing or unreadable",
            ) from exc
        head = _decode_document(data)
        last = entries[-1]
        expected: dict[str, object] = {
            "filename": last.path.name,
            "sequence": last.sequence,
            "sha256": last.sha256,
        }
        if head != expected or canonical_json_bytes(head) + b"\n" != data:
            raise ReceiptChainError(
                "RECEIPT_HEAD_MISMATCH",
                "chain head does not match the durable entries",
            )

    def _write_head(self, sequence: int, filename: str, digest: str) -> None:
        document: dict[str, object] = {
            "filename": filename,
            "sequence": sequence,
            "sha256": digest,
        }
        data = canonical_json_bytes(document) + b"\n"
        temporary = self.root / f"head-{uuid.uuid4().hex}.tmp"
        try:
            _exclusive_write(temporary, data)
            os.replace(temporary, self._head_path)
            _fsync_directory(self.root)
        finally:
            with suppress(FileNotFoundError):
                temporary.unlink()

    def _recover_head(self, entries: list[_StoredReceipt]) -> None:
        if entries:
            last = entries[-1]
            self._write_head(last.sequence, last.path.name, last.sha256)
            return
        try:
            self._head_path.unlink()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise ReceiptError("RECEIPT_HEAD_RECOVERY_FAILED", "cannot recover chain head") from exc
        _fsync_directory(self.root)


def canonical_result_sha256(canonical_result: object) -> str:
    """Hash the restricted, number-free canonical result subtree."""

    _validate_number_free(canonical_result)
    try:
        canonical = canonical_json_bytes(canonical_result)
    except AtlasResearchError as exc:
        raise ReceiptValidationError(
            "RECEIPT_RESULT_INVALID", "canonical result is not valid canonical JSON"
        ) from exc
    return hashlib.sha256(canonical).hexdigest()


def _receipt_mapping(value: object, *, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ReceiptValidationError("RECEIPT_INVALID", f"{field} is invalid")
    return cast(Mapping[str, object], value)


def _receipt_string(
    value: Mapping[str, object], field: str, *, minimum: int = 1, maximum: int
) -> str:
    item = value.get(field)
    if not isinstance(item, str) or not minimum <= len(item.encode("utf-8")) <= maximum:
        raise ReceiptValidationError("RECEIPT_INVALID", f"{field} is invalid")
    return item


def _receipt_integer(value: Mapping[str, object], field: str, *, maximum: int | None = None) -> int:
    item = value.get(field)
    if isinstance(item, bool) or not isinstance(item, int) or item < 0:
        raise ReceiptValidationError("RECEIPT_INVALID", f"{field} is invalid")
    if maximum is not None and item > maximum:
        raise ReceiptValidationError("RECEIPT_INVALID", f"{field} is invalid")
    return item


def _receipt_artifact(document: Mapping[str, object], field: str, *, role: str) -> ArtifactRef:
    try:
        reference = ArtifactRef.from_mapping(_receipt_mapping(document.get(field), field=field))
    except AtlasResearchError as exc:
        raise ReceiptValidationError("RECEIPT_INVALID", f"{field} is invalid") from exc
    if reference.role != role:
        raise ReceiptValidationError("RECEIPT_INVALID", f"{field} role is invalid")
    return reference


def _validate_canonical_result(value: object) -> None:
    result = _receipt_mapping(value, field="canonical_result")
    required = {"metrics", "all_gates_passed", "decision", "reason_codes"}
    optional = {"error"}
    if not required.issubset(result) or not set(result).issubset(required | optional):
        raise ReceiptValidationError(
            "RECEIPT_RESULT_INVALID", "canonical result fields are invalid"
        )
    metrics = _receipt_mapping(result.get("metrics"), field="canonical_result.metrics")
    if not set(metrics).issubset(_METRICS) or len(metrics) > len(_METRICS):
        raise ReceiptValidationError("RECEIPT_RESULT_INVALID", "canonical metrics are invalid")
    failed: list[str] = []
    for name in _METRICS:
        if name not in metrics:
            continue
        metric = _receipt_mapping(metrics[name], field=f"canonical_result.metrics.{name}")
        if set(metric) != {"baseline", "candidate", "candidate_minus_baseline", "passed"}:
            raise ReceiptValidationError("RECEIPT_RESULT_INVALID", "canonical metric is invalid")
        numbers: list[Decimal] = []
        for field in ("baseline", "candidate", "candidate_minus_baseline"):
            raw = metric.get(field)
            if not isinstance(raw, str) or len(raw) > 32 or _DECIMAL_RE.fullmatch(raw) is None:
                raise ReceiptValidationError(
                    "RECEIPT_RESULT_INVALID", "canonical decimal is invalid"
                )
            try:
                numbers.append(Decimal(raw))
            except InvalidOperation as exc:  # pragma: no cover - guarded by the regex
                raise ReceiptValidationError(
                    "RECEIPT_RESULT_INVALID", "canonical decimal is invalid"
                ) from exc
        if numbers[1] - numbers[0] != numbers[2]:
            raise ReceiptValidationError("RECEIPT_RESULT_INVALID", "canonical delta is invalid")
        passed = metric.get("passed")
        if not isinstance(passed, bool):
            raise ReceiptValidationError("RECEIPT_RESULT_INVALID", "canonical gate is invalid")
        if not passed:
            failed.append(f"{name.upper()}_GATE_FAILED")

    all_gates = result.get("all_gates_passed")
    decision = result.get("decision")
    reasons = result.get("reason_codes")
    if not isinstance(all_gates, bool) or decision not in {"KEEP", "DISCARD", "ERROR"}:
        raise ReceiptValidationError("RECEIPT_RESULT_INVALID", "canonical decision is invalid")
    if (
        not isinstance(reasons, list)
        or not 1 <= len(reasons) <= 32
        or any(not isinstance(item, str) or _CODE_RE.fullmatch(item) is None for item in reasons)
        or len(set(cast(list[str], reasons))) != len(reasons)
    ):
        raise ReceiptValidationError("RECEIPT_RESULT_INVALID", "canonical reasons are invalid")
    if decision == "KEEP":
        if not metrics or failed or all_gates is not True or reasons != ["ALL_GATES_PASSED"]:
            raise ReceiptValidationError(
                "RECEIPT_RESULT_INVALID", "KEEP gate semantics are invalid"
            )
        if "error" in result:
            raise ReceiptValidationError("RECEIPT_RESULT_INVALID", "KEEP cannot contain an error")
    elif decision == "DISCARD":
        if not metrics or not failed or all_gates is not False or reasons != failed:
            raise ReceiptValidationError(
                "RECEIPT_RESULT_INVALID", "DISCARD gate semantics are invalid"
            )
        if "error" in result:
            raise ReceiptValidationError(
                "RECEIPT_RESULT_INVALID", "DISCARD cannot contain an error"
            )
    else:
        error = _receipt_mapping(result.get("error"), field="canonical_result.error")
        if set(error) != {"code", "message"} or metrics or all_gates is not False:
            raise ReceiptValidationError("RECEIPT_RESULT_INVALID", "ERROR semantics are invalid")
        code = error.get("code")
        message = error.get("message")
        if (
            not isinstance(code, str)
            or _CODE_RE.fullmatch(code) is None
            or not isinstance(message, str)
            or not 1 <= len(message.encode("utf-8")) <= 512
        ):
            raise ReceiptValidationError("RECEIPT_RESULT_INVALID", "canonical error is invalid")


def validate_receipt_document(document: Mapping[str, object]) -> None:
    """Validate the closed public receipt contract and its local semantics."""

    if set(document) != _RECEIPT_FIELDS or document.get("schema_version") != SCHEMA_VERSION:
        raise ReceiptValidationError("RECEIPT_INVALID", "receipt fields are invalid")
    receipt_id = document["receipt_id"]
    key = document["idempotency_key"]
    job_digest = document["job_spec_sha256"]
    result_digest = document["canonical_result_sha256"]
    previous = document["previous_receipt_sha256"]
    filename = f"0000000000000001-{receipt_id}.json"
    if not isinstance(receipt_id, str) or _ENTRY_RE.fullmatch(filename) is None:
        raise ReceiptValidationError("RECEIPT_INVALID", "receipt identity is invalid")
    if not isinstance(key, str) or _IDEMPOTENCY_RE.fullmatch(key) is None:
        raise ReceiptValidationError("RECEIPT_INVALID", "idempotency key is invalid")
    if not isinstance(job_digest, str) or _HEX_RE.fullmatch(job_digest) is None:
        raise ReceiptValidationError("RECEIPT_INVALID", "job digest is invalid")
    if not isinstance(result_digest, str) or _HEX_RE.fullmatch(result_digest) is None:
        raise ReceiptValidationError("RECEIPT_INVALID", "canonical result digest is invalid")
    if previous is not None and (
        not isinstance(previous, str) or _HEX_RE.fullmatch(previous) is None
    ):
        raise ReceiptValidationError("RECEIPT_INVALID", "previous receipt digest is invalid")
    for field in ("experiment_id", "job_id"):
        item = document.get(field)
        if not isinstance(item, str) or _IDENTIFIER_RE.fullmatch(item) is None:
            raise ReceiptValidationError("RECEIPT_INVALID", f"{field} is invalid")
    attempt = document.get("attempt")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or not 1 <= attempt <= 1000:
        raise ReceiptValidationError("RECEIPT_INVALID", "attempt is invalid")
    try:
        started_at = parse_timestamp(document.get("started_at"), field="started_at")
        finished_at = parse_timestamp(document.get("finished_at"), field="finished_at")
        created_at = parse_timestamp(document.get("created_at"), field="created_at")
    except AtlasResearchError as exc:
        raise ReceiptValidationError("RECEIPT_INVALID", "receipt timestamps are invalid") from exc
    if started_at > finished_at or created_at != finished_at:
        raise ReceiptValidationError("RECEIPT_INVALID", "receipt timestamp order is invalid")
    _receipt_artifact(document, "dataset_manifest", role="dataset_manifest")
    _receipt_artifact(document, "benchmark_manifest", role="benchmark_manifest")
    _receipt_artifact(document, "baseline_evaluation_payload", role="evaluation_payload")
    _receipt_artifact(document, "candidate", role="candidate")
    if document.get("evaluation_split") not in {"validation", "test"}:
        raise ReceiptValidationError("RECEIPT_INVALID", "evaluation split is invalid")
    resource_usage = _receipt_mapping(document.get("resource_usage"), field="resource_usage")
    if set(resource_usage) != {"wall_milliseconds", "records_evaluated", "peak_rss_bytes"}:
        raise ReceiptValidationError("RECEIPT_INVALID", "resource usage fields are invalid")
    for field in resource_usage:
        _receipt_integer(resource_usage, field)
    provenance = _receipt_mapping(document.get("provenance"), field="provenance")
    required_provenance = {
        "atlas_research_version",
        "git_commit",
        "source_revision_kind",
        "python_version",
        "platform",
        "worker_id",
        "worker_session_id",
    }
    if not required_provenance.issubset(provenance) or not set(provenance).issubset(
        required_provenance | {"source_artifact_sha256"}
    ):
        raise ReceiptValidationError("RECEIPT_INVALID", "provenance fields are invalid")
    _receipt_string(provenance, "atlas_research_version", maximum=64)
    commit = _receipt_string(provenance, "git_commit", maximum=40)
    _receipt_string(provenance, "python_version", maximum=64)
    _receipt_string(provenance, "platform", maximum=256)
    worker_id = _receipt_string(provenance, "worker_id", maximum=128)
    session_id = _receipt_string(provenance, "worker_session_id", maximum=128)
    revision_kind = provenance.get("source_revision_kind")
    if _COMMIT_RE.fullmatch(commit) is None:
        raise ReceiptValidationError("RECEIPT_INVALID", "git commit is invalid")
    if _IDENTIFIER_RE.fullmatch(worker_id) is None or _IDENTIFIER_RE.fullmatch(session_id) is None:
        raise ReceiptValidationError("RECEIPT_INVALID", "worker provenance is invalid")
    source_digest = provenance.get("source_artifact_sha256")
    if revision_kind == "verified_checkout":
        if source_digest is not None:
            raise ReceiptValidationError("RECEIPT_INVALID", "checkout provenance is invalid")
    elif revision_kind == "declared_wheel_revision":
        if not isinstance(source_digest, str) or _HEX_RE.fullmatch(source_digest) is None:
            raise ReceiptValidationError("RECEIPT_INVALID", "wheel provenance is invalid")
    else:
        raise ReceiptValidationError("RECEIPT_INVALID", "source revision kind is invalid")
    _validate_canonical_result(document["canonical_result"])
    actual = canonical_result_sha256(document["canonical_result"])
    if actual != result_digest:
        raise ReceiptValidationError(
            "RECEIPT_RESULT_DIGEST_MISMATCH",
            "canonical result digest does not match its nested value",
        )


def _validate_receipt_document(document: Mapping[str, object]) -> None:
    validate_receipt_document(document)


def _validate_number_free(value: object, *, depth: int = 0) -> None:
    if depth > 32:
        raise ReceiptValidationError(
            "RECEIPT_RESULT_INVALID", "canonical result is too deeply nested"
        )
    if value is None or isinstance(value, (str, bool)):
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise ReceiptValidationError(
                    "RECEIPT_RESULT_INVALID", "canonical result keys must be strings"
                )
            _validate_number_free(child, depth=depth + 1)
        return
    if isinstance(value, (list, tuple)):
        for child in value:
            _validate_number_free(child, depth=depth + 1)
        return
    raise ReceiptValidationError(
        "RECEIPT_RESULT_NUMBER_FORBIDDEN",
        "canonical result must encode metric numbers as decimal strings",
    )


def _decode_document(data: bytes) -> dict[str, object]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ReceiptChainError("RECEIPT_JSON_INVALID", "stored JSON has duplicate keys")
            result[key] = value
        return result

    try:
        value = json.loads(
            data,
            object_pairs_hook=no_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("constant")),
        )
    except ReceiptChainError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ReceiptChainError("RECEIPT_JSON_INVALID", "stored receipt is invalid JSON") from exc
    if not isinstance(value, dict):
        raise ReceiptChainError("RECEIPT_JSON_INVALID", "stored receipt must be an object")
    return cast(dict[str, object], value)


def _ensure_private_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700, parents=False, exist_ok=True)
        metadata = path.lstat()
    except OSError as exc:
        raise ReceiptError("RECEIPT_STORAGE_FAILED", "cannot initialize receipt storage") from exc
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise ReceiptError("RECEIPT_UNSAFE_STORAGE", "receipt storage is not a real directory")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise ReceiptError("RECEIPT_UNSAFE_STORAGE", "receipt storage is not private")


def _exclusive_write(path: Path, data: bytes) -> None:
    try:
        committed = atomic_write_private(path.parent, path.name, data, max_bytes=MAX_JOB_BYTES)
    except AtlasResearchError as exc:
        raise ReceiptError("RECEIPT_WRITE_FAILED", "cannot persist receipt storage entry") from exc
    if committed != path:  # pragma: no cover - internal invariant
        raise ReceiptError("RECEIPT_WRITE_FAILED", "cannot persist receipt storage entry")


def _read_private_regular(path: Path, maximum: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise ReceiptError("RECEIPT_READ_FAILED", "cannot open receipt storage entry") from exc
    try:
        metadata = os.fstat(fd)
        _require_private_regular_fd(fd, "RECEIPT_UNSAFE_ENTRY")
        if metadata.st_size > maximum:
            raise ReceiptError("RECEIPT_TOO_LARGE", "receipt storage entry exceeds the size limit")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining > 0:
            chunk = os.read(fd, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) > maximum:
            raise ReceiptError("RECEIPT_TOO_LARGE", "receipt storage entry exceeds the size limit")
        after = os.fstat(fd)
        if (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise ReceiptError("RECEIPT_READ_RACE", "receipt storage entry changed while reading")
        return data
    finally:
        os.close(fd)


def _require_private_regular_fd(fd: int, code: str) -> None:
    metadata = os.fstat(fd)
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise ReceiptError(code, "receipt storage entry is not a private regular file")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise ReceiptError(code, "receipt storage entry permissions are not private")


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise ReceiptError("RECEIPT_FSYNC_FAILED", "cannot open receipt directory") from exc
    try:
        os.fsync(fd)
    except OSError as exc:
        raise ReceiptError("RECEIPT_FSYNC_FAILED", "cannot sync receipt directory") from exc
    finally:
        os.close(fd)

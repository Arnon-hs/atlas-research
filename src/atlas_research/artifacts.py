# SPDX-License-Identifier: MIT
"""Immutable artifact references, confined resolution, and private commits."""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets
import stat
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Final, TypeVar, cast

from . import __version__
from .canonical import JSONValue, canonical_json_bytes, strict_json_loads
from .constants import (
    ARTIFACT_REF_SCHEMA,
    BENCHMARK_MANIFEST_SCHEMA,
    CANDIDATE_SCHEMA,
    DATASET_MANIFEST_SCHEMA,
    MAX_ARTIFACT_BYTES,
    MAX_JSON_DEPTH,
    MAX_OUTPUT_BYTES,
    MAX_STRING_BYTES,
    PRODUCER_NAME,
    RECEIPT_SCHEMA,
    RESULT_SCHEMA,
    SCHEMA_VERSION,
    SCORING_EXAMPLE_SCHEMA,
)
from .errors import ConflictError, ResourceLimitError, ValidationError

T = TypeVar("T")

_ROLES: Final = frozenset(
    {
        "source_export",
        "dataset_split",
        "dataset_manifest",
        "benchmark_manifest",
        "evaluation_payload",
        "candidate",
        "experiment_receipt",
        "worker_result",
        "report",
        "opaque",
    }
)
_MEDIA_TYPE_RE: Final = re.compile(r"^[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+$")
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_PRODUCER_RE: Final = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_COMMIT_RE: Final = re.compile(r"^[0-9a-f]{40}$")
_VERSION_RE: Final = re.compile(r"^[0-9]+(?:\.[0-9]+){0,2}(?:[-+][0-9A-Za-z.-]+)?$")
_LOGICAL_NAME_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,254}$")
_LOCATOR_SEGMENT_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
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
_ROLE_MEDIA_SCHEMA: Final[dict[str, tuple[str, str]]] = {
    "dataset_manifest": (
        "application/vnd.atlas-research.dataset-manifest+json",
        DATASET_MANIFEST_SCHEMA,
    ),
    "benchmark_manifest": (
        "application/vnd.atlas-research.benchmark-manifest+json",
        BENCHMARK_MANIFEST_SCHEMA,
    ),
    "candidate": (
        "application/vnd.atlas-research.candidate+json",
        CANDIDATE_SCHEMA,
    ),
    "experiment_receipt": (
        "application/vnd.atlas-research.experiment-receipt+json",
        RECEIPT_SCHEMA,
    ),
    "worker_result": (
        "application/vnd.atlas-research.experiment-result+json",
        RESULT_SCHEMA,
    ),
}
_ATLAS_PRODUCED_ROLES: Final = frozenset(
    {
        "dataset_split",
        "dataset_manifest",
        "benchmark_manifest",
        "candidate",
        "experiment_receipt",
        "worker_result",
    }
)
_O_CLOEXEC: Final = getattr(os, "O_CLOEXEC", 0)
_O_NOFOLLOW: Final = getattr(os, "O_NOFOLLOW", 0)
_O_DIRECTORY: Final = getattr(os, "O_DIRECTORY", 0)
_O_NONBLOCK: Final = getattr(os, "O_NONBLOCK", 0)
_READ_CHUNK: Final = 1 << 20


def _exact_keys(
    value: Mapping[str, object],
    *,
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
    code: str,
) -> None:
    keys = frozenset(value.keys())
    if not required.issubset(keys) or not keys.issubset(required | optional):
        raise ValidationError(code, "Artifact metadata fields do not match the contract")


def _string(value: Mapping[str, object], key: str, code: str) -> str:
    result = value.get(key)
    if not isinstance(result, str):
        raise ValidationError(code, "Artifact metadata field has an invalid type")
    return result


def _optional_string(value: Mapping[str, object], key: str, code: str) -> str | None:
    result = value.get(key)
    if result is None:
        return None
    if not isinstance(result, str):
        raise ValidationError(code, "Artifact metadata field has an invalid type")
    return result


def _integer(value: Mapping[str, object], key: str, code: str) -> int:
    result = value.get(key)
    if isinstance(result, bool) or not isinstance(result, int):
        raise ValidationError(code, "Artifact metadata field has an invalid type")
    return result


def _mapping(value: Mapping[str, object], key: str, code: str) -> Mapping[str, object]:
    result = value.get(key)
    if not isinstance(result, Mapping):
        raise ValidationError(code, "Artifact metadata field has an invalid type")
    if any(not isinstance(item, str) for item in result):
        raise ValidationError(code, "Artifact metadata field has an invalid type")
    return cast(Mapping[str, object], result)


def _optional_mapping(
    value: Mapping[str, object], key: str, code: str
) -> Mapping[str, object] | None:
    result = value.get(key)
    if result is None:
        return None
    if not isinstance(result, Mapping) or any(not isinstance(item, str) for item in result):
        raise ValidationError(code, "Artifact metadata field has an invalid type")
    return cast(Mapping[str, object], result)


def _locator_parts(locator: str) -> tuple[str, ...]:
    if not locator or len(locator) > 2_048:
        raise ValidationError("ARTIFACT_URI_INVALID", "Artifact URI is not a bounded relative path")
    if any(marker in locator for marker in ("\\", "%", "?", "#", "\x00")):
        raise ValidationError("ARTIFACT_URI_INVALID", "Artifact URI is not a bounded relative path")
    if locator.startswith("/") or ":" in locator:
        raise ValidationError("ARTIFACT_URI_INVALID", "Artifact URI is not a bounded relative path")
    parts = tuple(locator.split("/"))
    if len(parts) > 32:
        raise ValidationError("ARTIFACT_URI_INVALID", "Artifact URI is not a bounded relative path")
    if any(
        part in {"", ".", ".."} or _LOCATOR_SEGMENT_RE.fullmatch(part) is None for part in parts
    ):
        raise ValidationError("ARTIFACT_URI_INVALID", "Artifact URI is not a bounded relative path")
    if any(part.lower().endswith(_ARCHIVE_SUFFIXES) for part in parts):
        raise ValidationError("ARTIFACT_ARCHIVE_REJECTED", "Archive artifacts are not accepted")
    return parts


@dataclass(frozen=True, slots=True)
class ProducerRef:
    """Producer identity embedded in an artifact reference."""

    name: str
    version: str
    commit: str | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> ProducerRef:
        _exact_keys(
            value,
            required=frozenset({"name", "version"}),
            optional=frozenset({"commit"}),
            code="ARTIFACT_PRODUCER_INVALID",
        )
        name = _string(value, "name", "ARTIFACT_PRODUCER_INVALID")
        version = _string(value, "version", "ARTIFACT_PRODUCER_INVALID")
        commit = _optional_string(value, "commit", "ARTIFACT_PRODUCER_INVALID")
        if _PRODUCER_RE.fullmatch(name) is None or not 1 <= len(version) <= 64:
            raise ValidationError("ARTIFACT_PRODUCER_INVALID", "Artifact producer is invalid")
        if commit is not None and _COMMIT_RE.fullmatch(commit) is None:
            raise ValidationError("ARTIFACT_PRODUCER_INVALID", "Artifact producer is invalid")
        return cls(name=name, version=version, commit=commit)

    def to_mapping(self) -> dict[str, object]:
        result: dict[str, object] = {"name": self.name, "version": self.version}
        if self.commit is not None:
            result["commit"] = self.commit
        return result


@dataclass(frozen=True, slots=True)
class ExternalSchemaRef:
    """Opaque external schema identity; it is never fetched from the network."""

    id: str
    version: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> ExternalSchemaRef:
        _exact_keys(
            value,
            required=frozenset({"id", "version"}),
            code="ARTIFACT_SCHEMA_INVALID",
        )
        schema_id = _string(value, "id", "ARTIFACT_SCHEMA_INVALID")
        version = _string(value, "version", "ARTIFACT_SCHEMA_INVALID")
        if not 1 <= len(schema_id) <= 512 or _VERSION_RE.fullmatch(version) is None:
            raise ValidationError("ARTIFACT_SCHEMA_INVALID", "Artifact schema identity is invalid")
        return cls(id=schema_id, version=version)

    def to_mapping(self) -> dict[str, object]:
        return {"id": self.id, "version": self.version}


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    """Validated immutable artifact reference v1."""

    uri: str
    role: str
    media_type: str
    sha256: str
    size_bytes: int
    producer: ProducerRef
    logical_name: str | None = None
    external_schema: ExternalSchemaRef | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> ArtifactRef:
        _exact_keys(
            value,
            required=frozenset({"uri", "role", "media_type", "sha256", "size_bytes", "producer"}),
            optional=frozenset({"logical_name", "external_schema"}),
            code="ARTIFACT_REF_INVALID",
        )
        artifact = cls(
            uri=_string(value, "uri", "ARTIFACT_REF_INVALID"),
            role=_string(value, "role", "ARTIFACT_REF_INVALID"),
            media_type=_string(value, "media_type", "ARTIFACT_REF_INVALID"),
            sha256=_string(value, "sha256", "ARTIFACT_REF_INVALID"),
            size_bytes=_integer(value, "size_bytes", "ARTIFACT_REF_INVALID"),
            producer=ProducerRef.from_mapping(
                _mapping(value, "producer", "ARTIFACT_PRODUCER_INVALID")
            ),
            logical_name=_optional_string(value, "logical_name", "ARTIFACT_REF_INVALID"),
            external_schema=(
                ExternalSchemaRef.from_mapping(schema)
                if (
                    schema := _optional_mapping(value, "external_schema", "ARTIFACT_SCHEMA_INVALID")
                )
                is not None
                else None
            ),
        )
        artifact.validate()
        return artifact

    def validate(self) -> None:
        if (
            not isinstance(self.uri, str)
            or not isinstance(self.role, str)
            or not isinstance(self.media_type, str)
            or not isinstance(self.sha256, str)
            or isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or not isinstance(self.producer, ProducerRef)
            or (self.logical_name is not None and not isinstance(self.logical_name, str))
            or (
                self.external_schema is not None
                and not isinstance(self.external_schema, ExternalSchemaRef)
            )
        ):
            raise ValidationError("ARTIFACT_REF_INVALID", "Artifact reference is invalid")
        ProducerRef.from_mapping(self.producer.to_mapping())
        if self.external_schema is not None:
            ExternalSchemaRef.from_mapping(self.external_schema.to_mapping())
        _locator_parts(self.uri)
        if self.role not in _ROLES:
            raise ValidationError("ARTIFACT_ROLE_INVALID", "Artifact role is not supported")
        if len(self.media_type) > 127 or _MEDIA_TYPE_RE.fullmatch(self.media_type) is None:
            raise ValidationError("ARTIFACT_MEDIA_TYPE_INVALID", "Artifact media type is invalid")
        if _SHA256_RE.fullmatch(self.sha256) is None:
            raise ValidationError("ARTIFACT_DIGEST_INVALID", "Artifact digest is invalid")
        if not 0 <= self.size_bytes <= MAX_ARTIFACT_BYTES:
            raise ValidationError("ARTIFACT_SIZE_INVALID", "Artifact size is outside the limit")
        if self.logical_name is not None and (
            _LOGICAL_NAME_RE.fullmatch(self.logical_name) is None
            or any(part in {"", ".", ".."} for part in self.logical_name.split("/"))
        ):
            raise ValidationError(
                "ARTIFACT_LOGICAL_NAME_INVALID", "Artifact logical name is invalid"
            )

        if self.role in _ATLAS_PRODUCED_ROLES and self.producer.name != PRODUCER_NAME:
            raise ValidationError(
                "ARTIFACT_PRODUCER_MISMATCH", "Artifact producer does not match role"
            )
        if self.role == "source_export":
            if self.media_type not in {"application/json", "application/x-ndjson"}:
                raise ValidationError(
                    "ARTIFACT_MEDIA_TYPE_MISMATCH", "Artifact media type does not match role"
                )
            if self.external_schema is None:
                raise ValidationError(
                    "ARTIFACT_SCHEMA_REQUIRED", "Artifact role requires a schema identity"
                )
        elif self.role == "dataset_split":
            self._require_role_contract(
                media_type="application/x-ndjson",
                schema_id=SCORING_EXAMPLE_SCHEMA,
            )
        elif self.role == "evaluation_payload":
            if self.media_type != "application/json":
                raise ValidationError(
                    "ARTIFACT_MEDIA_TYPE_MISMATCH", "Artifact media type does not match role"
                )
            if self.external_schema is None:
                raise ValidationError(
                    "ARTIFACT_SCHEMA_REQUIRED", "Artifact role requires a schema identity"
                )
        elif self.role in _ROLE_MEDIA_SCHEMA:
            media_type, schema_id = _ROLE_MEDIA_SCHEMA[self.role]
            self._require_role_contract(media_type=media_type, schema_id=schema_id)
        elif self.role == "report" and self.media_type != "text/html":
            raise ValidationError(
                "ARTIFACT_MEDIA_TYPE_MISMATCH", "Artifact media type does not match role"
            )

    def _require_role_contract(self, *, media_type: str, schema_id: str) -> None:
        if self.media_type != media_type:
            raise ValidationError(
                "ARTIFACT_MEDIA_TYPE_MISMATCH", "Artifact media type does not match role"
            )
        if self.external_schema is None:
            raise ValidationError(
                "ARTIFACT_SCHEMA_REQUIRED", "Artifact role requires a schema identity"
            )
        if self.external_schema.id != schema_id or self.external_schema.version != SCHEMA_VERSION:
            raise ValidationError("ARTIFACT_SCHEMA_MISMATCH", "Artifact schema does not match role")

    def to_mapping(self) -> dict[str, object]:
        result: dict[str, object] = {
            "uri": self.uri,
            "role": self.role,
            "media_type": self.media_type,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "producer": self.producer.to_mapping(),
        }
        if self.logical_name is not None:
            result["logical_name"] = self.logical_name
        if self.external_schema is not None:
            result["external_schema"] = self.external_schema.to_mapping()
        return result


@dataclass(frozen=True, slots=True)
class ArtifactExpectation:
    """Consumer-declared identity required before artifact bytes are opened."""

    role: str
    media_type: str | None = None
    producer_name: str | None = None
    producer_version: str | None = None
    external_schema_id: str | None = None
    external_schema_version: str | None = None

    def validate(self, reference: ArtifactRef) -> None:
        if reference.role != self.role:
            raise ValidationError(
                "ARTIFACT_ROLE_MISMATCH", "Artifact role does not match the consumer"
            )
        if self.media_type is not None and reference.media_type != self.media_type:
            raise ValidationError(
                "ARTIFACT_MEDIA_TYPE_MISMATCH", "Artifact media type does not match the consumer"
            )
        if self.producer_name is not None and reference.producer.name != self.producer_name:
            raise ValidationError(
                "ARTIFACT_PRODUCER_MISMATCH", "Artifact producer does not match the consumer"
            )
        if (
            self.producer_version is not None
            and reference.producer.version != self.producer_version
        ):
            raise ValidationError(
                "ARTIFACT_PRODUCER_MISMATCH", "Artifact producer does not match the consumer"
            )
        if self.external_schema_id is not None or self.external_schema_version is not None:
            if reference.external_schema is None:
                raise ValidationError(
                    "ARTIFACT_SCHEMA_MISMATCH", "Artifact schema does not match the consumer"
                )
            if (
                self.external_schema_id is not None
                and reference.external_schema.id != self.external_schema_id
            ) or (
                self.external_schema_version is not None
                and reference.external_schema.version != self.external_schema_version
            ):
                raise ValidationError(
                    "ARTIFACT_SCHEMA_MISMATCH", "Artifact schema does not match the consumer"
                )


@dataclass(frozen=True, slots=True)
class ResolvedArtifact:
    """Exact verified bytes and an optional strict JSON value."""

    reference: ArtifactRef
    data: bytes
    json_value: JSONValue | None = None


def _open_directory(path: Path) -> int:
    try:
        descriptor = os.open(path, os.O_RDONLY | _O_CLOEXEC | _O_NOFOLLOW | _O_DIRECTORY)
        metadata = os.fstat(descriptor)
    except OSError as exc:
        raise ValidationError(
            "ARTIFACT_ROOT_INVALID", "Artifact root cannot be opened safely"
        ) from exc
    if not stat.S_ISDIR(metadata.st_mode):
        os.close(descriptor)
        raise ValidationError("ARTIFACT_ROOT_INVALID", "Artifact root is not a directory")
    return descriptor


def _open_parent(root_descriptor: int, parts: tuple[str, ...]) -> int:
    current = os.dup(root_descriptor)
    try:
        for part in parts:
            next_descriptor = os.open(
                part,
                os.O_RDONLY | _O_CLOEXEC | _O_NOFOLLOW | _O_DIRECTORY,
                dir_fd=current,
            )
            metadata = os.fstat(next_descriptor)
            if not stat.S_ISDIR(metadata.st_mode):
                os.close(next_descriptor)
                raise ValidationError(
                    "ARTIFACT_PATH_INVALID", "Artifact path is not safely traversable"
                )
            os.close(current)
            current = next_descriptor
        return current
    except OSError as exc:
        os.close(current)
        raise ValidationError(
            "ARTIFACT_PATH_INVALID", "Artifact path is not safely traversable"
        ) from exc
    except Exception:
        os.close(current)
        raise


def _stable_identity(metadata: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


class ArtifactResolver:
    """Resolve immutable artifacts beneath one pinned directory descriptor."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = Path(root)
        self._root_descriptor = _open_directory(self.root)

    def close(self) -> None:
        if self._root_descriptor >= 0:
            os.close(self._root_descriptor)
            self._root_descriptor = -1

    def __enter__(self) -> ArtifactResolver:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

    def _read(
        self,
        reference: ArtifactRef,
        *,
        max_bytes: int,
        parser: Callable[[bytes], object] | None = None,
    ) -> tuple[bytes, object | None]:
        if self._root_descriptor < 0:
            raise RuntimeError("artifact resolver is closed")
        if max_bytes < 0:
            raise ValueError("max_bytes must be non-negative")
        effective_limit = min(max_bytes, MAX_ARTIFACT_BYTES)
        if reference.size_bytes > effective_limit:
            raise ResourceLimitError("ARTIFACT_BYTES_EXCEEDED", "Artifact exceeds the byte limit")
        parts = _locator_parts(reference.uri)
        parent_descriptor = _open_parent(self._root_descriptor, parts[:-1])
        file_descriptor = -1
        try:
            try:
                file_descriptor = os.open(
                    parts[-1],
                    os.O_RDONLY | _O_CLOEXEC | _O_NOFOLLOW | _O_NONBLOCK,
                    dir_fd=parent_descriptor,
                )
            except OSError as exc:
                raise ValidationError(
                    "ARTIFACT_OPEN_FAILED", "Artifact cannot be opened safely"
                ) from exc
            before = os.fstat(file_descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise ValidationError("ARTIFACT_NOT_REGULAR", "Artifact must be a regular file")
            if before.st_nlink != 1:
                raise ValidationError("ARTIFACT_LINK_REJECTED", "Linked artifacts are not accepted")
            if before.st_size != reference.size_bytes:
                raise ValidationError(
                    "ARTIFACT_SIZE_MISMATCH", "Artifact size does not match its reference"
                )
            if before.st_size > effective_limit:
                raise ResourceLimitError(
                    "ARTIFACT_BYTES_EXCEEDED", "Artifact exceeds the byte limit"
                )

            chunks: list[bytes] = []
            remaining = before.st_size
            while remaining:
                chunk = os.read(file_descriptor, min(_READ_CHUNK, remaining))
                if not chunk:
                    raise ValidationError("ARTIFACT_TRUNCATED", "Artifact changed while being read")
                chunks.append(chunk)
                remaining -= len(chunk)
            if os.read(file_descriptor, 1):
                raise ValidationError("ARTIFACT_GREW", "Artifact changed while being read")
            data = b"".join(chunks)
            digest = hashlib.sha256(data).hexdigest()
            if not hmac.compare_digest(digest, reference.sha256):
                raise ValidationError(
                    "ARTIFACT_DIGEST_MISMATCH", "Artifact digest does not match its reference"
                )
            parsed = parser(data) if parser is not None else None
            after = os.fstat(file_descriptor)
            if _stable_identity(before) != _stable_identity(after):
                raise ValidationError("ARTIFACT_CHANGED", "Artifact changed while being read")
            return data, parsed
        finally:
            if file_descriptor >= 0:
                os.close(file_descriptor)
            os.close(parent_descriptor)

    def resolve(
        self,
        reference: ArtifactRef | Mapping[str, object],
        expectation: ArtifactExpectation,
        *,
        parse_json: bool = False,
        max_bytes: int = MAX_ARTIFACT_BYTES,
        max_json_depth: int = MAX_JSON_DEPTH,
        max_string_bytes: int = MAX_STRING_BYTES,
    ) -> ResolvedArtifact:
        """Resolve, verify, and optionally strictly parse one artifact."""

        artifact_ref = (
            reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_mapping(reference)
        )
        artifact_ref.validate()
        expectation.validate(artifact_ref)
        if parse_json and not (
            artifact_ref.media_type == "application/json"
            or artifact_ref.media_type.endswith("+json")
        ):
            raise ValidationError(
                "ARTIFACT_MEDIA_TYPE_MISMATCH", "Artifact is not a JSON media type"
            )

        parser: Callable[[bytes], object] | None = None
        if parse_json:

            def parse_strict_json(data: bytes) -> JSONValue:
                return strict_json_loads(
                    data,
                    max_bytes=max_bytes,
                    max_depth=max_json_depth,
                    max_string_bytes=max_string_bytes,
                )

            parser = parse_strict_json
        data, parsed = self._read(artifact_ref, max_bytes=max_bytes, parser=parser)
        return ResolvedArtifact(
            reference=artifact_ref,
            data=data,
            json_value=cast(JSONValue | None, parsed),
        )

    def resolve_with_parser(
        self,
        reference: ArtifactRef | Mapping[str, object],
        expectation: ArtifactExpectation,
        parser: Callable[[bytes], T],
        *,
        max_bytes: int = MAX_ARTIFACT_BYTES,
    ) -> tuple[ResolvedArtifact, T]:
        """Run a bounded domain parser before the stable descriptor check."""

        artifact_ref = (
            reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_mapping(reference)
        )
        artifact_ref.validate()
        expectation.validate(artifact_ref)
        data, parsed = self._read(
            artifact_ref,
            max_bytes=max_bytes,
            parser=cast(Callable[[bytes], object], parser),
        )
        return ResolvedArtifact(reference=artifact_ref, data=data), cast(T, parsed)


def ensure_private_directory(path: str | os.PathLike[str]) -> Path:
    """Create or validate one operator-selected private workspace root."""

    result = Path(path)
    try:
        result.mkdir(mode=0o700, parents=False, exist_ok=True)
        metadata = result.lstat()
    except OSError as exc:
        raise ValidationError(
            "OUTPUT_ROOT_INVALID", "Output root cannot be created safely"
        ) from exc
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise ValidationError("OUTPUT_ROOT_INVALID", "Output root must be a real directory")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise ValidationError("OUTPUT_ROOT_NOT_PRIVATE", "Output root permissions must be private")
    return result


def atomic_write_private(
    root: str | os.PathLike[str],
    relative_path: str,
    data: bytes,
    *,
    overwrite: bool = False,
    max_bytes: int = MAX_OUTPUT_BYTES,
) -> Path:
    """Atomically commit private bytes beneath a directory descriptor."""

    if not isinstance(data, bytes):
        raise TypeError("data must be bytes")
    if max_bytes < 0:
        raise ValueError("max_bytes must be non-negative")
    if len(data) > min(max_bytes, MAX_OUTPUT_BYTES):
        raise ResourceLimitError("OUTPUT_BYTES_EXCEEDED", "Output exceeds the byte limit")
    root_path = ensure_private_directory(root)
    parts = _locator_parts(relative_path)
    root_descriptor = _open_directory(root_path)
    parent_descriptor = -1
    temporary_name = f".atlas-research-{secrets.token_hex(16)}.tmp"
    temporary_descriptor = -1
    temporary_exists = False
    try:
        parent_descriptor = _open_parent(root_descriptor, parts[:-1])
        try:
            temporary_descriptor = os.open(
                temporary_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_CLOEXEC | _O_NOFOLLOW,
                0o600,
                dir_fd=parent_descriptor,
            )
            temporary_exists = True
            view = memoryview(data)
            offset = 0
            while offset < len(view):
                written = os.write(temporary_descriptor, view[offset:])
                if written <= 0:
                    raise ValidationError(
                        "OUTPUT_WRITE_FAILED", "Output could not be committed safely"
                    )
                offset += written
            os.fchmod(temporary_descriptor, 0o600)
            os.fsync(temporary_descriptor)
        except OSError as exc:
            raise ValidationError(
                "OUTPUT_WRITE_FAILED", "Output could not be committed safely"
            ) from exc
        finally:
            if temporary_descriptor >= 0:
                os.close(temporary_descriptor)
                temporary_descriptor = -1

        if overwrite:
            try:
                os.replace(
                    temporary_name,
                    parts[-1],
                    src_dir_fd=parent_descriptor,
                    dst_dir_fd=parent_descriptor,
                )
                temporary_exists = False
            except OSError as exc:
                raise ValidationError(
                    "OUTPUT_COMMIT_FAILED", "Output could not be committed safely"
                ) from exc
        else:
            try:
                os.link(
                    temporary_name,
                    parts[-1],
                    src_dir_fd=parent_descriptor,
                    dst_dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
            except FileExistsError as exc:
                raise ConflictError("OUTPUT_EXISTS", "Immutable output already exists") from exc
            except OSError as exc:
                raise ValidationError(
                    "OUTPUT_COMMIT_FAILED", "Output could not be committed safely"
                ) from exc
            os.unlink(temporary_name, dir_fd=parent_descriptor)
            temporary_exists = False

        os.fsync(parent_descriptor)
        try:
            committed = os.stat(parts[-1], dir_fd=parent_descriptor, follow_symlinks=False)
        except OSError as exc:
            raise ValidationError(
                "OUTPUT_COMMIT_FAILED", "Output could not be committed safely"
            ) from exc
        if not stat.S_ISREG(committed.st_mode) or stat.S_IMODE(committed.st_mode) != 0o600:
            raise ValidationError("OUTPUT_COMMIT_FAILED", "Output could not be committed safely")
        if committed.st_nlink != 1:
            raise ValidationError("OUTPUT_COMMIT_FAILED", "Output could not be committed safely")
        return root_path.joinpath(*parts)
    finally:
        if temporary_descriptor >= 0:
            os.close(temporary_descriptor)
        if temporary_exists and parent_descriptor >= 0:
            with suppress(OSError):
                os.unlink(temporary_name, dir_fd=parent_descriptor)
        if parent_descriptor >= 0:
            os.close(parent_descriptor)
        os.close(root_descriptor)


def read_private_bytes(
    root: str | os.PathLike[str],
    relative_path: str,
    *,
    max_bytes: int = MAX_OUTPUT_BYTES,
) -> tuple[Path, bytes] | None:
    """Read one private regular file without following any path symlink."""

    if max_bytes < 0:
        raise ValueError("max_bytes must be non-negative")
    effective_limit = min(max_bytes, MAX_OUTPUT_BYTES)
    root_path = ensure_private_directory(root)
    parts = _locator_parts(relative_path)
    root_descriptor = _open_directory(root_path)
    parent_descriptor = -1
    file_descriptor = -1
    try:
        parent_descriptor = _open_parent(root_descriptor, parts[:-1])
        try:
            file_descriptor = os.open(
                parts[-1],
                os.O_RDONLY | _O_CLOEXEC | _O_NOFOLLOW | _O_NONBLOCK,
                dir_fd=parent_descriptor,
            )
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ValidationError(
                "OUTPUT_READ_FAILED", "Private output cannot be opened safely"
            ) from exc

        before = os.fstat(file_descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) & 0o077
            or before.st_size > effective_limit
        ):
            raise ValidationError(
                "OUTPUT_READ_FAILED", "Private output is not a bounded private regular file"
            )
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(file_descriptor, min(_READ_CHUNK, remaining))
            if not chunk:
                raise ValidationError(
                    "OUTPUT_READ_FAILED", "Private output changed while being read"
                )
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(file_descriptor, 1):
            raise ValidationError("OUTPUT_READ_FAILED", "Private output changed while being read")
        data = b"".join(chunks)
        after = os.fstat(file_descriptor)
        if _stable_identity(before) != _stable_identity(after):
            raise ValidationError("OUTPUT_READ_FAILED", "Private output changed while being read")
        return root_path.joinpath(*parts), data
    finally:
        if file_descriptor >= 0:
            os.close(file_descriptor)
        if parent_descriptor >= 0:
            os.close(parent_descriptor)
        os.close(root_descriptor)


def write_canonical_json_private(
    root: str | os.PathLike[str],
    relative_path: str,
    value: object,
    *,
    trailing_newline: bool = True,
    overwrite: bool = False,
) -> Path:
    """Commit canonical JSON with private permissions."""

    data = canonical_json_bytes(value) + (b"\n" if trailing_newline else b"")
    return atomic_write_private(root, relative_path, data, overwrite=overwrite)


def build_artifact_ref(
    *,
    uri: str,
    role: str,
    media_type: str,
    data: bytes,
    producer_name: str = PRODUCER_NAME,
    producer_version: str = __version__,
    producer_commit: str | None = None,
    logical_name: str | None = None,
    external_schema_id: str | None = None,
    external_schema_version: str | None = None,
) -> ArtifactRef:
    """Build and validate a digest-pinned reference for exact bytes."""

    if (external_schema_id is None) != (external_schema_version is None):
        raise ValidationError("ARTIFACT_SCHEMA_INVALID", "Artifact schema identity is incomplete")
    reference = ArtifactRef(
        uri=uri,
        role=role,
        media_type=media_type,
        sha256=hashlib.sha256(data).hexdigest(),
        size_bytes=len(data),
        producer=ProducerRef(
            name=producer_name,
            version=producer_version,
            commit=producer_commit,
        ),
        logical_name=logical_name,
        external_schema=(
            ExternalSchemaRef(id=external_schema_id, version=external_schema_version)
            if external_schema_id is not None and external_schema_version is not None
            else None
        ),
    )
    reference.validate()
    return reference


__all__ = [
    "ARTIFACT_REF_SCHEMA",
    "ArtifactExpectation",
    "ArtifactRef",
    "ArtifactResolver",
    "ExternalSchemaRef",
    "ProducerRef",
    "ResolvedArtifact",
    "atomic_write_private",
    "build_artifact_ref",
    "ensure_private_directory",
    "read_private_bytes",
    "write_canonical_json_private",
]

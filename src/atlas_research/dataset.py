# SPDX-License-Identifier: MIT
"""Strict scoring records and deterministic sha256-id-v1 dataset splits."""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from typing import Final, Literal, TypeAlias, cast

from .artifacts import ArtifactExpectation, ArtifactRef, ArtifactResolver
from .canonical import canonical_json_bytes, strict_json_loads
from .constants import (
    MAX_ARTIFACT_BYTES,
    MAX_FEATURES,
    MAX_JSON_DEPTH,
    MAX_JSONL_LINE_BYTES,
    MAX_OUTPUT_BYTES,
    MAX_RECORDS,
    MAX_STRING_BYTES,
    MAX_TOTAL_INPUT_BYTES,
    PRODUCER_NAME,
    SCHEMA_VERSION,
    SCORING_EXAMPLE_SCHEMA,
)
from .errors import ResourceLimitError, ValidationError

SplitName: TypeAlias = Literal["train", "validation", "test"]
RecordInput: TypeAlias = "ScoringRecord | Mapping[str, object]"

_ID_RE: Final = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,127}$", flags=re.ASCII)
_FEATURE_RE: Final = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$", flags=re.ASCII)
_DATASET_ID_RE: Final = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$", flags=re.ASCII)
_SPLIT_PREFIX: Final = b"atlas-research:split:v1"
_TRAIN_BOUNDARY: Final = 14_757_395_258_967_641_292
_VALIDATION_BOUNDARY: Final = 16_602_069_666_338_596_454
_FEATURE_MIN: Final = Decimal("-1000000")
_FEATURE_MAX: Final = Decimal("1000000")
_LABEL_MIN: Final = Decimal(0)
_LABEL_MAX: Final = Decimal(100)
_SPLIT_NAMES: Final[tuple[SplitName, ...]] = ("train", "validation", "test")


def _finite_decimal(value: object, *, code: str, message: str) -> Decimal:
    if isinstance(value, bool):
        raise ValidationError(code, message)
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError(code, message)
        result = Decimal(str(value))
    else:
        raise ValidationError(code, message)
    if not result.is_finite():
        raise ValidationError(code, message)
    return result


@dataclass(frozen=True, slots=True)
class ScoringRecord:
    """Normalized research-only scoring-example v1 record."""

    id: str
    features: tuple[tuple[str, Decimal], ...]
    label: Decimal

    def to_mapping(self) -> dict[str, object]:
        return {
            "id": self.id,
            "features": {name: value for name, value in self.features},
            "label": self.label,
        }


def parse_scoring_record(value: object) -> ScoringRecord:
    """Validate one exact scoring-example v1 object."""

    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ValidationError("RECORD_INVALID", "Scoring record must be a JSON object")
    record = cast(Mapping[str, object], value)
    if set(record) != {"id", "features", "label"}:
        raise ValidationError(
            "RECORD_FIELDS_INVALID", "Scoring record fields do not match the contract"
        )

    record_id = record["id"]
    if not isinstance(record_id, str) or _ID_RE.fullmatch(record_id) is None:
        raise ValidationError("RECORD_ID_INVALID", "Scoring record ID is invalid")

    raw_features = record["features"]
    if not isinstance(raw_features, Mapping) or any(
        not isinstance(key, str) for key in raw_features
    ):
        raise ValidationError("RECORD_FEATURES_INVALID", "Scoring record features are invalid")
    features = cast(Mapping[str, object], raw_features)
    if not 1 <= len(features) <= MAX_FEATURES:
        raise ValidationError("RECORD_FEATURES_INVALID", "Scoring record features are invalid")
    normalized_features: list[tuple[str, Decimal]] = []
    for name, raw_value in features.items():
        if _FEATURE_RE.fullmatch(name) is None:
            raise ValidationError("RECORD_FEATURE_NAME_INVALID", "Scoring feature name is invalid")
        feature_value = _finite_decimal(
            raw_value,
            code="RECORD_FEATURE_VALUE_INVALID",
            message="Scoring feature value is invalid",
        )
        if not _FEATURE_MIN <= feature_value <= _FEATURE_MAX:
            raise ValidationError(
                "RECORD_FEATURE_VALUE_INVALID", "Scoring feature value is invalid"
            )
        normalized_features.append((name, feature_value))

    label = _finite_decimal(
        record["label"],
        code="RECORD_LABEL_INVALID",
        message="Scoring label is invalid",
    )
    if not _LABEL_MIN <= label <= _LABEL_MAX:
        raise ValidationError("RECORD_LABEL_INVALID", "Scoring label is invalid")
    normalized_features.sort(key=lambda item: item[0].encode("ascii"))
    return ScoringRecord(id=record_id, features=tuple(normalized_features), label=label)


def _record_bytes(record: ScoringRecord) -> bytes:
    return canonical_json_bytes(record.to_mapping()) + b"\n"


def parse_scoring_jsonl(
    data: bytes,
    *,
    max_bytes: int = MAX_ARTIFACT_BYTES,
    max_line_bytes: int = MAX_JSONL_LINE_BYTES,
    max_records: int = MAX_RECORDS,
    max_json_depth: int = MAX_JSON_DEPTH,
    max_string_bytes: int = MAX_STRING_BYTES,
    allow_empty: bool = False,
) -> tuple[ScoringRecord, ...]:
    """Parse bounded UTF-8 JSONL and prove record IDs are unique."""

    if not isinstance(data, bytes):
        raise TypeError("data must be bytes")
    if min(max_bytes, max_line_bytes, max_records, max_json_depth, max_string_bytes) < 1:
        raise ValueError("JSONL limits must be positive")
    if len(data) > min(max_bytes, MAX_ARTIFACT_BYTES):
        raise ResourceLimitError("JSONL_BYTES_EXCEEDED", "JSONL input exceeds the byte limit")
    if not data:
        if allow_empty:
            return ()
        raise ValidationError("DATASET_EMPTY", "Dataset must contain at least one record")

    records: list[ScoringRecord] = []
    seen_ids: set[str] = set()
    offset = 0
    while offset < len(data):
        newline = data.find(b"\n", offset)
        if newline < 0:
            line = data[offset:]
            offset = len(data)
        else:
            line = data[offset:newline]
            offset = newline + 1
        if line.endswith(b"\r"):
            line = line[:-1]
        if not line:
            raise ValidationError("JSONL_EMPTY_LINE", "JSONL must not contain empty lines")
        if len(line) > min(max_line_bytes, MAX_JSONL_LINE_BYTES):
            raise ResourceLimitError("JSONL_LINE_TOO_LONG", "JSONL line exceeds the byte limit")
        if len(records) >= min(max_records, MAX_RECORDS):
            raise ResourceLimitError(
                "JSONL_RECORDS_EXCEEDED", "JSONL record count exceeds the limit"
            )
        parsed = strict_json_loads(
            line,
            max_bytes=min(max_line_bytes, MAX_JSONL_LINE_BYTES),
            max_depth=min(max_json_depth, MAX_JSON_DEPTH),
            max_string_bytes=min(max_string_bytes, MAX_STRING_BYTES),
        )
        record = parse_scoring_record(parsed)
        if record.id in seen_ids:
            raise ValidationError("RECORD_ID_DUPLICATE", "Scoring record IDs must be unique")
        seen_ids.add(record.id)
        records.append(record)
    if not records and not allow_empty:
        raise ValidationError("DATASET_EMPTY", "Dataset must contain at least one record")
    return tuple(records)


def _validate_seed(seed: int) -> None:
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed <= 0xFFFFFFFF:
        raise ValidationError(
            "DATASET_SEED_INVALID", "Dataset seed must be an unsigned 32-bit integer"
        )


def _validate_record_id(record_id: str) -> None:
    if _ID_RE.fullmatch(record_id) is None:
        raise ValidationError("RECORD_ID_INVALID", "Scoring record ID is invalid")


def _validate_utc_timestamp(value: object) -> None:
    if not isinstance(value, str) or not value.endswith("Z") or len(value) > 32:
        raise ValidationError("DATASET_TIMESTAMP_INVALID", "Dataset timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError as error:
        raise ValidationError(
            "DATASET_TIMESTAMP_INVALID", "Dataset timestamp is invalid"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValidationError("DATASET_TIMESTAMP_INVALID", "Dataset timestamp is invalid")


def split_for_id(record_id: str, seed: int) -> SplitName:
    """Map an ASCII record ID with the byte-exact sha256-id-v1 algorithm."""

    _validate_seed(seed)
    _validate_record_id(record_id)
    digest = hashlib.sha256(
        _SPLIT_PREFIX + b"\x00" + seed.to_bytes(4, "big") + b"\x00" + record_id.encode("ascii")
    ).digest()
    bucket = int.from_bytes(digest[:8], "big", signed=False)
    if bucket < _TRAIN_BOUNDARY:
        return "train"
    if bucket < _VALIDATION_BOUNDARY:
        return "validation"
    return "test"


@dataclass(frozen=True, slots=True)
class FrozenSplit:
    """Exact canonical JSONL bytes and their expected manifest metadata."""

    name: SplitName
    data: bytes
    sha256: str
    record_count: int
    sealed: bool


@dataclass(frozen=True, slots=True)
class FrozenDataset:
    """Three deterministic, pairwise-disjoint split artifacts."""

    seed: int
    train: FrozenSplit
    validation: FrozenSplit
    test: FrozenSplit
    source_record_count: int

    @property
    def splits(self) -> dict[SplitName, FrozenSplit]:
        return {
            "train": self.train,
            "validation": self.validation,
            "test": self.test,
        }


def _normalize_record(value: RecordInput) -> ScoringRecord:
    return parse_scoring_record(value.to_mapping() if isinstance(value, ScoringRecord) else value)


def freeze_records(
    records: Iterable[RecordInput],
    *,
    seed: int,
    max_records: int = MAX_RECORDS,
    max_output_bytes: int = MAX_OUTPUT_BYTES,
) -> FrozenDataset:
    """Freeze records into canonical train, validation, and sealed-test bytes."""

    _validate_seed(seed)
    if max_records < 1 or max_output_bytes < 1:
        raise ValueError("dataset limits must be positive")
    normalized: list[ScoringRecord] = []
    seen_ids: set[str] = set()
    for raw_record in records:
        if len(normalized) >= min(max_records, MAX_RECORDS):
            raise ResourceLimitError(
                "DATASET_RECORDS_EXCEEDED", "Dataset record count exceeds the limit"
            )
        record = _normalize_record(raw_record)
        if record.id in seen_ids:
            raise ValidationError("RECORD_ID_DUPLICATE", "Scoring record IDs must be unique")
        seen_ids.add(record.id)
        normalized.append(record)
    if not normalized:
        raise ValidationError("DATASET_EMPTY", "Dataset must contain at least one record")
    normalized.sort(key=lambda record: record.id.encode("ascii"))

    split_content: dict[SplitName, bytearray] = {
        "train": bytearray(),
        "validation": bytearray(),
        "test": bytearray(),
    }
    split_counts: dict[SplitName, int] = {"train": 0, "validation": 0, "test": 0}
    total_output_bytes = 0
    for record in normalized:
        row = _record_bytes(record)
        if len(row) - 1 > MAX_JSONL_LINE_BYTES:
            raise ResourceLimitError("JSONL_LINE_TOO_LONG", "JSONL line exceeds the byte limit")
        total_output_bytes += len(row)
        if total_output_bytes > min(max_output_bytes, MAX_OUTPUT_BYTES):
            raise ResourceLimitError("OUTPUT_BYTES_EXCEEDED", "Output exceeds the byte limit")
        name = split_for_id(record.id, seed)
        split_content[name].extend(row)
        split_counts[name] += 1

    frozen: dict[SplitName, FrozenSplit] = {}
    for name in _SPLIT_NAMES:
        content = bytes(split_content[name])
        frozen[name] = FrozenSplit(
            name=name,
            data=content,
            sha256=hashlib.sha256(content).hexdigest(),
            record_count=split_counts[name],
            sealed=name == "test",
        )
    dataset = FrozenDataset(
        seed=seed,
        train=frozen["train"],
        validation=frozen["validation"],
        test=frozen["test"],
        source_record_count=len(normalized),
    )
    verify_frozen_dataset(dataset)
    return dataset


def freeze_dataset(
    records: Iterable[RecordInput],
    *,
    seed: int,
    max_records: int = MAX_RECORDS,
    max_output_bytes: int = MAX_OUTPUT_BYTES,
) -> FrozenDataset:
    """Compatibility name for the public deterministic freeze operation."""

    return freeze_records(
        records,
        seed=seed,
        max_records=max_records,
        max_output_bytes=max_output_bytes,
    )


def freeze_jsonl_sources(
    sources: Iterable[bytes],
    *,
    seed: int,
    max_total_bytes: int = MAX_TOTAL_INPUT_BYTES,
    max_records: int = MAX_RECORDS,
    max_output_bytes: int = MAX_OUTPUT_BYTES,
) -> FrozenDataset:
    """Parse multiple source exports, enforcing global ID and byte bounds."""

    if min(max_total_bytes, max_records, max_output_bytes) < 1:
        raise ValueError("dataset limits must be positive")
    all_records: list[ScoringRecord] = []
    total_bytes = 0
    seen_ids: set[str] = set()
    for source in sources:
        total_bytes += len(source)
        if total_bytes > min(max_total_bytes, MAX_TOTAL_INPUT_BYTES):
            raise ResourceLimitError(
                "DATASET_BYTES_EXCEEDED", "Dataset inputs exceed the byte limit"
            )
        remaining = min(max_records, MAX_RECORDS) - len(all_records)
        if remaining < 1:
            raise ResourceLimitError(
                "DATASET_RECORDS_EXCEEDED", "Dataset record count exceeds the limit"
            )
        parsed = parse_scoring_jsonl(source, max_records=remaining)
        for record in parsed:
            if record.id in seen_ids:
                raise ValidationError("RECORD_ID_DUPLICATE", "Scoring record IDs must be unique")
            seen_ids.add(record.id)
            all_records.append(record)
    return freeze_records(
        all_records,
        seed=seed,
        max_records=max_records,
        max_output_bytes=max_output_bytes,
    )


def verify_frozen_dataset(dataset: FrozenDataset) -> None:
    """Recompute ordering, membership, counts, digests, sealing, and disjointness."""

    _validate_seed(dataset.seed)
    if (
        isinstance(dataset.source_record_count, bool)
        or not isinstance(dataset.source_record_count, int)
        or not 1 <= dataset.source_record_count <= MAX_RECORDS
    ):
        raise ValidationError("DATASET_COUNT_MISMATCH", "Dataset record counts do not match")

    all_ids: set[str] = set()
    total = 0
    total_bytes = 0
    for expected_name in _SPLIT_NAMES:
        split = dataset.splits[expected_name]
        if split.name != expected_name:
            raise ValidationError(
                "DATASET_SPLIT_NAME_MISMATCH", "Dataset split name does not match"
            )
        if split.sealed is not (expected_name == "test"):
            raise ValidationError("DATASET_SEAL_MISMATCH", "Dataset split sealing does not match")
        if hashlib.sha256(split.data).hexdigest() != split.sha256:
            raise ValidationError("DATASET_DIGEST_MISMATCH", "Dataset split digest does not match")
        records = parse_scoring_jsonl(split.data, allow_empty=True)
        total_bytes += len(split.data)
        if total_bytes > MAX_OUTPUT_BYTES:
            raise ResourceLimitError("OUTPUT_BYTES_EXCEEDED", "Output exceeds the byte limit")
        if len(records) != split.record_count:
            raise ValidationError("DATASET_COUNT_MISMATCH", "Dataset record counts do not match")
        canonical = b"".join(_record_bytes(record) for record in records)
        if canonical != split.data:
            raise ValidationError("DATASET_NOT_CANONICAL", "Dataset split bytes are not canonical")
        ids = [record.id for record in records]
        if ids != sorted(ids, key=str.encode):
            raise ValidationError(
                "DATASET_ORDER_INVALID", "Dataset records are not sorted by ASCII ID"
            )
        for record_id in ids:
            if split_for_id(record_id, dataset.seed) != expected_name:
                raise ValidationError(
                    "DATASET_MEMBERSHIP_INVALID", "Dataset record is in the wrong split"
                )
            if record_id in all_ids:
                raise ValidationError(
                    "DATASET_SPLITS_OVERLAP", "Dataset splits must be pairwise-disjoint"
                )
            all_ids.add(record_id)
        total += len(records)
    if total != dataset.source_record_count:
        raise ValidationError("DATASET_COUNT_MISMATCH", "Dataset record counts do not match")


def build_dataset_manifest(
    dataset: FrozenDataset,
    *,
    dataset_id: str,
    created_at: str,
    source_artifacts: Iterable[ArtifactRef | Mapping[str, object]],
    split_artifacts: Mapping[SplitName, ArtifactRef | Mapping[str, object]],
) -> dict[str, object]:
    """Build a schema-shaped manifest after semantic split verification."""

    verify_frozen_dataset(dataset)
    if _DATASET_ID_RE.fullmatch(dataset_id) is None:
        raise ValidationError("DATASET_ID_INVALID", "Dataset ID is invalid")
    _validate_utc_timestamp(created_at)
    sources = [
        item if isinstance(item, ArtifactRef) else ArtifactRef.from_mapping(item)
        for item in source_artifacts
    ]
    if not 1 <= len(sources) <= 64 or any(item.role != "source_export" for item in sources):
        raise ValidationError("DATASET_SOURCES_INVALID", "Dataset source artifacts are invalid")
    if set(split_artifacts) != set(_SPLIT_NAMES):
        raise ValidationError("DATASET_SPLITS_INVALID", "Dataset split artifacts are invalid")

    split_refs: dict[SplitName, ArtifactRef] = {}
    for name in _SPLIT_NAMES:
        raw_ref = split_artifacts[name]
        reference = (
            raw_ref if isinstance(raw_ref, ArtifactRef) else ArtifactRef.from_mapping(raw_ref)
        )
        if reference.role != "dataset_split":
            raise ValidationError("DATASET_SPLITS_INVALID", "Dataset split artifacts are invalid")
        frozen = dataset.splits[name]
        if reference.sha256 != frozen.sha256 or reference.size_bytes != len(frozen.data):
            raise ValidationError("DATASET_DIGEST_MISMATCH", "Dataset split digest does not match")
        split_refs[name] = reference

    return {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": dataset_id,
        "created_at": created_at,
        "seed": dataset.seed,
        "split_method": "sha256-id-v1",
        "split_ratios": {"train": 0.8, "validation": 0.1, "test": 0.1},
        "source_artifacts": [item.to_mapping() for item in sources],
        "record_schema": {"id": SCORING_EXAMPLE_SCHEMA, "version": SCHEMA_VERSION},
        "splits": {
            name: {
                "artifact": split_refs[name].to_mapping(),
                "record_count": dataset.splits[name].record_count,
                "sealed": dataset.splits[name].sealed,
            }
            for name in _SPLIT_NAMES
        },
    }


def verify_dataset_manifest(
    manifest: object,
    resolver: ArtifactResolver,
    *,
    max_records: int = MAX_RECORDS,
    max_input_bytes: int = MAX_TOTAL_INPUT_BYTES,
    max_json_depth: int = MAX_JSON_DEPTH,
    max_string_bytes: int = MAX_STRING_BYTES,
) -> FrozenDataset:
    """Resolve the three pinned split bytes and verify the v1 semantic contract."""

    if min(max_records, max_input_bytes, max_json_depth, max_string_bytes) < 1:
        raise ValueError("dataset verification limits must be positive")
    effective_record_limit = min(max_records, MAX_RECORDS)
    effective_byte_limit = min(max_input_bytes, MAX_TOTAL_INPUT_BYTES)
    if not isinstance(manifest, Mapping) or any(not isinstance(key, str) for key in manifest):
        raise ValidationError("DATASET_MANIFEST_INVALID", "Dataset manifest is invalid")
    value = cast(Mapping[str, object], manifest)
    required = {
        "schema_version",
        "dataset_id",
        "created_at",
        "seed",
        "split_method",
        "split_ratios",
        "source_artifacts",
        "record_schema",
        "splits",
    }
    if set(value) != required:
        raise ValidationError("DATASET_MANIFEST_INVALID", "Dataset manifest is invalid")
    if value["schema_version"] != SCHEMA_VERSION or value["split_method"] != "sha256-id-v1":
        raise ValidationError("DATASET_MANIFEST_INVALID", "Dataset manifest is invalid")
    dataset_id = value["dataset_id"]
    if not isinstance(dataset_id, str) or _DATASET_ID_RE.fullmatch(dataset_id) is None:
        raise ValidationError("DATASET_ID_INVALID", "Dataset ID is invalid")
    _validate_utc_timestamp(value["created_at"])
    record_schema = value["record_schema"]
    if not isinstance(record_schema, Mapping) or dict(record_schema) != {
        "id": SCORING_EXAMPLE_SCHEMA,
        "version": SCHEMA_VERSION,
    }:
        raise ValidationError("DATASET_RECORD_SCHEMA_INVALID", "Dataset record schema is invalid")
    raw_sources = value["source_artifacts"]
    if not isinstance(raw_sources, list) or not 1 <= len(raw_sources) <= 64:
        raise ValidationError("DATASET_SOURCES_INVALID", "Dataset source artifacts are invalid")
    sources: list[ArtifactRef] = []
    for raw_source in raw_sources:
        if not isinstance(raw_source, Mapping):
            raise ValidationError("DATASET_SOURCES_INVALID", "Dataset source artifacts are invalid")
        source = ArtifactRef.from_mapping(cast(Mapping[str, object], raw_source))
        if source.role != "source_export":
            raise ValidationError("DATASET_SOURCES_INVALID", "Dataset source artifacts are invalid")
        sources.append(source)
    total_bytes = 0
    for source in sources:
        if source.size_bytes > effective_byte_limit - total_bytes:
            raise ResourceLimitError(
                "DATASET_BYTES_EXCEEDED", "Dataset inputs exceed the byte limit"
            )
        resolved = resolver.resolve(
            source,
            ArtifactExpectation(role="source_export"),
            max_bytes=effective_byte_limit - total_bytes,
        )
        total_bytes += len(resolved.data)
    seed = value["seed"]
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValidationError(
            "DATASET_SEED_INVALID", "Dataset seed must be an unsigned 32-bit integer"
        )
    _validate_seed(seed)
    ratios = value["split_ratios"]
    expected_ratios = {
        "train": Decimal("0.8"),
        "validation": Decimal("0.1"),
        "test": Decimal("0.1"),
    }
    if not isinstance(ratios, Mapping) or set(ratios) != set(expected_ratios):
        raise ValidationError("DATASET_RATIOS_INVALID", "Dataset split ratios do not match")
    for name, expected_ratio in expected_ratios.items():
        if (
            _finite_decimal(
                ratios[name],
                code="DATASET_RATIOS_INVALID",
                message="Dataset split ratios do not match",
            )
            != expected_ratio
        ):
            raise ValidationError("DATASET_RATIOS_INVALID", "Dataset split ratios do not match")
    splits_value = value["splits"]
    if not isinstance(splits_value, Mapping) or set(splits_value) != set(_SPLIT_NAMES):
        raise ValidationError("DATASET_SPLITS_INVALID", "Dataset split metadata is invalid")

    frozen_splits: dict[SplitName, FrozenSplit] = {}
    total = 0
    for name in _SPLIT_NAMES:
        raw_split = splits_value[name]
        if not isinstance(raw_split, Mapping) or set(raw_split) != {
            "artifact",
            "record_count",
            "sealed",
        }:
            raise ValidationError("DATASET_SPLITS_INVALID", "Dataset split metadata is invalid")
        artifact_value = raw_split["artifact"]
        if not isinstance(artifact_value, Mapping):
            raise ValidationError("DATASET_SPLITS_INVALID", "Dataset split metadata is invalid")
        reference = ArtifactRef.from_mapping(cast(Mapping[str, object], artifact_value))
        count = raw_split["record_count"]
        sealed = raw_split["sealed"]
        if isinstance(count, bool) or not isinstance(count, int) or not isinstance(sealed, bool):
            raise ValidationError("DATASET_SPLITS_INVALID", "Dataset split metadata is invalid")
        if count > effective_record_limit - total:
            raise ResourceLimitError(
                "DATASET_RECORDS_EXCEEDED", "Dataset record count exceeds the limit"
            )
        if reference.size_bytes > effective_byte_limit - total_bytes:
            raise ResourceLimitError(
                "DATASET_BYTES_EXCEEDED", "Dataset inputs exceed the byte limit"
            )
        remaining_records = effective_record_limit - total
        remaining_bytes = effective_byte_limit - total_bytes
        parser = partial(
            parse_scoring_jsonl,
            max_bytes=remaining_bytes,
            max_records=remaining_records,
            max_json_depth=min(max_json_depth, MAX_JSON_DEPTH),
            max_string_bytes=min(max_string_bytes, MAX_STRING_BYTES),
            allow_empty=True,
        )
        resolved, records = resolver.resolve_with_parser(
            reference,
            ArtifactExpectation(
                role="dataset_split",
                media_type="application/x-ndjson",
                producer_name=PRODUCER_NAME,
                external_schema_id=SCORING_EXAMPLE_SCHEMA,
                external_schema_version=SCHEMA_VERSION,
            ),
            parser,
            max_bytes=remaining_bytes,
        )
        if len(records) != count:
            raise ValidationError("DATASET_COUNT_MISMATCH", "Dataset record counts do not match")
        frozen_splits[name] = FrozenSplit(
            name=name,
            data=resolved.data,
            sha256=reference.sha256,
            record_count=count,
            sealed=sealed,
        )
        total += count
        total_bytes += len(resolved.data)
    dataset = FrozenDataset(
        seed=seed,
        train=frozen_splits["train"],
        validation=frozen_splits["validation"],
        test=frozen_splits["test"],
        source_record_count=total,
    )
    verify_frozen_dataset(dataset)
    return dataset


__all__ = [
    "FrozenDataset",
    "FrozenSplit",
    "ScoringRecord",
    "SplitName",
    "build_dataset_manifest",
    "freeze_dataset",
    "freeze_jsonl_sources",
    "freeze_records",
    "parse_scoring_jsonl",
    "parse_scoring_record",
    "split_for_id",
    "verify_dataset_manifest",
    "verify_frozen_dataset",
]

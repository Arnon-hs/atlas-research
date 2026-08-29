# SPDX-License-Identifier: MIT
from __future__ import annotations

import hashlib
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from atlas_research.artifacts import (
    ArtifactResolver,
    atomic_write_private,
    build_artifact_ref,
    ensure_private_directory,
)
from atlas_research.constants import SCHEMA_VERSION, SCORING_EXAMPLE_SCHEMA
from atlas_research.dataset import (
    FrozenDataset,
    build_dataset_manifest,
    freeze_jsonl_sources,
    freeze_records,
    parse_scoring_jsonl,
    parse_scoring_record,
    split_for_id,
    verify_dataset_manifest,
    verify_frozen_dataset,
)
from atlas_research.errors import ResourceLimitError, ValidationError


def _record(index: int) -> dict[str, object]:
    return {
        "id": f"repo-{index:03d}",
        "features": {"quality": Decimal(index) / Decimal(10), "risk": -index},
        "label": Decimal(index % 101),
    }


def test_scoring_record_is_exact_ascii_finite_and_bounded() -> None:
    parsed = parse_scoring_record(_record(1))
    assert parsed.id == "repo-001"
    assert parsed.features == (("quality", Decimal("0.1")), ("risk", Decimal(-1)))

    invalid_values = [
        {**_record(1), "extra": 1},
        {**_record(1), "id": "Répo"},
        {**_record(1), "features": {"quality": True}},
        {**_record(1), "features": {"quality": Decimal("NaN")}},
        {**_record(1), "features": {"quality": 1_000_001}},
        {**_record(1), "label": 101},
    ]
    for invalid in invalid_values:
        with pytest.raises(ValidationError):
            parse_scoring_record(invalid)


def test_jsonl_rejects_duplicates_unknowns_empty_lines_and_bounds() -> None:
    row = b'{"features":{"quality":1},"id":"repo-1","label":50}'
    with pytest.raises(ValidationError, match="RECORD_ID_DUPLICATE"):
        parse_scoring_jsonl(row + b"\n" + row + b"\n")
    with pytest.raises(ValidationError, match="JSON_DUPLICATE_KEY"):
        parse_scoring_jsonl(b'{"id":"x","id":"y","features":{"a":1},"label":1}\n')
    with pytest.raises(ValidationError, match="JSONL_EMPTY_LINE"):
        parse_scoring_jsonl(row + b"\n\n")
    with pytest.raises(ResourceLimitError, match="JSONL_LINE_TOO_LONG"):
        parse_scoring_jsonl(row, max_line_bytes=10)
    with pytest.raises(ResourceLimitError, match="JSONL_RECORDS_EXCEEDED"):
        parse_scoring_jsonl(row + b"\n" + row.replace(b"repo-1", b"repo-2"), max_records=1)


def test_split_algorithm_is_byte_exact() -> None:
    prefix = b"atlas-research:split:v1"
    seed = 0x01020304
    record_id = "repo-001"
    digest = hashlib.sha256(
        prefix + b"\x00" + seed.to_bytes(4, "big") + b"\x00" + record_id.encode("ascii")
    ).digest()
    bucket = int.from_bytes(digest[:8], "big")
    expected = (
        "train"
        if bucket < 14_757_395_258_967_641_292
        else "validation"
        if bucket < 16_602_069_666_338_596_454
        else "test"
    )
    assert split_for_id(record_id, seed) == expected
    with pytest.raises(ValidationError, match="DATASET_SEED_INVALID"):
        split_for_id(record_id, -1)


def test_freeze_is_deterministic_canonical_disjoint_and_sealed() -> None:
    records = [_record(index) for index in range(100)]
    first = freeze_records(records, seed=7)
    second = freeze_records(reversed(records), seed=7)
    assert first == second
    assert first.test.sealed is True
    assert first.train.sealed is False
    assert first.validation.sealed is False
    assert first.source_record_count == sum(split.record_count for split in first.splits.values())
    assert all(not split.data or split.data.endswith(b"\n") for split in first.splits.values())
    assert all(split.record_count > 0 for split in first.splits.values())
    verify_frozen_dataset(first)


def test_verifier_rejects_digest_count_seal_and_membership_tampering() -> None:
    dataset = freeze_records((_record(index) for index in range(100)), seed=9)
    with pytest.raises(ValidationError, match="DATASET_DIGEST_MISMATCH"):
        verify_frozen_dataset(replace(dataset, train=replace(dataset.train, sha256="0" * 64)))
    with pytest.raises(ValidationError, match="DATASET_COUNT_MISMATCH"):
        verify_frozen_dataset(
            replace(dataset, validation=replace(dataset.validation, record_count=999))
        )
    with pytest.raises(ValidationError, match="DATASET_SEAL_MISMATCH"):
        verify_frozen_dataset(replace(dataset, test=replace(dataset.test, sealed=False)))

    populated = next(split for split in dataset.splits.values() if split.record_count)
    wrong_name = next(name for name in dataset.splits if name != populated.name)
    wrong = replace(populated, name=wrong_name)
    changed = FrozenDataset(
        seed=dataset.seed,
        train=wrong if wrong_name == "train" else dataset.train,
        validation=wrong if wrong_name == "validation" else dataset.validation,
        test=wrong if wrong_name == "test" else dataset.test,
        source_record_count=dataset.source_record_count,
    )
    with pytest.raises(ValidationError):
        verify_frozen_dataset(changed)


def test_freeze_jsonl_sources_rejects_cross_source_duplicate_ids() -> None:
    row = b'{"features":{"quality":1},"id":"repo-1","label":50}\n'
    with pytest.raises(ValidationError, match="RECORD_ID_DUPLICATE"):
        freeze_jsonl_sources([row, row], seed=1)


def test_freeze_honors_stricter_output_limit() -> None:
    with pytest.raises(ResourceLimitError, match="OUTPUT_BYTES_EXCEEDED"):
        freeze_records([_record(1)], seed=1, max_output_bytes=10)


def test_manifest_verification_honors_stricter_caller_limits(tmp_path: Path) -> None:
    dataset = freeze_records((_record(index) for index in range(30)), seed=3)
    root = ensure_private_directory(tmp_path / "artifacts")
    split_refs = {}
    for name, split in dataset.splits.items():
        uri = f"{name}.jsonl"
        atomic_write_private(root, uri, split.data)
        split_refs[name] = build_artifact_ref(
            uri=uri,
            role="dataset_split",
            media_type="application/x-ndjson",
            data=split.data,
            external_schema_id=SCORING_EXAMPLE_SCHEMA,
            external_schema_version=SCHEMA_VERSION,
        )
    source_data = b"source"
    source_path = atomic_write_private(root, "source.jsonl", source_data)
    source = build_artifact_ref(
        uri="source.jsonl",
        role="source_export",
        media_type="application/x-ndjson",
        data=source_data,
        producer_name="fixture",
        producer_version="1.0.0",
        external_schema_id="urn:example:source",
        external_schema_version="1.0.0",
    )
    manifest = build_dataset_manifest(
        dataset,
        dataset_id="fixture",
        created_at="2026-08-30T00:00:00Z",
        source_artifacts=[source],
        split_artifacts=split_refs,
    )
    with (
        ArtifactResolver(root) as resolver,
        pytest.raises(ResourceLimitError, match="DATASET_RECORDS_EXCEEDED"),
    ):
        verify_dataset_manifest(manifest, resolver, max_records=dataset.source_record_count - 1)

    total_input_bytes = len(source_data) + sum(len(split.data) for split in dataset.splits.values())
    with ArtifactResolver(root) as resolver:
        assert (
            verify_dataset_manifest(manifest, resolver, max_input_bytes=total_input_bytes)
            == dataset
        )
    with (
        ArtifactResolver(root) as resolver,
        pytest.raises(ResourceLimitError, match="DATASET_BYTES_EXCEEDED"),
    ):
        verify_dataset_manifest(manifest, resolver, max_input_bytes=total_input_bytes - 1)

    source_path.write_bytes(b"tamper")
    with (
        ArtifactResolver(root) as resolver,
        pytest.raises(ValidationError, match="ARTIFACT_DIGEST_MISMATCH"),
    ):
        verify_dataset_manifest(manifest, resolver)

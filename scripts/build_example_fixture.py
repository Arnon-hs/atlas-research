# SPDX-License-Identifier: MIT
"""Build the deterministic synthetic fixture committed under examples/fixture-v1."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

from atlas_research.artifacts import (
    ArtifactRef,
    atomic_write_private,
    build_artifact_ref,
    ensure_private_directory,
    write_canonical_json_private,
)
from atlas_research.canonical import canonical_json_bytes, canonical_sha256
from atlas_research.constants import (
    BENCHMARK_MANIFEST_SCHEMA,
    CANDIDATE_SCHEMA,
    DATASET_MANIFEST_SCHEMA,
    LINEAR_EVALUATOR_SCHEMA,
    SCHEMA_VERSION,
    SCORING_EXAMPLE_SCHEMA,
)
from atlas_research.dataset import SplitName, build_dataset_manifest, freeze_records

CREATED_AT = "2026-08-29T00:00:00Z"
DEADLINE = "2099-01-01T00:00:00Z"
_BUNDLE_FILES = (
    "baseline.json",
    "benchmark.json",
    "candidate-payload.json",
    "candidate.json",
    "dataset.json",
    "job.json",
    "source.jsonl",
    "test.jsonl",
    "train.jsonl",
    "validation.jsonl",
)


def _limits() -> dict[str, int]:
    return {
        "wall_seconds": 30,
        "max_records": 100,
        "max_input_bytes": 1 << 20,
        "max_output_bytes": 1 << 20,
        "max_workspace_bytes": 1 << 20,
        "max_peak_rss_bytes": 1 << 30,
        "max_open_files": 64,
        "max_json_depth": 16,
        "max_string_bytes": 4_096,
    }


def _write_json_artifact(
    root: Path,
    *,
    uri: str,
    role: str,
    media_type: str,
    value: object,
    schema_id: str,
) -> ArtifactRef:
    data = canonical_json_bytes(value) + b"\n"
    atomic_write_private(root, uri, data)
    return build_artifact_ref(
        uri=uri,
        role=role,
        media_type=media_type,
        data=data,
        external_schema_id=schema_id,
        external_schema_version=SCHEMA_VERSION,
    )


def build_fixture(root: Path) -> None:
    artifact_root = ensure_private_directory(root)
    records = [
        {"id": "repo-000", "features": {"quality": 10}, "label": 20},
        {"id": "repo-004", "features": {"quality": 10}, "label": 20},
        {"id": "repo-007", "features": {"quality": 10}, "label": 20},
    ]
    source_data = b"".join(canonical_json_bytes(record) + b"\n" for record in records)
    atomic_write_private(artifact_root, "source.jsonl", source_data)
    source_ref = build_artifact_ref(
        uri="source.jsonl",
        role="source_export",
        media_type="application/x-ndjson",
        data=source_data,
        producer_name="atlas-research-fixture",
        producer_version="1.0.0",
        external_schema_id="urn:atlasrepo:example:synthetic-scoring-export:v1",
        external_schema_version=SCHEMA_VERSION,
    )

    frozen = freeze_records(records, seed=7)
    split_refs: dict[SplitName, ArtifactRef] = {}
    for name, split in frozen.splits.items():
        uri = f"{name}.jsonl"
        atomic_write_private(artifact_root, uri, split.data)
        split_refs[name] = build_artifact_ref(
            uri=uri,
            role="dataset_split",
            media_type="application/x-ndjson",
            data=split.data,
            external_schema_id=SCORING_EXAMPLE_SCHEMA,
            external_schema_version=SCHEMA_VERSION,
        )

    dataset = build_dataset_manifest(
        frozen,
        dataset_id="atlas-research-fixture-v1",
        created_at=CREATED_AT,
        source_artifacts=[source_ref],
        split_artifacts=split_refs,
    )
    dataset_ref = _write_json_artifact(
        artifact_root,
        uri="dataset.json",
        role="dataset_manifest",
        media_type="application/vnd.atlas-research.dataset-manifest+json",
        value=dataset,
        schema_id=DATASET_MANIFEST_SCHEMA,
    )

    baseline = {"schema": LINEAR_EVALUATOR_SCHEMA, "bias": 0, "weights": {"quality": 1}}
    baseline_ref = _write_json_artifact(
        artifact_root,
        uri="baseline.json",
        role="evaluation_payload",
        media_type="application/json",
        value=baseline,
        schema_id=LINEAR_EVALUATOR_SCHEMA,
    )
    proposed = {"schema": LINEAR_EVALUATOR_SCHEMA, "bias": 0, "weights": {"quality": 2}}
    proposed_ref = _write_json_artifact(
        artifact_root,
        uri="candidate-payload.json",
        role="evaluation_payload",
        media_type="application/json",
        value=proposed,
        schema_id=LINEAR_EVALUATOR_SCHEMA,
    )
    candidate = {
        "schema_version": SCHEMA_VERSION,
        "candidate_id": "atlas-research-fixture-v1-candidate",
        "created_at": CREATED_AT,
        "status": "proposed",
        "parent_evaluation_payload": baseline_ref.to_mapping(),
        "research_level": "LEVEL_2",
        "hypothesis": "Increase one synthetic quality weight from 1 to 2.",
        "changed_variable": {"path": "weights.quality", "old_value": 1, "new_value": 2},
        "evaluation_payload": proposed_ref.to_mapping(),
        "target_contract": {
            "id": "urn:atlasrepo:scout:scoring-definition",
            "version": SCHEMA_VERSION,
        },
        "generator": {"kind": "human"},
    }
    candidate_ref = _write_json_artifact(
        artifact_root,
        uri="candidate.json",
        role="candidate",
        media_type="application/vnd.atlas-research.candidate+json",
        value=candidate,
        schema_id=CANDIDATE_SCHEMA,
    )
    benchmark = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_id": "atlas-research-fixture-v1-benchmark",
        "created_at": CREATED_AT,
        "dataset_manifest": dataset_ref.to_mapping(),
        "baseline_evaluation_payload": baseline_ref.to_mapping(),
        "evaluation_split": "validation",
        "metrics": {
            "mae": {
                "direction": "lower",
                "gate": {"absolute_threshold": 0, "minimum_delta": 0},
            },
            "calibration_error": {
                "direction": "lower",
                "parameters": {"bins": 10},
                "gate": {"absolute_threshold": 0, "minimum_delta": 0},
            },
        },
        "minimum_records": {"train": 1, "validation": 1, "test": 1},
        "limits": _limits(),
    }
    benchmark_ref = _write_json_artifact(
        artifact_root,
        uri="benchmark.json",
        role="benchmark_manifest",
        media_type="application/vnd.atlas-research.benchmark-manifest+json",
        value=benchmark,
        schema_id=BENCHMARK_MANIFEST_SCHEMA,
    )
    job = {
        "schema_version": SCHEMA_VERSION,
        "task": "research.experiment",
        "job_id": "atlas-research-fixture-v1-job",
        "attempt": 1,
        "idempotency_key": "atlas-research-fixture-v1",
        "created_at": CREATED_AT,
        "deadline": DEADLINE,
        "dataset_manifest": dataset_ref.to_mapping(),
        "benchmark_manifest": benchmark_ref.to_mapping(),
        "baseline_evaluation_payload": baseline_ref.to_mapping(),
        "candidate": candidate_ref.to_mapping(),
        "evaluation_split": "validation",
        "limits": _limits(),
    }
    write_canonical_json_private(artifact_root, "job.json", job)

    files: dict[str, dict[str, int | str]] = {}
    for bundle_name in _BUNDLE_FILES:
        data = (artifact_root / bundle_name).read_bytes()
        files[bundle_name] = {
            "sha256": hashlib.sha256(data).hexdigest(),
            "size_bytes": len(data),
        }
    manifest = {
        "fixture_version": "1.0.0",
        "purpose": "synthetic offline validation smoke",
        "job": "job.json",
        "job_spec_sha256": canonical_sha256(job),
        "expected": {"status": "completed", "decision": "KEEP", "records_evaluated": 1},
        "files": files,
    }
    write_canonical_json_private(artifact_root, "bundle-manifest.json", manifest)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    build_fixture(args.output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# SPDX-License-Identifier: MIT
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from atlas_research.errors import ValidationError
from atlas_research.evaluation import METRIC_DIRECTIONS, METRIC_PARAMETERS, parse_metric_specs

SCHEMA_ROOT = Path(__file__).parents[1] / "schemas" / "v1"
SCHEMAS = {path.name: json.loads(path.read_text()) for path in SCHEMA_ROOT.glob("*.json")}


def _registry() -> Registry[Any]:
    registry: Registry[Any] = Registry()
    for schema in SCHEMAS.values():
        registry = registry.with_resource(schema["$id"], Resource.from_contents(schema))
    return registry


def _artifact(role: str) -> dict[str, Any]:
    media_and_schema = {
        "source_export": ("application/x-ndjson", "urn:atlasrepo:test:source:v1"),
        "dataset_split": (
            "application/x-ndjson",
            "urn:atlasrepo:atlas-research:record:v1:scoring-example",
        ),
        "dataset_manifest": (
            "application/vnd.atlas-research.dataset-manifest+json",
            "urn:atlasrepo:atlas-research:schema:v1:dataset-manifest",
        ),
        "benchmark_manifest": (
            "application/vnd.atlas-research.benchmark-manifest+json",
            "urn:atlasrepo:atlas-research:schema:v1:benchmark-manifest",
        ),
        "evaluation_payload": ("application/json", "urn:atlasrepo:test:evaluator:v1"),
        "candidate": (
            "application/vnd.atlas-research.candidate+json",
            "urn:atlasrepo:atlas-research:schema:v1:candidate-artifact",
        ),
        "experiment_receipt": (
            "application/vnd.atlas-research.experiment-receipt+json",
            "urn:atlasrepo:atlas-research:schema:v1:experiment-receipt",
        ),
    }
    media_type, schema_id = media_and_schema[role]
    return {
        "uri": "inputs/artifact.json",
        "role": role,
        "media_type": media_type,
        "sha256": "0" * 64,
        "size_bytes": 1,
        "producer": {"name": "atlas-research", "version": "0.1.0"},
        "external_schema": {"id": schema_id, "version": "1.0.0"},
    }


def _limits() -> dict[str, int]:
    return {
        "wall_seconds": 60,
        "max_records": 100,
        "max_input_bytes": 1_000,
        "max_output_bytes": 1_000,
        "max_workspace_bytes": 10_000,
        "max_peak_rss_bytes": 1_048_576,
        "max_open_files": 16,
        "max_json_depth": 8,
        "max_string_bytes": 128,
    }


def valid_documents() -> dict[str, dict[str, Any]]:
    timestamp = "2026-08-30T00:00:00Z"
    digest = "0" * 64
    dataset = _artifact("dataset_manifest")
    benchmark = _artifact("benchmark_manifest")
    evaluator = _artifact("evaluation_payload")
    candidate = _artifact("candidate")
    receipt = _artifact("experiment_receipt")
    return {
        "artifact-ref.v1.schema.json": dataset,
        "dataset-manifest.v1.schema.json": {
            "schema_version": "1.0.0",
            "dataset_id": "dataset-1",
            "created_at": timestamp,
            "seed": 7,
            "split_method": "sha256-id-v1",
            "split_ratios": {"train": 0.8, "validation": 0.1, "test": 0.1},
            "source_artifacts": [_artifact("source_export")],
            "record_schema": {
                "id": "urn:atlasrepo:atlas-research:record:v1:scoring-example",
                "version": "1.0.0",
            },
            "splits": {
                split: {
                    "artifact": _artifact("dataset_split"),
                    "record_count": 1,
                    "sealed": split == "test",
                }
                for split in ("train", "validation", "test")
            },
        },
        "benchmark-manifest.v1.schema.json": {
            "schema_version": "1.0.0",
            "benchmark_id": "benchmark-1",
            "created_at": timestamp,
            "dataset_manifest": dataset,
            "baseline_evaluation_payload": evaluator,
            "evaluation_split": "validation",
            "metrics": {
                "mae": {
                    "direction": "lower",
                    "gate": {"absolute_threshold": 10, "minimum_delta": 0},
                },
                "calibration_error": {
                    "direction": "lower",
                    "parameters": {"bins": 10},
                    "gate": {"absolute_threshold": 1, "minimum_delta": 0},
                },
            },
            "minimum_records": {"train": 1, "validation": 1, "test": 1},
            "limits": _limits(),
        },
        "candidate-artifact.v1.schema.json": {
            "schema_version": "1.0.0",
            "candidate_id": "candidate-1",
            "created_at": timestamp,
            "status": "proposed",
            "parent_evaluation_payload": evaluator,
            "research_level": "LEVEL_2",
            "hypothesis": "Increase one bounded feature weight.",
            "changed_variable": {"path": "weights.quality", "old_value": 1, "new_value": 2},
            "evaluation_payload": evaluator,
            "target_contract": {"id": "urn:atlasrepo:scout:scoring", "version": "1.0.0"},
            "generator": {"kind": "human"},
        },
        "experiment-receipt.v1.schema.json": {
            "schema_version": "1.0.0",
            "receipt_id": "receipt-1",
            "previous_receipt_sha256": None,
            "created_at": timestamp,
            "started_at": timestamp,
            "finished_at": timestamp,
            "experiment_id": "experiment-1",
            "job_id": "job-1",
            "attempt": 1,
            "idempotency_key": "experiment-key-0001",
            "job_spec_sha256": digest,
            "canonical_result_sha256": digest,
            "dataset_manifest": dataset,
            "benchmark_manifest": benchmark,
            "baseline_evaluation_payload": evaluator,
            "candidate": candidate,
            "evaluation_split": "validation",
            "canonical_result": {
                "metrics": {
                    "mae": {
                        "baseline": "2",
                        "candidate": "1",
                        "candidate_minus_baseline": "-1",
                        "passed": True,
                    },
                    "calibration_error": {
                        "baseline": "0.2",
                        "candidate": "0.1",
                        "candidate_minus_baseline": "-0.1",
                        "passed": True,
                    },
                },
                "all_gates_passed": True,
                "decision": "KEEP",
                "reason_codes": ["ALL_GATES_PASSED"],
            },
            "resource_usage": {
                "wall_milliseconds": 1,
                "records_evaluated": 1,
                "peak_rss_bytes": 1,
            },
            "provenance": {
                "atlas_research_version": "0.1.0",
                "git_commit": "0" * 40,
                "source_revision_kind": "verified_checkout",
                "python_version": "3.11",
                "platform": "test",
                "worker_id": "worker-1",
                "worker_session_id": "session-1",
            },
        },
        "research-experiment-job.v1.schema.json": {
            "schema_version": "1.0.0",
            "task": "research.experiment",
            "job_id": "job-1",
            "attempt": 1,
            "idempotency_key": "experiment-key-0001",
            "created_at": timestamp,
            "deadline": "2026-08-30T01:00:00Z",
            "dataset_manifest": dataset,
            "benchmark_manifest": benchmark,
            "baseline_evaluation_payload": evaluator,
            "candidate": candidate,
            "evaluation_split": "validation",
            "limits": _limits(),
        },
        "research-experiment-result.v1.schema.json": {
            "schema_version": "1.0.0",
            "task": "research.experiment",
            "job_id": "job-1",
            "attempt": 1,
            "idempotency_key": "experiment-key-0001",
            "job_spec_sha256": digest,
            "status": "completed",
            "started_at": timestamp,
            "finished_at": timestamp,
            "worker": {"worker_id": "worker-1", "session_id": "session-1", "version": "0.1.0"},
            "receipt": receipt,
            "artifacts": [],
        },
    }


@pytest.mark.parametrize("schema_name", sorted(SCHEMAS))
def test_valid_contract_document(schema_name: str) -> None:
    Draft202012Validator(SCHEMAS[schema_name], registry=_registry()).validate(
        valid_documents()[schema_name]
    )


def test_terminal_error_receipt_has_no_metric_values() -> None:
    receipt = copy.deepcopy(valid_documents()["experiment-receipt.v1.schema.json"])
    receipt["canonical_result"] = {
        "metrics": {},
        "all_gates_passed": False,
        "decision": "ERROR",
        "reason_codes": ["EVALUATION_INCOMPLETE"],
        "error": {
            "code": "EVALUATION_INCOMPLETE",
            "message": "Evaluation did not complete",
        },
    }
    validator = Draft202012Validator(
        SCHEMAS["experiment-receipt.v1.schema.json"], registry=_registry()
    )
    validator.validate(receipt)

    invalid = copy.deepcopy(receipt)
    invalid["canonical_result"]["metrics"] = {  # type: ignore[index]
        "mae": {
            "baseline": "1",
            "candidate": "1",
            "candidate_minus_baseline": "0",
            "passed": False,
        }
    }
    _assert_invalid("experiment-receipt.v1.schema.json", invalid)


def _assert_invalid(schema_name: str, document: dict[str, Any]) -> None:
    errors = list(
        Draft202012Validator(SCHEMAS[schema_name], registry=_registry()).iter_errors(document)
    )
    assert errors


def test_contract_critical_negatives() -> None:
    documents = valid_documents()

    dataset = copy.deepcopy(documents["dataset-manifest.v1.schema.json"])
    dataset["splits"]["test"]["sealed"] = False
    _assert_invalid("dataset-manifest.v1.schema.json", dataset)

    benchmark = copy.deepcopy(documents["benchmark-manifest.v1.schema.json"])
    benchmark["metrics"]["mae"]["direction"] = "higher"
    _assert_invalid("benchmark-manifest.v1.schema.json", benchmark)

    candidate = copy.deepcopy(documents["candidate-artifact.v1.schema.json"])
    candidate["generator"] = {"kind": "qwen"}
    _assert_invalid("candidate-artifact.v1.schema.json", candidate)

    receipt = copy.deepcopy(documents["experiment-receipt.v1.schema.json"])
    receipt["canonical_result"]["metrics"]["mae"]["passed"] = False
    _assert_invalid("experiment-receipt.v1.schema.json", receipt)

    receipt = copy.deepcopy(documents["experiment-receipt.v1.schema.json"])
    receipt["canonical_result"]["metrics"]["mae"]["candidate"] = "1.00"
    _assert_invalid("experiment-receipt.v1.schema.json", receipt)

    job = copy.deepcopy(documents["research-experiment-job.v1.schema.json"])
    job["dataset_manifest"] = _artifact("candidate")
    _assert_invalid("research-experiment-job.v1.schema.json", job)

    result = copy.deepcopy(documents["research-experiment-result.v1.schema.json"])
    result["status"] = "rejected"
    result.pop("receipt")
    result["error"] = {"code": "REJECTED", "message": "rejected", "retryable": False}
    result["artifacts"] = [_artifact("experiment_receipt")]
    _assert_invalid("research-experiment-result.v1.schema.json", result)


@pytest.mark.parametrize("metric_name", METRIC_PARAMETERS)
def test_benchmark_metric_parameter_schema_matches_runtime_allowlist(metric_name: str) -> None:
    parameter_examples = {"threshold": 50, "bins": 10, "max_pairs": 100}
    for parameter_name, parameter_value in parameter_examples.items():
        benchmark = copy.deepcopy(valid_documents()["benchmark-manifest.v1.schema.json"])
        definition = {
            "direction": METRIC_DIRECTIONS[metric_name],
            "parameters": {parameter_name: parameter_value},
            "gate": {"absolute_threshold": 0, "minimum_delta": 0},
        }
        companion_name = "mae" if metric_name != "mae" else "calibration_error"
        companion: dict[str, object] = {
            "direction": METRIC_DIRECTIONS[companion_name],
            "gate": {"absolute_threshold": 0, "minimum_delta": 0},
        }
        if companion_name == "calibration_error":
            companion["parameters"] = {"bins": 10}
        benchmark["metrics"] = {metric_name: definition, companion_name: companion}

        if parameter_name in METRIC_PARAMETERS[metric_name]:
            parse_metric_specs(benchmark["metrics"])
            Draft202012Validator(
                SCHEMAS["benchmark-manifest.v1.schema.json"], registry=_registry()
            ).validate(benchmark)
        else:
            with pytest.raises(ValidationError, match="outside its allowlist"):
                parse_metric_specs(benchmark["metrics"])
            _assert_invalid("benchmark-manifest.v1.schema.json", benchmark)


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-08-30 00:00:00Z",
        "20260830T000000Z",
        "2026-08-30T00:00Z",
        "2026-08-30T00:00:00.Z",
        "2026-08-30T00:00:00.1234567Z",
        "2026-08-30T24:00:00Z",
        "2026-08-30T00:00:00+00:00",
    ],
)
def test_job_schema_rejects_noncanonical_utc_timestamp_forms(timestamp: str) -> None:
    job = copy.deepcopy(valid_documents()["research-experiment-job.v1.schema.json"])
    job["created_at"] = timestamp
    _assert_invalid("research-experiment-job.v1.schema.json", job)


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-08-30T00:00:00Z",
        "2026-08-30T00:00:00.1Z",
        "2026-08-30T00:00:00.123456Z",
    ],
)
def test_job_schema_accepts_canonical_utc_with_bounded_fraction(timestamp: str) -> None:
    job = copy.deepcopy(valid_documents()["research-experiment-job.v1.schema.json"])
    job["created_at"] = timestamp
    Draft202012Validator(
        SCHEMAS["research-experiment-job.v1.schema.json"], registry=_registry()
    ).validate(job)


def test_contract_timestamp_definitions_share_the_canonical_pattern() -> None:
    patterns = {
        schema["$defs"]["timestamp"]["pattern"]
        for schema in SCHEMAS.values()
        if "timestamp" in schema.get("$defs", {})
    }
    assert len(patterns) == 1
    assert patterns != {"Z$"}


def test_all_schemas_are_draft_2020_12() -> None:
    for schema in SCHEMAS.values():
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        Draft202012Validator.check_schema(schema)

# SPDX-License-Identifier: MIT
"""Versioned contract identities and operator hard ceilings."""

from __future__ import annotations

from typing import Final

ARTIFACT_REF_SCHEMA: Final = "urn:atlasrepo:atlas-research:schema:v1:artifact-ref"
DATASET_MANIFEST_SCHEMA: Final = "urn:atlasrepo:atlas-research:schema:v1:dataset-manifest"
BENCHMARK_MANIFEST_SCHEMA: Final = "urn:atlasrepo:atlas-research:schema:v1:benchmark-manifest"
CANDIDATE_SCHEMA: Final = "urn:atlasrepo:atlas-research:schema:v1:candidate-artifact"
RECEIPT_SCHEMA: Final = "urn:atlasrepo:atlas-research:schema:v1:experiment-receipt"
JOB_SCHEMA: Final = "urn:atlasrepo:atlas-research:schema:v1:research-experiment-job"
RESULT_SCHEMA: Final = "urn:atlasrepo:atlas-research:schema:v1:research-experiment-result"

SCORING_EXAMPLE_SCHEMA: Final = "urn:atlasrepo:atlas-research:record:v1:scoring-example"
LINEAR_EVALUATOR_SCHEMA: Final = "urn:atlasrepo:atlas-research:fixture:v1:linear-evaluator"

MAX_JOB_BYTES: Final = 1 << 20
MAX_ARTIFACTS: Final = 64
MAX_ARTIFACT_BYTES: Final = 256 << 20
MAX_TOTAL_INPUT_BYTES: Final = 1 << 30
MAX_OUTPUT_BYTES: Final = 256 << 20
MAX_WORKSPACE_BYTES: Final = 1 << 30
MAX_RECORDS: Final = 1_000_000
MAX_JSONL_LINE_BYTES: Final = 1 << 20
MAX_FEATURES: Final = 256
MAX_WALL_SECONDS: Final = 3_600
MAX_RSS_BYTES: Final = 6 << 30
MAX_OPEN_FILES: Final = 128
MAX_JSON_DEPTH: Final = 32
MAX_STRING_BYTES: Final = 65_536
MAX_QWEN_RESPONSE_BYTES: Final = 65_536
MAX_QWEN_SHOW_BYTES: Final = 262_144
MAX_QWEN_TIMEOUT_SECONDS: Final = 60

PRODUCER_NAME: Final = "atlas-research"
SCHEMA_VERSION: Final = "1.0.0"

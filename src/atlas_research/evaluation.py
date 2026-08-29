# SPDX-License-Identifier: MIT
"""Offline evaluator composition, benchmark gates, and canonical results."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Literal, TypedDict, cast

from .candidate import LinearEvaluator, score_linear_evaluator, validate_linear_evaluator
from .canonical import decimal_string, quantize_decimal
from .constants import MAX_RECORDS
from .errors import ValidationError
from .metrics import (
    MAX_PAIRWISE_PAIRS,
    Number,
    calibration_error,
    f1,
    mae,
    ndcg,
    pairwise_accuracy,
    spearman,
)

MetricDirection = Literal["higher", "lower"]
Decision = Literal["KEEP", "DISCARD"]

METRIC_DIRECTIONS: Mapping[str, MetricDirection] = {
    "mae": "lower",
    "spearman": "higher",
    "pairwise_accuracy": "higher",
    "ndcg_at_10": "higher",
    "ndcg_at_50": "higher",
    "f1": "higher",
    "calibration_error": "lower",
}
METRIC_PARAMETERS: Mapping[str, frozenset[str]] = {
    "mae": frozenset(),
    "spearman": frozenset(),
    "pairwise_accuracy": frozenset({"max_pairs"}),
    "ndcg_at_10": frozenset(),
    "ndcg_at_50": frozenset(),
    "f1": frozenset({"threshold"}),
    "calibration_error": frozenset({"bins"}),
}

RECORD_ID = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,127}$")


@dataclass(frozen=True, slots=True)
class MetricDefinition:
    name: str
    direction: MetricDirection
    absolute_threshold: Decimal
    minimum_delta: Decimal
    threshold: Decimal = Decimal(50)
    bins: int = 10
    max_pairs: int = MAX_PAIRWISE_PAIRS


class MetricResult(TypedDict):
    baseline: str
    candidate: str
    candidate_minus_baseline: str
    passed: bool


class CanonicalResult(TypedDict):
    metrics: dict[str, MetricResult]
    all_gates_passed: bool
    decision: Decision
    reason_codes: list[str]


def _decimal(value: object, *, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float)):
        raise ValidationError("INVALID_BENCHMARK", f"{field} must be a finite number")
    try:
        converted = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValidationError("INVALID_BENCHMARK", f"{field} must be a finite number") from exc
    if not converted.is_finite():
        raise ValidationError("INVALID_BENCHMARK", f"{field} must be a finite number")
    return converted


def _integer(value: object, *, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValidationError(
            "INVALID_BENCHMARK", f"{field} must be an integer in [{minimum}, {maximum}]"
        )
    return value


def _gate_ranges(name: str) -> tuple[Decimal, Decimal, Decimal]:
    if name == "mae":
        return Decimal(0), Decimal(100), Decimal(100)
    if name == "spearman":
        return Decimal(-1), Decimal(1), Decimal(2)
    return Decimal(0), Decimal(1), Decimal(1)


def parse_metric_specs(metric_specs: Mapping[str, object]) -> dict[str, MetricDefinition]:
    """Validate a benchmark metric map, including fixed directions/parameters."""

    if not 1 <= len(metric_specs) <= len(METRIC_DIRECTIONS):
        raise ValidationError("INVALID_BENCHMARK", "benchmark must define between 1 and 7 metrics")
    unknown = set(metric_specs) - set(METRIC_DIRECTIONS)
    if unknown:
        raise ValidationError("UNKNOWN_METRIC", "benchmark contains a metric outside the allowlist")
    parsed: dict[str, MetricDefinition] = {}
    for name in METRIC_DIRECTIONS:
        if name not in metric_specs:
            continue
        raw_definition = metric_specs[name]
        if not isinstance(raw_definition, Mapping):
            raise ValidationError("INVALID_BENCHMARK", f"metrics.{name} must be an object")
        if (
            not {"direction", "gate"}
            <= set(raw_definition)
            <= {
                "direction",
                "parameters",
                "gate",
            }
        ):
            raise ValidationError(
                "INVALID_BENCHMARK", f"metrics.{name} has missing or unknown properties"
            )
        expected_direction = METRIC_DIRECTIONS[name]
        if raw_definition.get("direction") != expected_direction:
            raise ValidationError(
                "METRIC_DIRECTION_MISMATCH",
                f"metrics.{name}.direction must be {expected_direction}",
            )

        raw_parameters = raw_definition.get("parameters", {})
        if not isinstance(raw_parameters, Mapping):
            raise ValidationError(
                "INVALID_BENCHMARK", f"metrics.{name}.parameters must be an object"
            )
        allowed_parameters = METRIC_PARAMETERS[name]
        if set(raw_parameters) - allowed_parameters:
            raise ValidationError(
                "METRIC_PARAMETER_MISMATCH",
                f"metrics.{name} has parameters outside its allowlist",
            )

        raw_gate = raw_definition.get("gate")
        if not isinstance(raw_gate, Mapping) or set(raw_gate) != {
            "absolute_threshold",
            "minimum_delta",
        }:
            raise ValidationError(
                "INVALID_BENCHMARK",
                f"metrics.{name}.gate must contain absolute_threshold and minimum_delta",
            )
        absolute_threshold = _decimal(
            raw_gate.get("absolute_threshold"),
            field=f"metrics.{name}.gate.absolute_threshold",
        )
        minimum_delta = _decimal(
            raw_gate.get("minimum_delta"), field=f"metrics.{name}.gate.minimum_delta"
        )
        absolute_minimum, absolute_maximum, delta_maximum = _gate_ranges(name)
        if not absolute_minimum <= absolute_threshold <= absolute_maximum:
            raise ValidationError(
                "INVALID_BENCHMARK", f"metrics.{name} absolute threshold is out of range"
            )
        if not Decimal(0) <= minimum_delta <= delta_maximum:
            raise ValidationError(
                "INVALID_BENCHMARK", f"metrics.{name} minimum delta is out of range"
            )

        threshold = Decimal(50)
        bins = 10
        max_pairs = MAX_PAIRWISE_PAIRS
        if "threshold" in raw_parameters:
            threshold = _decimal(
                raw_parameters.get("threshold"), field=f"metrics.{name}.parameters.threshold"
            )
            if not Decimal(0) <= threshold <= Decimal(100):
                raise ValidationError(
                    "INVALID_BENCHMARK", f"metrics.{name}.parameters.threshold is out of range"
                )
        if "bins" in raw_parameters:
            bins = _integer(
                raw_parameters.get("bins"),
                field=f"metrics.{name}.parameters.bins",
                minimum=2,
                maximum=50,
            )
        if "max_pairs" in raw_parameters:
            max_pairs = _integer(
                raw_parameters.get("max_pairs"),
                field=f"metrics.{name}.parameters.max_pairs",
                minimum=1,
                maximum=MAX_PAIRWISE_PAIRS,
            )
        parsed[name] = MetricDefinition(
            name=name,
            direction=expected_direction,
            absolute_threshold=absolute_threshold,
            minimum_delta=minimum_delta,
            threshold=threshold,
            bins=bins,
            max_pairs=max_pairs,
        )
    return parsed


def _metric_value(
    definition: MetricDefinition, labels: Sequence[Number], predictions: Sequence[Number]
) -> Decimal:
    if definition.name == "mae":
        return mae(labels, predictions)
    if definition.name == "spearman":
        return spearman(labels, predictions)
    if definition.name == "pairwise_accuracy":
        return pairwise_accuracy(labels, predictions, max_pairs=definition.max_pairs)
    if definition.name == "ndcg_at_10":
        return ndcg(labels, predictions, k=10)
    if definition.name == "ndcg_at_50":
        return ndcg(labels, predictions, k=50)
    if definition.name == "f1":
        return f1(labels, predictions, threshold=definition.threshold)
    if definition.name == "calibration_error":
        return calibration_error(labels, predictions, bins=definition.bins)
    raise AssertionError(f"unhandled metric {definition.name}")  # pragma: no cover


def evaluate_predictions(
    labels: Sequence[Number],
    predictions: Sequence[Number],
    metric_specs: Mapping[str, object],
) -> dict[str, Decimal]:
    """Compute exactly the allowlisted metrics declared by one benchmark."""

    definitions = parse_metric_specs(metric_specs)
    return {
        name: _metric_value(definition, labels, predictions)
        for name, definition in definitions.items()
    }


def _ordered_records(
    records: Sequence[Mapping[str, object]],
) -> tuple[tuple[str, Mapping[str, object], Number], ...]:
    if not 1 <= len(records) <= MAX_RECORDS:
        raise ValidationError(
            "INVALID_SCORING_EXAMPLES", f"records must contain between 1 and {MAX_RECORDS} rows"
        )
    parsed: list[tuple[str, Mapping[str, object], Number]] = []
    seen: set[str] = set()
    for index, record in enumerate(records):
        if set(record) != {"id", "features", "label"}:
            raise ValidationError(
                "INVALID_SCORING_EXAMPLE", f"records[{index}] has missing or unknown fields"
            )
        record_id = record.get("id")
        features = record.get("features")
        label = record.get("label")
        if not isinstance(record_id, str) or RECORD_ID.fullmatch(record_id) is None:
            raise ValidationError("INVALID_SCORING_EXAMPLE", f"records[{index}].id is invalid")
        if record_id in seen:
            raise ValidationError("DUPLICATE_RECORD_ID", f"duplicate record id {record_id!r}")
        seen.add(record_id)
        if not isinstance(features, Mapping):
            raise ValidationError(
                "INVALID_SCORING_EXAMPLE", f"records[{index}].features must be an object"
            )
        if isinstance(label, bool) or not isinstance(label, (Decimal, int, float)):
            raise ValidationError(
                "INVALID_SCORING_EXAMPLE", f"records[{index}].label must be a number"
            )
        parsed.append((record_id, cast(Mapping[str, object], features), label))
    return tuple(sorted(parsed, key=lambda item: item[0]))


def evaluate_payload(
    payload: LinearEvaluator | Mapping[str, object],
    records: Sequence[Mapping[str, object]],
    metric_specs: Mapping[str, object],
) -> dict[str, Decimal]:
    """Evaluate one strict research-only linear payload over deterministic rows."""

    evaluator = (
        payload if isinstance(payload, LinearEvaluator) else validate_linear_evaluator(payload)
    )
    ordered = _ordered_records(records)
    labels: list[Number] = []
    predictions: list[Number] = []
    for _, features, label in ordered:
        labels.append(label)
        predictions.append(score_linear_evaluator(evaluator, features))
    return evaluate_predictions(labels, predictions, metric_specs)


def recompute_canonical_result(
    baseline_metrics: Mapping[str, Number],
    candidate_metrics: Mapping[str, Number],
    metric_specs: Mapping[str, object],
) -> CanonicalResult:
    """Recompute deltas and every benchmark gate; never trust producer arithmetic."""

    definitions = parse_metric_specs(metric_specs)
    expected_names = set(definitions)
    if set(baseline_metrics) != expected_names or set(candidate_metrics) != expected_names:
        raise ValidationError(
            "METRIC_SET_MISMATCH", "metric value keys must exactly match the benchmark"
        )

    results: dict[str, MetricResult] = {}
    failed: list[str] = []
    for name, definition in definitions.items():
        baseline = quantize_decimal(_decimal(baseline_metrics[name], field=f"baseline.{name}"))
        candidate = quantize_decimal(_decimal(candidate_metrics[name], field=f"candidate.{name}"))
        delta = quantize_decimal(candidate - baseline)
        improvement = delta if definition.direction == "higher" else -delta
        absolute_passed = (
            candidate >= definition.absolute_threshold
            if definition.direction == "higher"
            else candidate <= definition.absolute_threshold
        )
        passed = absolute_passed and improvement >= definition.minimum_delta
        results[name] = {
            "baseline": decimal_string(baseline),
            "candidate": decimal_string(candidate),
            "candidate_minus_baseline": decimal_string(delta),
            "passed": passed,
        }
        if not passed:
            failed.append(f"{name.upper()}_GATE_FAILED")

    all_gates_passed = not failed
    decision: Decision = "KEEP" if all_gates_passed else "DISCARD"
    return {
        "metrics": results,
        "all_gates_passed": all_gates_passed,
        "decision": decision,
        "reason_codes": ["ALL_GATES_PASSED"] if all_gates_passed else failed,
    }


def evaluate_experiment(
    baseline_payload: LinearEvaluator | Mapping[str, object],
    candidate_payload: LinearEvaluator | Mapping[str, object],
    records: Sequence[Mapping[str, object]],
    metric_specs: Mapping[str, object],
) -> CanonicalResult:
    """Evaluate parent/candidate payloads and produce the terminal gate result."""

    baseline = evaluate_payload(baseline_payload, records, metric_specs)
    candidate = evaluate_payload(candidate_payload, records, metric_specs)
    return recompute_canonical_result(baseline, candidate, metric_specs)


__all__ = [
    "METRIC_DIRECTIONS",
    "METRIC_PARAMETERS",
    "CanonicalResult",
    "Decision",
    "MetricDefinition",
    "MetricDirection",
    "MetricResult",
    "evaluate_experiment",
    "evaluate_payload",
    "evaluate_predictions",
    "parse_metric_specs",
    "recompute_canonical_result",
]

# SPDX-License-Identifier: MIT
from __future__ import annotations

from decimal import Decimal

import pytest

from atlas_research.constants import LINEAR_EVALUATOR_SCHEMA
from atlas_research.errors import ValidationError
from atlas_research.evaluation import (
    evaluate_experiment,
    evaluate_payload,
    evaluate_predictions,
    parse_metric_specs,
    recompute_canonical_result,
)


def metric(
    direction: str,
    absolute_threshold: int | float,
    minimum_delta: int | float,
    *,
    parameters: dict[str, object] | None = None,
) -> dict[str, object]:
    value: dict[str, object] = {
        "direction": direction,
        "gate": {
            "absolute_threshold": absolute_threshold,
            "minimum_delta": minimum_delta,
        },
    }
    if parameters is not None:
        value["parameters"] = parameters
    return value


def test_recompute_canonical_result_normalizes_delta_and_gates() -> None:
    specs = {
        "mae": metric("lower", 3, 0.5),
        "spearman": metric("higher", 0.9, 0.1),
    }
    result = recompute_canonical_result(
        {"mae": Decimal("4.0000000000004"), "spearman": Decimal("0.8")},
        {"mae": Decimal("2"), "spearman": Decimal("0.95")},
        specs,
    )
    assert result == {
        "metrics": {
            "mae": {
                "baseline": "4",
                "candidate": "2",
                "candidate_minus_baseline": "-2",
                "passed": True,
            },
            "spearman": {
                "baseline": "0.8",
                "candidate": "0.95",
                "candidate_minus_baseline": "0.15",
                "passed": True,
            },
        },
        "all_gates_passed": True,
        "decision": "KEEP",
        "reason_codes": ["ALL_GATES_PASSED"],
    }


def test_recompute_canonical_result_discards_on_any_failed_gate() -> None:
    specs = {
        "mae": metric("lower", 3, 1),
        "f1": metric("higher", 0.8, 0.1, parameters={"threshold": 60}),
    }
    result = recompute_canonical_result({"mae": 3, "f1": 0.75}, {"mae": 2.5, "f1": 0.8}, specs)
    assert result["decision"] == "DISCARD"
    assert result["all_gates_passed"] is False
    assert result["reason_codes"] == ["MAE_GATE_FAILED", "F1_GATE_FAILED"]


def test_metric_map_has_fixed_directions_parameters_and_keys() -> None:
    with pytest.raises(ValidationError) as raised:
        parse_metric_specs({"mae": metric("higher", 1, 0)})
    assert raised.value.code == "METRIC_DIRECTION_MISMATCH"
    with pytest.raises(ValidationError) as raised:
        parse_metric_specs({"mae": metric("lower", 1, 0, parameters={"threshold": 50})})
    assert raised.value.code == "METRIC_PARAMETER_MISMATCH"
    with pytest.raises(ValidationError) as raised:
        recompute_canonical_result({"mae": 1}, {"mae": 1, "f1": 1}, {"mae": metric("lower", 1, 0)})
    assert raised.value.code == "METRIC_SET_MISMATCH"


def test_evaluate_predictions_computes_every_declared_metric() -> None:
    specs = {
        "mae": metric("lower", 100, 0),
        "spearman": metric("higher", -1, 0),
        "pairwise_accuracy": metric("higher", 0, 0, parameters={"max_pairs": 17}),
        "ndcg_at_10": metric("higher", 0, 0),
        "ndcg_at_50": metric("higher", 0, 0),
        "f1": metric("higher", 0, 0, parameters={"threshold": 50}),
        "calibration_error": metric("lower", 1, 0, parameters={"bins": 10}),
    }
    values = evaluate_predictions([0, 50, 100], [0, 50, 100], specs)
    assert set(values) == set(specs)
    assert values["mae"] == Decimal("0E-12")
    assert values["spearman"] == Decimal("1.000000000000")
    assert values["pairwise_accuracy"] == Decimal("1.000000000000")


def linear(*, weight: int) -> dict[str, object]:
    return {
        "schema": LINEAR_EVALUATOR_SCHEMA,
        "bias": 0,
        "weights": {"quality": weight},
    }


def records() -> list[dict[str, object]]:
    return [
        {"id": "b", "features": {"quality": 20}, "label": 40},
        {"id": "a", "features": {"quality": 10}, "label": 20},
        {"id": "c", "features": {"quality": 30}, "label": 60},
    ]


def test_payload_and_experiment_evaluation_are_research_only_and_deterministic() -> None:
    specs = {"mae": metric("lower", 0, 1)}
    baseline = evaluate_payload(linear(weight=1), records(), specs)
    assert baseline == {"mae": Decimal("20.000000000000")}
    result = evaluate_experiment(linear(weight=1), linear(weight=2), records(), specs)
    assert result["decision"] == "KEEP"
    assert result["metrics"]["mae"]["candidate"] == "0"
    assert result["metrics"]["mae"]["candidate_minus_baseline"] == "-20"


def test_payload_evaluation_rejects_duplicate_records() -> None:
    duplicate = records()
    duplicate[1]["id"] = "b"
    with pytest.raises(ValidationError) as raised:
        evaluate_payload(linear(weight=1), duplicate, {"mae": metric("lower", 100, 0)})
    assert raised.value.code == "DUPLICATE_RECORD_ID"

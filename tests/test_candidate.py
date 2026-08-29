# SPDX-License-Identifier: MIT
from __future__ import annotations

from decimal import Decimal

import pytest

from atlas_research.candidate import (
    score_linear_evaluator,
    validate_linear_evaluator,
    verify_candidate_change,
)
from atlas_research.constants import LINEAR_EVALUATOR_SCHEMA
from atlas_research.errors import ValidationError


def payload(*, bias: int | float = 10, quality: int | float = 2) -> dict[str, object]:
    return {
        "schema": LINEAR_EVALUATOR_SCHEMA,
        "bias": bias,
        "weights": {"quality": quality, "activity.recent": 1},
    }


def candidate(path: str, old: int | float, new: int | float) -> dict[str, object]:
    return {
        "changed_variable": {"path": path, "old_value": old, "new_value": new},
        "target_contract": {"id": "opaque:scout:provenance", "version": "999"},
    }


def test_linear_evaluator_is_strict_research_only_and_clamped() -> None:
    evaluator = validate_linear_evaluator(payload())
    assert evaluator.bias == Decimal(10)
    assert list(evaluator.weights) == ["activity.recent", "quality"]
    assert score_linear_evaluator(evaluator, {"quality": 4, "activity.recent": 2}) == Decimal(
        "20.000000000000"
    )
    assert score_linear_evaluator(
        payload(bias=100), {"quality": 100, "activity.recent": 1}
    ) == Decimal("100.000000000000")
    assert score_linear_evaluator(
        payload(bias=-100, quality=-2), {"quality": 100, "activity.recent": 1}
    ) == Decimal("0E-12")


def test_linear_evaluator_rejects_unknown_or_unsafe_shape() -> None:
    extra = payload()
    extra["production_activation"] = True
    with pytest.raises(ValidationError, match="exactly"):
        validate_linear_evaluator(extra)
    wrong_schema = payload()
    wrong_schema["schema"] = "urn:atlasrepo:scout:scoring"
    with pytest.raises(ValidationError) as raised:
        validate_linear_evaluator(wrong_schema)
    assert raised.value.code == "UNKNOWN_EVALUATOR_SCHEMA"
    boolean_weight = payload()
    boolean_weight["weights"] = {"quality": True}
    with pytest.raises(ValidationError):
        validate_linear_evaluator(boolean_weight)


def test_candidate_verifier_proves_exactly_one_weight_change() -> None:
    parent = payload()
    proposed = payload(quality=3)
    verified = verify_candidate_change(candidate("weights.quality", 2, 3), parent, proposed)
    assert verified.path == "weights.quality"
    assert verified.old_decimal == Decimal(2)
    assert verified.new_decimal == Decimal(3)


def test_candidate_target_contract_is_only_opaque_provenance() -> None:
    parent = payload()
    proposed = payload(bias=11)
    verified = verify_candidate_change(candidate("bias", 10, 11), parent, proposed)
    assert verified.path == "bias"


def test_candidate_rejects_hidden_second_change_and_structure_drift() -> None:
    parent = payload()
    proposed = payload(bias=11, quality=3)
    with pytest.raises(ValidationError) as raised:
        verify_candidate_change(candidate("bias", 10, 11), parent, proposed)
    assert raised.value.code == "CANDIDATE_CHANGE_COUNT"

    added = payload(quality=3)
    assert isinstance(added["weights"], dict)
    added["weights"]["new_feature"] = 1  # type: ignore[index]
    with pytest.raises(ValidationError) as raised:
        verify_candidate_change(candidate("weights.quality", 2, 3), parent, added)
    assert raised.value.code == "CANDIDATE_STRUCTURE_CHANGE"


def test_candidate_rejects_type_path_value_and_schema_mismatches() -> None:
    parent = payload()
    proposed_type = payload(quality=3.0)
    with pytest.raises(ValidationError) as raised:
        verify_candidate_change(candidate("weights.quality", 2, 3.0), parent, proposed_type)
    assert raised.value.code == "CANDIDATE_TYPE_CHANGE"

    proposed = payload(quality=3)
    with pytest.raises(ValidationError) as raised:
        verify_candidate_change(candidate("bias", 10, 11), parent, proposed)
    assert raised.value.code == "CANDIDATE_PATH_MISMATCH"
    with pytest.raises(ValidationError) as raised:
        verify_candidate_change(candidate("weights.quality", 999, 3), parent, proposed)
    assert raised.value.code == "CANDIDATE_OLD_VALUE_MISMATCH"
    with pytest.raises(ValidationError) as raised:
        verify_candidate_change(
            candidate("weights.quality", 2, 3),
            parent,
            proposed,
            evaluation_schema_id="urn:unknown",
        )
    assert raised.value.code == "UNKNOWN_EVALUATOR_SCHEMA"

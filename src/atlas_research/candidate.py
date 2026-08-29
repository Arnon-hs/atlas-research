# SPDX-License-Identifier: MIT
"""Strict research-only candidate and synthetic evaluator validation.

The bundled linear evaluator is an offline fixture. It is deliberately not a
Scout scoring definition and this module provides no activation or import path.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext
from types import MappingProxyType
from typing import TypeAlias

from .canonical import quantize_decimal
from .constants import LINEAR_EVALUATOR_SCHEMA, MAX_FEATURES
from .errors import ValidationError

JsonScalar: TypeAlias = str | int | float | Decimal | bool | None

FEATURE_NAME = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
BIAS_MIN = Decimal("-100")
BIAS_MAX = Decimal("100")
WEIGHT_MIN = Decimal("-100")
WEIGHT_MAX = Decimal("100")
FEATURE_VALUE_MIN = Decimal("-1000000")
FEATURE_VALUE_MAX = Decimal("1000000")


@dataclass(frozen=True, slots=True)
class LinearEvaluator:
    """Validated research-only ``bias + sum(weight * feature)`` fixture."""

    bias: Decimal
    weights: Mapping[str, Decimal]
    schema: str = LINEAR_EVALUATOR_SCHEMA


@dataclass(frozen=True, slots=True)
class VerifiedCandidateChange:
    """Evidence that the proposed payload differs by exactly one scalar leaf."""

    path: str
    old_value: JsonScalar
    new_value: JsonScalar
    old_decimal: Decimal
    new_decimal: Decimal


def _finite_decimal(
    value: object,
    *,
    field: str,
    minimum: Decimal,
    maximum: Decimal,
) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValidationError("INVALID_LINEAR_EVALUATOR", f"{field} must be a number")
    try:
        converted = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValidationError(
            "INVALID_LINEAR_EVALUATOR", f"{field} must be a finite number"
        ) from exc
    if not converted.is_finite() or not minimum <= converted <= maximum:
        raise ValidationError(
            "INVALID_LINEAR_EVALUATOR",
            f"{field} must be finite and between {minimum} and {maximum}",
        )
    return converted


def validate_linear_evaluator(payload: Mapping[str, object]) -> LinearEvaluator:
    """Validate the exact bundled linear-fixture shape and return immutable values."""

    if set(payload) != {"schema", "bias", "weights"}:
        raise ValidationError(
            "INVALID_LINEAR_EVALUATOR",
            "linear evaluator must contain exactly schema, bias, and weights",
        )
    if payload.get("schema") != LINEAR_EVALUATOR_SCHEMA:
        raise ValidationError(
            "UNKNOWN_EVALUATOR_SCHEMA", "linear evaluator schema is not allowlisted"
        )
    bias = _finite_decimal(payload.get("bias"), field="bias", minimum=BIAS_MIN, maximum=BIAS_MAX)
    raw_weights = payload.get("weights")
    if not isinstance(raw_weights, Mapping):
        raise ValidationError("INVALID_LINEAR_EVALUATOR", "weights must be an object")
    if not 1 <= len(raw_weights) <= MAX_FEATURES:
        raise ValidationError(
            "INVALID_LINEAR_EVALUATOR", f"weights must contain between 1 and {MAX_FEATURES} entries"
        )

    weights: dict[str, Decimal] = {}
    for key, value in raw_weights.items():
        if not isinstance(key, str) or FEATURE_NAME.fullmatch(key) is None:
            raise ValidationError(
                "INVALID_LINEAR_EVALUATOR", "weight names must use the feature-name grammar"
            )
        weights[key] = _finite_decimal(
            value,
            field=f"weights.{key}",
            minimum=WEIGHT_MIN,
            maximum=WEIGHT_MAX,
        )
    return LinearEvaluator(bias=bias, weights=MappingProxyType(dict(sorted(weights.items()))))


def score_linear_evaluator(
    evaluator: LinearEvaluator | Mapping[str, object], features: Mapping[str, object]
) -> Decimal:
    """Evaluate one feature map and clamp the research score to ``[0, 100]``."""

    validated = (
        evaluator
        if isinstance(evaluator, LinearEvaluator)
        else validate_linear_evaluator(evaluator)
    )
    parsed_features: dict[str, Decimal] = {}
    for key, value in features.items():
        if not isinstance(key, str) or FEATURE_NAME.fullmatch(key) is None:
            raise ValidationError(
                "INVALID_SCORING_EXAMPLE", "feature names must use the feature-name grammar"
            )
        parsed_features[key] = _finite_decimal(
            value,
            field=f"features.{key}",
            minimum=FEATURE_VALUE_MIN,
            maximum=FEATURE_VALUE_MAX,
        )
    if not 1 <= len(parsed_features) <= MAX_FEATURES:
        raise ValidationError(
            "INVALID_SCORING_EXAMPLE",
            f"features must contain between 1 and {MAX_FEATURES} entries",
        )

    with localcontext() as context:
        context.prec = 50
        context.rounding = ROUND_HALF_EVEN
        score = validated.bias + sum(
            (
                weight * parsed_features.get(feature_name, Decimal(0))
                for feature_name, weight in validated.weights.items()
            ),
            Decimal(0),
        )
        return quantize_decimal(max(Decimal(0), min(Decimal(100), score)))


def _target_contract_is_provenance(candidate: Mapping[str, object]) -> None:
    target = candidate.get("target_contract")
    if not isinstance(target, Mapping) or set(target) != {"id", "version"}:
        raise ValidationError(
            "INVALID_TARGET_CONTRACT", "target_contract must contain exactly id and version"
        )
    identifier = target.get("id")
    version = target.get("version")
    if not isinstance(identifier, str) or not identifier or len(identifier) > 512:
        raise ValidationError("INVALID_TARGET_CONTRACT", "target_contract.id is invalid")
    if not isinstance(version, str) or not version or len(version) > 64:
        raise ValidationError("INVALID_TARGET_CONTRACT", "target_contract.version is invalid")
    # Deliberately do not select an evaluator or infer compatibility from these
    # opaque provenance strings.


def _changed_variable(candidate: Mapping[str, object]) -> tuple[str, JsonScalar, JsonScalar]:
    changed = candidate.get("changed_variable")
    if not isinstance(changed, Mapping) or set(changed) != {"path", "old_value", "new_value"}:
        raise ValidationError(
            "INVALID_CHANGED_VARIABLE",
            "changed_variable must contain exactly path, old_value, and new_value",
        )
    path = changed.get("path")
    old_value = changed.get("old_value")
    new_value = changed.get("new_value")
    if not isinstance(path, str):
        raise ValidationError("INVALID_CHANGED_VARIABLE", "changed_variable.path is invalid")
    if isinstance(old_value, bool) or not isinstance(old_value, (int, float, Decimal)):
        raise ValidationError("INVALID_CHANGED_VARIABLE", "old_value must be a numeric scalar")
    if isinstance(new_value, bool) or not isinstance(new_value, (int, float, Decimal)):
        raise ValidationError("INVALID_CHANGED_VARIABLE", "new_value must be a numeric scalar")
    if type(old_value) is not type(new_value):
        raise ValidationError(
            "CANDIDATE_TYPE_CHANGE", "changed values must have the same JSON scalar type"
        )
    return path, old_value, new_value


def _raw_leaves(payload: Mapping[str, object]) -> dict[str, object]:
    raw_weights = payload["weights"]
    if not isinstance(raw_weights, Mapping):  # pragma: no cover - validated caller invariant
        raise ValidationError("INVALID_LINEAR_EVALUATOR", "weights must be an object")
    leaves: dict[str, object] = {"bias": payload["bias"]}
    leaves.update({f"weights.{key}": value for key, value in raw_weights.items()})
    return leaves


def _json_number_equal(left: object, right: object) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, bool) or not isinstance(left, (int, float, Decimal)):
        return left == right
    return Decimal(str(left)) == Decimal(str(right))


def verify_candidate_change(
    candidate: Mapping[str, object],
    parent_payload: Mapping[str, object],
    proposed_payload: Mapping[str, object],
    *,
    evaluation_schema_id: str = LINEAR_EVALUATOR_SCHEMA,
) -> VerifiedCandidateChange:
    """Prove exactly one allowlisted scalar change and no hidden payload drift.

    ``target_contract`` is checked only as opaque provenance. Evaluator
    selection is driven exclusively by the separately verified artifact schema
    identity supplied in ``evaluation_schema_id``.
    """

    if evaluation_schema_id != LINEAR_EVALUATOR_SCHEMA:
        raise ValidationError(
            "UNKNOWN_EVALUATOR_SCHEMA", "candidate evaluator schema is not allowlisted"
        )
    _target_contract_is_provenance(candidate)
    validate_linear_evaluator(parent_payload)
    validate_linear_evaluator(proposed_payload)
    path, declared_old, declared_new = _changed_variable(candidate)

    parent_leaves = _raw_leaves(parent_payload)
    proposed_leaves = _raw_leaves(proposed_payload)
    if parent_leaves.keys() != proposed_leaves.keys():
        raise ValidationError(
            "CANDIDATE_STRUCTURE_CHANGE", "candidate cannot add or remove evaluator variables"
        )

    differences: list[str] = []
    for leaf_path in sorted(parent_leaves):
        parent_value = parent_leaves[leaf_path]
        proposed_value = proposed_leaves[leaf_path]
        if type(parent_value) is not type(proposed_value):
            raise ValidationError(
                "CANDIDATE_TYPE_CHANGE", f"candidate changes scalar type at {leaf_path}"
            )
        if not _json_number_equal(parent_value, proposed_value):
            differences.append(leaf_path)
    if len(differences) != 1:
        raise ValidationError(
            "CANDIDATE_CHANGE_COUNT",
            f"candidate must change exactly one scalar; observed {len(differences)}",
        )
    observed_path = differences[0]
    if path != observed_path:
        raise ValidationError(
            "CANDIDATE_PATH_MISMATCH",
            f"declared path {path!r} does not match observed path {observed_path!r}",
        )

    observed_old = parent_leaves[path]
    observed_new = proposed_leaves[path]
    if type(declared_old) is not type(observed_old) or not _json_number_equal(
        declared_old, observed_old
    ):
        raise ValidationError(
            "CANDIDATE_OLD_VALUE_MISMATCH", "declared old_value does not match parent payload"
        )
    if type(declared_new) is not type(observed_new) or not _json_number_equal(
        declared_new, observed_new
    ):
        raise ValidationError(
            "CANDIDATE_NEW_VALUE_MISMATCH", "declared new_value does not match proposed payload"
        )

    minimum, maximum = (BIAS_MIN, BIAS_MAX) if path == "bias" else (WEIGHT_MIN, WEIGHT_MAX)
    old_decimal = _finite_decimal(
        declared_old, field=f"changed_variable.{path}.old", minimum=minimum, maximum=maximum
    )
    new_decimal = _finite_decimal(
        declared_new, field=f"changed_variable.{path}.new", minimum=minimum, maximum=maximum
    )
    if old_decimal == new_decimal:
        raise ValidationError(
            "CANDIDATE_ZERO_CHANGE", "changed values are equal after canonical comparison"
        )
    return VerifiedCandidateChange(
        path=path,
        old_value=declared_old,
        new_value=declared_new,
        old_decimal=old_decimal,
        new_decimal=new_decimal,
    )


__all__ = [
    "BIAS_MAX",
    "BIAS_MIN",
    "FEATURE_VALUE_MAX",
    "FEATURE_VALUE_MIN",
    "WEIGHT_MAX",
    "WEIGHT_MIN",
    "LinearEvaluator",
    "VerifiedCandidateChange",
    "score_linear_evaluator",
    "validate_linear_evaluator",
    "verify_candidate_change",
]

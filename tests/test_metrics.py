# SPDX-License-Identifier: MIT
from __future__ import annotations

from decimal import Decimal

import pytest

from atlas_research.errors import ValidationError
from atlas_research.metrics import (
    calibration_error,
    f1,
    mae,
    ndcg,
    normalize_decimal,
    pairwise_accuracy,
    spearman,
)


def test_mae_and_contract_decimal_normalization() -> None:
    assert mae([0, 10], [1, 7]) == Decimal("2.000000000000")
    assert normalize_decimal(Decimal("1.2345678901235")) == "1.234567890124"
    assert normalize_decimal(Decimal("-0.0000000000004")) == "0"
    assert normalize_decimal(Decimal("100.000000000000")) == "100"


def test_spearman_uses_average_ranks_for_ties() -> None:
    assert spearman([1, 2, 2, 3], [10, 20, 20, 30]) == Decimal("1.000000000000")
    assert spearman([1, 2, 2, 3], [30, 20, 20, 10]) == Decimal("-1.000000000000")
    assert spearman([1, 1, 1], [1, 2, 3]) == Decimal(0)


def test_pairwise_accuracy_is_bounded_deterministic_and_tie_aware() -> None:
    labels = list(range(500))
    assert pairwise_accuracy(labels, labels, max_pairs=37) == Decimal("1.000000000000")
    assert pairwise_accuracy([1, 2, 3], [3, 2, 1]) == Decimal("0E-12")
    assert pairwise_accuracy([1, 2, 3], [0, 0, 0]) == Decimal("0.500000000000")
    assert pairwise_accuracy([1, 1], [0, 100]) == Decimal(0)


def test_ndcg_uses_deterministic_stable_tie_breaking() -> None:
    labels = [100, 50, 0]
    assert ndcg(labels, [100, 50, 0], k=10) == Decimal("1.000000000000")
    degraded = ndcg(labels, [0, 50, 100], k=10)
    assert Decimal(0) < degraded < Decimal(1)
    assert ndcg([0, 0], [100, 0], k=10) == Decimal(0)


def test_f1_and_calibration_error() -> None:
    assert f1([0, 100, 100], [0, 0, 100], threshold=50) == Decimal("0.666666666667")
    assert f1([0, 0], [0, 0], threshold=50) == Decimal(0)
    assert calibration_error([0, 100], [0, 100], bins=10) == Decimal("0E-12")
    assert calibration_error([0, 100], [0, 0], bins=10) == Decimal("0.500000000000")


@pytest.mark.parametrize(
    ("call", "code"),
    [
        (lambda: mae([], []), "EMPTY_METRIC_INPUT"),
        (lambda: mae([1], [1, 2]), "METRIC_LENGTH_MISMATCH"),
        (lambda: mae([True], [1]), "INVALID_METRIC_INPUT"),
        (lambda: pairwise_accuracy([1, 2], [1, 2], max_pairs=50_001), "INVALID_MAX_PAIRS"),
        (lambda: ndcg([-1], [0], k=10), "INVALID_RELEVANCE"),
        (lambda: f1([0], [0], threshold=101), "INVALID_THRESHOLD"),
        (lambda: calibration_error([0], [0], bins=1), "INVALID_BINS"),
    ],
)
def test_metric_input_validation(call: object, code: str) -> None:
    with pytest.raises(ValidationError) as raised:
        call()  # type: ignore[operator]
    assert raised.value.code == code

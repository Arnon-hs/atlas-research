# SPDX-License-Identifier: MIT
"""Deterministic, decimal offline-evaluation metrics.

The functions in this module operate only on already-exported research data.
They do not implement or mirror Scout production scoring.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext
from typing import TypeAlias

from .canonical import decimal_string, quantize_decimal
from .errors import ValidationError

Number: TypeAlias = Decimal | int | float

DECIMAL_PRECISION = 50
DECIMAL_QUANTUM = Decimal("0.000000000001")
MAX_PAIRWISE_PAIRS = 50_000


def _number(value: object, *, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float)):
        raise ValidationError("INVALID_METRIC_INPUT", f"{field} must be a finite number")
    try:
        converted = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValidationError("INVALID_METRIC_INPUT", f"{field} must be a finite number") from exc
    if not converted.is_finite():
        raise ValidationError("INVALID_METRIC_INPUT", f"{field} must be a finite number")
    return converted


def _paired(
    labels: Sequence[Number], predictions: Sequence[Number]
) -> tuple[tuple[Decimal, ...], tuple[Decimal, ...]]:
    if len(labels) != len(predictions):
        raise ValidationError(
            "METRIC_LENGTH_MISMATCH", "labels and predictions must have equal length"
        )
    if not labels:
        raise ValidationError("EMPTY_METRIC_INPUT", "at least one record is required")
    return (
        tuple(_number(value, field=f"labels[{index}]") for index, value in enumerate(labels)),
        tuple(
            _number(value, field=f"predictions[{index}]") for index, value in enumerate(predictions)
        ),
    )


def normalize_decimal(value: Number) -> str:
    """Return the contract's single normalized 12-place decimal spelling."""

    return decimal_string(_number(value, field="value"))


def mae(labels: Sequence[Number], predictions: Sequence[Number]) -> Decimal:
    """Return mean absolute error, quantized to 12 fractional places."""

    actual, predicted = _paired(labels, predictions)
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        context.rounding = ROUND_HALF_EVEN
        value = sum(
            (abs(left - right) for left, right in zip(actual, predicted, strict=True)),
            Decimal(0),
        )
        return quantize_decimal(value / Decimal(len(actual)))


def _average_ranks(values: Sequence[Decimal]) -> tuple[Decimal, ...]:
    order = sorted(range(len(values)), key=lambda index: (values[index], index))
    ranks = [Decimal(0)] * len(values)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[end]] == values[order[start]]:
            end += 1
        # Ranks are one-based. The average of [start + 1, end] is exact.
        rank = (Decimal(start + 1) + Decimal(end)) / Decimal(2)
        for position in range(start, end):
            ranks[order[position]] = rank
        start = end
    return tuple(ranks)


def spearman(labels: Sequence[Number], predictions: Sequence[Number]) -> Decimal:
    """Return Spearman rank correlation with deterministic average tie ranks.

    A constant rank vector has no defined correlation. The research contract
    assigns it the conservative deterministic value zero.
    """

    actual, predicted = _paired(labels, predictions)
    if len(actual) < 2:
        return Decimal(0)
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        context.rounding = ROUND_HALF_EVEN
        left_ranks = _average_ranks(actual)
        right_ranks = _average_ranks(predicted)
        count = Decimal(len(actual))
        left_mean = sum(left_ranks, Decimal(0)) / count
        right_mean = sum(right_ranks, Decimal(0)) / count
        covariance = sum(
            (
                (left - left_mean) * (right - right_mean)
                for left, right in zip(left_ranks, right_ranks, strict=True)
            ),
            Decimal(0),
        )
        left_variance = sum(((value - left_mean) ** 2 for value in left_ranks), Decimal(0))
        right_variance = sum(((value - right_mean) ** 2 for value in right_ranks), Decimal(0))
        if left_variance == 0 or right_variance == 0:
            return Decimal(0)
        value = covariance / (left_variance * right_variance).sqrt(context)
        # Guard against a one-ulp context overshoot without hiding real errors.
        value = max(Decimal(-1), min(Decimal(1), value))
        return quantize_decimal(value)


def _pairs_before(index: int, record_count: int) -> int:
    return index * (2 * record_count - index - 1) // 2


def _unrank_pair(pair_index: int, record_count: int) -> tuple[int, int]:
    """Map a lexicographic combination index to one ``(left, right)`` pair."""

    low = 0
    high = record_count - 1
    while low < high:
        middle = (low + high + 1) // 2
        if _pairs_before(middle, record_count) <= pair_index:
            low = middle
        else:
            high = middle - 1
    left = low
    right = left + 1 + pair_index - _pairs_before(left, record_count)
    return left, right


def _sampled_pair_indices(record_count: int, max_pairs: int) -> tuple[int, ...]:
    total = record_count * (record_count - 1) // 2
    if total <= max_pairs:
        return tuple(range(total))
    # Midpoints of equal-width integer intervals cover the pair space without a
    # PRNG and remain stable on every Python/platform version.
    return tuple(((2 * slot + 1) * total) // (2 * max_pairs) for slot in range(max_pairs))


def pairwise_accuracy(
    labels: Sequence[Number],
    predictions: Sequence[Number],
    *,
    max_pairs: int = MAX_PAIRWISE_PAIRS,
) -> Decimal:
    """Return bounded deterministic pairwise ranking accuracy.

    True-label ties are excluded. A prediction tie for a comparable pair earns
    one half. At most 50,000 positions from the complete pair space are read.
    """

    actual, predicted = _paired(labels, predictions)
    if isinstance(max_pairs, bool) or not isinstance(max_pairs, int):
        raise ValidationError("INVALID_MAX_PAIRS", "max_pairs must be an integer")
    if not 1 <= max_pairs <= MAX_PAIRWISE_PAIRS:
        raise ValidationError("INVALID_MAX_PAIRS", "max_pairs must be between 1 and 50000")
    if len(actual) < 2:
        return Decimal(0)

    correct = Decimal(0)
    comparable = 0
    for pair_index in _sampled_pair_indices(len(actual), max_pairs):
        left, right = _unrank_pair(pair_index, len(actual))
        expected = actual[left].compare(actual[right])
        if expected == 0:
            continue
        observed = predicted[left].compare(predicted[right])
        comparable += 1
        if observed == expected:
            correct += 1
        elif observed == 0:
            correct += Decimal("0.5")
    if comparable == 0:
        return Decimal(0)
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        context.rounding = ROUND_HALF_EVEN
        return quantize_decimal(correct / Decimal(comparable))


def _dcg(relevances: Sequence[Decimal]) -> Decimal:
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        context.rounding = ROUND_HALF_EVEN
        logarithm_two = Decimal(2).ln(context)
        return sum(
            (
                relevance * logarithm_two / Decimal(rank + 1).ln(context)
                for rank, relevance in enumerate(relevances, start=1)
            ),
            Decimal(0),
        )


def ndcg(labels: Sequence[Number], predictions: Sequence[Number], *, k: int) -> Decimal:
    """Return normalized discounted cumulative gain using linear relevance."""

    actual, predicted = _paired(labels, predictions)
    if isinstance(k, bool) or not isinstance(k, int) or k < 1:
        raise ValidationError("INVALID_NDCG_K", "k must be a positive integer")
    if any(value < 0 for value in actual):
        raise ValidationError("INVALID_RELEVANCE", "NDCG labels must be non-negative")
    limit = min(k, len(actual))
    predicted_order = sorted(range(len(actual)), key=lambda index: (-predicted[index], index))
    ideal_order = sorted(range(len(actual)), key=lambda index: (-actual[index], index))
    observed = _dcg(tuple(actual[index] for index in predicted_order[:limit]))
    ideal = _dcg(tuple(actual[index] for index in ideal_order[:limit]))
    if ideal == 0:
        return Decimal(0)
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        context.rounding = ROUND_HALF_EVEN
        value = max(Decimal(0), min(Decimal(1), observed / ideal))
        return quantize_decimal(value)


def f1(
    labels: Sequence[Number],
    predictions: Sequence[Number],
    *,
    threshold: Number = Decimal(50),
) -> Decimal:
    """Return binary F1 after applying one score threshold to both vectors."""

    actual, predicted = _paired(labels, predictions)
    boundary = _number(threshold, field="threshold")
    if not Decimal(0) <= boundary <= Decimal(100):
        raise ValidationError("INVALID_THRESHOLD", "threshold must be between 0 and 100")
    true_positive = 0
    false_positive = 0
    false_negative = 0
    for expected, observed in zip(actual, predicted, strict=True):
        expected_positive = expected >= boundary
        observed_positive = observed >= boundary
        if expected_positive and observed_positive:
            true_positive += 1
        elif observed_positive:
            false_positive += 1
        elif expected_positive:
            false_negative += 1
    denominator = 2 * true_positive + false_positive + false_negative
    if denominator == 0:
        return Decimal(0)
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        context.rounding = ROUND_HALF_EVEN
        return quantize_decimal(Decimal(2 * true_positive) / Decimal(denominator))


def calibration_error(
    labels: Sequence[Number],
    predictions: Sequence[Number],
    *,
    bins: int = 10,
) -> Decimal:
    """Return equal-width expected calibration error for scores in ``[0, 100]``."""

    actual, predicted = _paired(labels, predictions)
    if isinstance(bins, bool) or not isinstance(bins, int) or not 2 <= bins <= 50:
        raise ValidationError("INVALID_BINS", "bins must be an integer between 2 and 50")
    if any(not Decimal(0) <= value <= Decimal(100) for value in (*actual, *predicted)):
        raise ValidationError(
            "INVALID_CALIBRATION_SCORE", "calibration labels and predictions must be in [0, 100]"
        )

    counts = [0] * bins
    expected_sums = [Decimal(0)] * bins
    observed_sums = [Decimal(0)] * bins
    for expected, observed in zip(actual, predicted, strict=True):
        index = min(bins - 1, int(observed * bins / Decimal(100)))
        counts[index] += 1
        expected_sums[index] += expected / Decimal(100)
        observed_sums[index] += observed / Decimal(100)

    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        context.rounding = ROUND_HALF_EVEN
        error = Decimal(0)
        total = Decimal(len(actual))
        for count, expected_sum, observed_sum in zip(
            counts, expected_sums, observed_sums, strict=True
        ):
            if count == 0:
                continue
            size = Decimal(count)
            error += size / total * abs(observed_sum / size - expected_sum / size)
        return quantize_decimal(error)


__all__ = [
    "DECIMAL_PRECISION",
    "DECIMAL_QUANTUM",
    "MAX_PAIRWISE_PAIRS",
    "Number",
    "calibration_error",
    "f1",
    "mae",
    "ndcg",
    "normalize_decimal",
    "pairwise_accuracy",
    "spearman",
]

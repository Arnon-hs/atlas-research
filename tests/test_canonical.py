# SPDX-License-Identifier: MIT
from __future__ import annotations

from decimal import Decimal

import pytest

from atlas_research.canonical import (
    canonical_json_bytes,
    canonical_sha256,
    decimal_string,
    quantize_decimal,
    strict_json_loads,
)
from atlas_research.errors import ResourceLimitError, ValidationError


def test_strict_json_rejects_duplicate_nonfinite_and_invalid_utf8() -> None:
    with pytest.raises(ValidationError, match="JSON_DUPLICATE_KEY"):
        strict_json_loads(b'{"a":1,"a":2}')
    with pytest.raises(ValidationError, match="JSON_NONFINITE_NUMBER"):
        strict_json_loads(b'{"a":NaN}')
    with pytest.raises(ValidationError, match="JSON_INVALID_UTF8"):
        strict_json_loads(b'"\xff"')


def test_strict_json_enforces_bytes_depth_and_utf8_string_limits() -> None:
    with pytest.raises(ResourceLimitError, match="JSON_BYTES_EXCEEDED"):
        strict_json_loads(b'{"a":1}', max_bytes=6)
    with pytest.raises(ResourceLimitError, match="JSON_DEPTH_EXCEEDED"):
        strict_json_loads(b'{"a":{"b":1}}', max_depth=1)
    with pytest.raises(ResourceLimitError, match="JSON_STRING_TOO_LONG"):
        strict_json_loads('"éé"', max_string_bytes=3)


def test_canonical_json_uses_utf16_key_order_and_normalized_numbers() -> None:
    value = {
        "\ue000": 1,
        "\U00010000": 2,
        "small": Decimal("0.000001"),
        "negative_zero": -0.0,
        "large": Decimal("1e20"),
    }
    assert canonical_json_bytes(value) == (
        b'{"large":100000000000000000000,"negative_zero":0,"small":0.000001,'
        b'"\xf0\x90\x80\x80":2,"\xee\x80\x80":1}'
    )
    assert canonical_sha256(value) == canonical_sha256(dict(reversed(list(value.items()))))


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0.8, b"0.8"),
        (0.1, b"0.1"),
        (0.000001, b"0.000001"),
        (0.0000001, b"1e-7"),
        (1e20, b"100000000000000000000"),
        (1e21, b"1e+21"),
    ],
)
def test_canonical_float_rendering_matches_jcs_thresholds(value: float, expected: bytes) -> None:
    assert canonical_json_bytes(value) == expected


def test_canonical_json_rejects_cycles_large_integers_and_nonfinite_values() -> None:
    cycle: list[object] = []
    cycle.append(cycle)
    with pytest.raises(ValidationError, match="JSON_CYCLE"):
        canonical_json_bytes(cycle)
    with pytest.raises(ValidationError, match="JSON_INTEGER_OUT_OF_RANGE"):
        canonical_json_bytes(1 << 60)
    with pytest.raises(ValidationError, match="JSON_NONFINITE_NUMBER"):
        canonical_json_bytes(float("inf"))


def test_decimal_normalization_is_half_even_bounded_and_has_one_zero() -> None:
    assert decimal_string(Decimal("1.2300000000004")) == "1.23"
    assert decimal_string(Decimal("1.2345678901235")) == "1.234567890124"
    assert decimal_string(Decimal("-0.0000000000004")) == "0"
    assert quantize_decimal("2.0000000000005") == Decimal("2.000000000000")
    with pytest.raises(ValidationError, match="DECIMAL_NONFINITE"):
        decimal_string("NaN")

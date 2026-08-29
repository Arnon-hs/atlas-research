# SPDX-License-Identifier: MIT
"""Strict JSON parsing and deterministic Atlas canonical JSON v1."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from decimal import (
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    InvalidOperation,
    localcontext,
)
from typing import Final, TypeAlias, cast

from .constants import MAX_JOB_BYTES, MAX_JSON_DEPTH, MAX_STRING_BYTES
from .errors import ResourceLimitError, ValidationError

JSONScalar: TypeAlias = bool | int | Decimal | str | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
DecimalInput: TypeAlias = Decimal | int | float | str

_DECIMAL_CONTEXT: Final = Context(prec=50, rounding=ROUND_HALF_EVEN)
_DECIMAL_QUANTUM: Final = Decimal("0.000000000001")
_MAX_NUMBER_TOKEN_CHARS: Final = 1_024
_MAX_SAFE_INTEGER: Final = (1 << 53) - 1


def _raise_duplicate_key(_pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in _pairs:
        if key in result:
            raise ValidationError("JSON_DUPLICATE_KEY", "JSON object keys must be unique")
        result[key] = value
    return result


def _parse_integer(token: str) -> int:
    if len(token) > _MAX_NUMBER_TOKEN_CHARS:
        raise ResourceLimitError("JSON_NUMBER_TOO_LONG", "JSON number token exceeds the limit")
    return int(token, 10)


def _parse_decimal(token: str) -> Decimal:
    if len(token) > _MAX_NUMBER_TOKEN_CHARS:
        raise ResourceLimitError("JSON_NUMBER_TOO_LONG", "JSON number token exceeds the limit")
    value = Decimal(token)
    if not value.is_finite():
        raise ValidationError("JSON_NONFINITE_NUMBER", "JSON numbers must be finite")
    return value


def _reject_constant(_token: str) -> None:
    raise ValidationError("JSON_NONFINITE_NUMBER", "JSON numbers must be finite")


def _utf8_length(value: str) -> int:
    try:
        return len(value.encode("utf-8", errors="strict"))
    except UnicodeEncodeError as exc:
        raise ValidationError(
            "JSON_INVALID_UNICODE", "JSON strings must contain valid Unicode"
        ) from exc


def _validate_json_tree(
    value: object,
    *,
    max_depth: int,
    max_string_bytes: int,
    depth: int = 0,
    ancestors: set[int] | None = None,
) -> None:
    if isinstance(value, str):
        if _utf8_length(value) > max_string_bytes:
            raise ResourceLimitError("JSON_STRING_TOO_LONG", "JSON string exceeds the byte limit")
        return
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        return
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValidationError("JSON_NONFINITE_NUMBER", "JSON numbers must be finite")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError("JSON_NONFINITE_NUMBER", "JSON numbers must be finite")
        return

    if not isinstance(value, (Mapping, list, tuple)):
        raise ValidationError(
            "JSON_UNSUPPORTED_TYPE", "Value is not representable as canonical JSON"
        )
    if depth >= max_depth:
        raise ResourceLimitError("JSON_DEPTH_EXCEEDED", "JSON nesting exceeds the depth limit")

    current_ancestors = ancestors if ancestors is not None else set()
    identity = id(value)
    if identity in current_ancestors:
        raise ValidationError("JSON_CYCLE", "Canonical JSON cannot contain reference cycles")
    current_ancestors.add(identity)
    try:
        if isinstance(value, Mapping):
            for key, child in value.items():
                if not isinstance(key, str):
                    raise ValidationError("JSON_NON_STRING_KEY", "JSON object keys must be strings")
                if _utf8_length(key) > max_string_bytes:
                    raise ResourceLimitError(
                        "JSON_STRING_TOO_LONG", "JSON string exceeds the byte limit"
                    )
                _validate_json_tree(
                    child,
                    max_depth=max_depth,
                    max_string_bytes=max_string_bytes,
                    depth=depth + 1,
                    ancestors=current_ancestors,
                )
        else:
            for child in value:
                _validate_json_tree(
                    child,
                    max_depth=max_depth,
                    max_string_bytes=max_string_bytes,
                    depth=depth + 1,
                    ancestors=current_ancestors,
                )
    finally:
        current_ancestors.remove(identity)


def strict_json_loads(
    data: bytes | str,
    *,
    max_bytes: int = MAX_JOB_BYTES,
    max_depth: int = MAX_JSON_DEPTH,
    max_string_bytes: int = MAX_STRING_BYTES,
) -> JSONValue:
    """Parse bounded UTF-8 JSON while rejecting ambiguous or unsafe values."""

    if max_bytes < 1 or max_depth < 1 or max_string_bytes < 1:
        raise ValueError("JSON limits must be positive")
    if isinstance(data, bytes):
        if len(data) > max_bytes:
            raise ResourceLimitError("JSON_BYTES_EXCEEDED", "JSON input exceeds the byte limit")
        try:
            text = data.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise ValidationError("JSON_INVALID_UTF8", "JSON input must be valid UTF-8") from exc
    else:
        if _utf8_length(data) > max_bytes:
            raise ResourceLimitError("JSON_BYTES_EXCEEDED", "JSON input exceeds the byte limit")
        text = data

    try:
        parsed = cast(
            object,
            json.loads(
                text,
                object_pairs_hook=_raise_duplicate_key,
                parse_float=_parse_decimal,
                parse_int=_parse_integer,
                parse_constant=_reject_constant,
            ),
        )
    except (ValidationError, ResourceLimitError):
        raise
    except (json.JSONDecodeError, UnicodeError, RecursionError, InvalidOperation) as exc:
        raise ValidationError("JSON_INVALID", "JSON input is invalid") from exc

    _validate_json_tree(
        parsed,
        max_depth=max_depth,
        max_string_bytes=max_string_bytes,
    )
    return cast(JSONValue, parsed)


def _json_string(value: str) -> str:
    _utf8_length(value)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _utf16_sort_key(value: str) -> bytes:
    try:
        return value.encode("utf-16-be", errors="strict")
    except UnicodeEncodeError as exc:
        raise ValidationError(
            "JSON_INVALID_UNICODE", "JSON strings must contain valid Unicode"
        ) from exc


def _float_to_jcs(value: float) -> str:
    if not math.isfinite(value):
        raise ValidationError("JSON_NONFINITE_NUMBER", "JSON numbers must be finite")
    if value == 0.0:
        return "0"

    negative = value < 0
    magnitude = -value if negative else value
    raw = repr(magnitude).lower()
    if "e" in raw:
        mantissa, exponent_text = raw.split("e", 1)
        exponent = int(exponent_text, 10)
    else:
        mantissa = raw
        exponent = 0

    integer_part, dot, fractional_part = mantissa.partition(".")
    untrimmed_digits = integer_part + (fractional_part if dot else "")
    leading_zeroes = len(untrimmed_digits) - len(untrimmed_digits.lstrip("0"))
    digits = untrimmed_digits.lstrip("0") or "0"
    decimal_position = len(integer_part) + exponent - leading_zeroes

    if Decimal("0.000001") <= Decimal(raw) < Decimal("1e21"):
        if decimal_position <= 0:
            rendered = "0." + ("0" * -decimal_position) + digits
        elif decimal_position >= len(digits):
            rendered = digits + ("0" * (decimal_position - len(digits)))
        else:
            rendered = digits[:decimal_position] + "." + digits[decimal_position:]
        if "." in rendered:
            rendered = rendered.rstrip("0").rstrip(".")
    else:
        scientific_exponent = decimal_position - 1
        tail = digits[1:].rstrip("0")
        rendered = digits[0] + (("." + tail) if tail else "")
        sign = "+" if scientific_exponent >= 0 else ""
        rendered += f"e{sign}{scientific_exponent}"
    return ("-" if negative else "") + rendered


def _number_to_canonical(value: int | float | Decimal) -> str:
    if isinstance(value, bool):
        raise AssertionError("booleans are serialized before numeric values")
    if isinstance(value, int):
        if abs(value) > _MAX_SAFE_INTEGER:
            raise ValidationError(
                "JSON_INTEGER_OUT_OF_RANGE", "JSON integer exceeds the interoperable range"
            )
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValidationError("JSON_NONFINITE_NUMBER", "JSON numbers must be finite")
        try:
            float_value = float(value)
        except (OverflowError, ValueError) as exc:
            raise ValidationError(
                "JSON_NUMBER_OUT_OF_RANGE", "JSON number exceeds the interoperable range"
            ) from exc
        if not math.isfinite(float_value):
            raise ValidationError(
                "JSON_NUMBER_OUT_OF_RANGE", "JSON number exceeds the interoperable range"
            )
        return _float_to_jcs(float_value)
    return _float_to_jcs(value)


def _canonical_text(value: object) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return _json_string(value)
    if isinstance(value, (int, float, Decimal)):
        return _number_to_canonical(value)
    if isinstance(value, Mapping):
        keys = list(value.keys())
        if any(not isinstance(key, str) for key in keys):
            raise ValidationError("JSON_NON_STRING_KEY", "JSON object keys must be strings")
        ordered = sorted(cast(list[str], keys), key=_utf16_sort_key)
        return (
            "{"
            + ",".join(_json_string(key) + ":" + _canonical_text(value[key]) for key in ordered)
            + "}"
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return "[" + ",".join(_canonical_text(item) for item in value) + "]"
    raise ValidationError("JSON_UNSUPPORTED_TYPE", "Value is not representable as canonical JSON")


def canonical_json_text(
    value: object,
    *,
    max_depth: int = MAX_JSON_DEPTH,
    max_string_bytes: int = MAX_STRING_BYTES,
) -> str:
    """Return deterministic RFC 8785-compatible JSON for the supported domain."""

    if max_depth < 1 or max_string_bytes < 1:
        raise ValueError("JSON limits must be positive")
    _validate_json_tree(value, max_depth=max_depth, max_string_bytes=max_string_bytes)
    return _canonical_text(value)


def canonical_json_bytes(
    value: object,
    *,
    max_depth: int = MAX_JSON_DEPTH,
    max_string_bytes: int = MAX_STRING_BYTES,
) -> bytes:
    """Return UTF-8 canonical JSON without a trailing newline."""

    return canonical_json_text(
        value,
        max_depth=max_depth,
        max_string_bytes=max_string_bytes,
    ).encode("utf-8")


def canonical_sha256(value: object) -> str:
    """Hash canonical JSON bytes with lowercase SHA-256."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _coerce_decimal(value: DecimalInput) -> Decimal:
    if isinstance(value, bool):
        raise ValidationError("DECIMAL_INVALID", "Decimal value must be numeric")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError("DECIMAL_NONFINITE", "Decimal value must be finite")
        result = Decimal(str(value))
    elif isinstance(value, str):
        if len(value) > _MAX_NUMBER_TOKEN_CHARS:
            raise ResourceLimitError("DECIMAL_TOO_LONG", "Decimal value exceeds the limit")
        try:
            result = Decimal(value)
        except InvalidOperation as exc:
            raise ValidationError("DECIMAL_INVALID", "Decimal value must be numeric") from exc
    else:
        raise ValidationError("DECIMAL_INVALID", "Decimal value must be numeric")
    if not result.is_finite():
        raise ValidationError("DECIMAL_NONFINITE", "Decimal value must be finite")
    return result


def quantize_decimal(value: DecimalInput) -> Decimal:
    """Quantize a finite decimal to 12 places with precision 50, half-even."""

    decimal_value = _coerce_decimal(value)
    try:
        with localcontext(_DECIMAL_CONTEXT):
            quantized = decimal_value.quantize(_DECIMAL_QUANTUM)
    except InvalidOperation as exc:
        raise ValidationError(
            "DECIMAL_OUT_OF_RANGE", "Decimal value exceeds the supported range"
        ) from exc
    if quantized.is_zero():
        return Decimal(0)
    return quantized


def decimal_string(value: DecimalInput) -> str:
    """Return the one normalized decimal spelling used by receipt metrics."""

    quantized = quantize_decimal(value)
    if quantized.is_zero():
        return "0"
    rendered = format(quantized, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered

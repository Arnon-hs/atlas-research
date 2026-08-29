# SPDX-License-Identifier: MIT
"""Validated resource ceilings reduced monotonically across authorities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Final

from .constants import (
    MAX_JSON_DEPTH,
    MAX_OPEN_FILES,
    MAX_OUTPUT_BYTES,
    MAX_RECORDS,
    MAX_RSS_BYTES,
    MAX_STRING_BYTES,
    MAX_TOTAL_INPUT_BYTES,
    MAX_WALL_SECONDS,
    MAX_WORKSPACE_BYTES,
)
from .errors import ValidationError

_LIMIT_NAMES: Final = (
    "wall_seconds",
    "max_records",
    "max_input_bytes",
    "max_output_bytes",
    "max_workspace_bytes",
    "max_peak_rss_bytes",
    "max_open_files",
    "max_json_depth",
    "max_string_bytes",
)
_MINIMUMS: Final[dict[str, int]] = {
    "wall_seconds": 1,
    "max_records": 1,
    "max_input_bytes": 1,
    "max_output_bytes": 1,
    "max_workspace_bytes": 1,
    "max_peak_rss_bytes": 1 << 20,
    "max_open_files": 16,
    "max_json_depth": 4,
    "max_string_bytes": 1,
}


@dataclass(frozen=True, slots=True)
class EffectiveLimits:
    """Complete v0.1 experiment limits after monotonic reduction."""

    wall_seconds: int
    max_records: int
    max_input_bytes: int
    max_output_bytes: int
    max_workspace_bytes: int
    max_peak_rss_bytes: int
    max_open_files: int
    max_json_depth: int
    max_string_bytes: int

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValidationError("LIMIT_TYPE_INVALID", "Resource limit must be an integer")
            if value < _MINIMUMS[field.name]:
                raise ValidationError(
                    "LIMIT_BELOW_MINIMUM", "Resource limit is below the safe minimum"
                )

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> EffectiveLimits:
        """Parse a complete limits object and reject missing or unknown fields."""

        if set(value) != set(_LIMIT_NAMES):
            raise ValidationError(
                "LIMIT_FIELDS_INVALID", "Resource limit fields do not match the contract"
            )
        parsed: dict[str, int] = {}
        for name in _LIMIT_NAMES:
            item = value[name]
            if isinstance(item, bool) or not isinstance(item, int):
                raise ValidationError("LIMIT_TYPE_INVALID", "Resource limit must be an integer")
            parsed[name] = item
        return cls(**parsed)

    def to_mapping(self) -> dict[str, int]:
        return {name: getattr(self, name) for name in _LIMIT_NAMES}


OPERATOR_CEILINGS: Final = EffectiveLimits(
    wall_seconds=MAX_WALL_SECONDS,
    max_records=MAX_RECORDS,
    max_input_bytes=MAX_TOTAL_INPUT_BYTES,
    max_output_bytes=MAX_OUTPUT_BYTES,
    max_workspace_bytes=MAX_WORKSPACE_BYTES,
    max_peak_rss_bytes=MAX_RSS_BYTES,
    max_open_files=MAX_OPEN_FILES,
    max_json_depth=MAX_JSON_DEPTH,
    max_string_bytes=MAX_STRING_BYTES,
)


def _as_limits(value: EffectiveLimits | Mapping[str, object]) -> EffectiveLimits:
    return value if isinstance(value, EffectiveLimits) else EffectiveLimits.from_mapping(value)


def reduce_limits(
    *values: EffectiveLimits | Mapping[str, object],
    operator: EffectiveLimits = OPERATOR_CEILINGS,
) -> EffectiveLimits:
    """Take the field-wise minimum; no input can increase operator ceilings."""

    all_limits = (operator, *(_as_limits(value) for value in values))
    return EffectiveLimits(
        **{name: min(getattr(limit, name) for limit in all_limits) for name in _LIMIT_NAMES}
    )


def effective_limits(
    job: EffectiveLimits | Mapping[str, object],
    benchmark: EffectiveLimits | Mapping[str, object],
    *,
    host: EffectiveLimits | Mapping[str, object] | None = None,
    operator: EffectiveLimits = OPERATOR_CEILINGS,
) -> EffectiveLimits:
    """Reduce job, benchmark, operator, and optional host-safe ceilings."""

    values: list[EffectiveLimits | Mapping[str, object]] = [job, benchmark]
    if host is not None:
        values.append(host)
    return reduce_limits(*values, operator=operator)


__all__ = ["OPERATOR_CEILINGS", "EffectiveLimits", "effective_limits", "reduce_limits"]

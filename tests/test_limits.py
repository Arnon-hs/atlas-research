# SPDX-License-Identifier: MIT
from __future__ import annotations

import pytest

from atlas_research.errors import ValidationError
from atlas_research.limits import (
    OPERATOR_CEILINGS,
    EffectiveLimits,
    effective_limits,
    reduce_limits,
)


def _mapping(**changes: int) -> dict[str, object]:
    result: dict[str, object] = OPERATOR_CEILINGS.to_mapping()
    result.update(changes)
    return result


def test_limits_require_complete_integer_contract() -> None:
    assert EffectiveLimits.from_mapping(_mapping()) == OPERATOR_CEILINGS
    missing = _mapping()
    del missing["wall_seconds"]
    with pytest.raises(ValidationError, match="LIMIT_FIELDS_INVALID"):
        EffectiveLimits.from_mapping(missing)
    with pytest.raises(ValidationError, match="LIMIT_FIELDS_INVALID"):
        EffectiveLimits.from_mapping({**_mapping(), "surprise": 1})
    with pytest.raises(ValidationError, match="LIMIT_TYPE_INVALID"):
        EffectiveLimits.from_mapping(_mapping(wall_seconds=True))
    with pytest.raises(ValidationError, match="LIMIT_BELOW_MINIMUM"):
        EffectiveLimits.from_mapping(_mapping(max_open_files=15))


def test_limit_reduction_is_fieldwise_and_never_raises_operator_ceiling() -> None:
    job = EffectiveLimits.from_mapping(_mapping(wall_seconds=100, max_records=500))
    benchmark = EffectiveLimits.from_mapping(_mapping(wall_seconds=90, max_records=600))
    host = EffectiveLimits.from_mapping(_mapping(wall_seconds=80, max_records=400))
    reduced = effective_limits(job, benchmark, host=host)
    assert reduced.wall_seconds == 80
    assert reduced.max_records == 400
    assert reduced.max_output_bytes == OPERATOR_CEILINGS.max_output_bytes

    oversized = EffectiveLimits(
        **{name: value * 2 for name, value in OPERATOR_CEILINGS.to_mapping().items()}
    )
    assert reduce_limits(oversized) == OPERATOR_CEILINGS

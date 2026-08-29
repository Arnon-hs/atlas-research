# SPDX-License-Identifier: MIT
"""Stable, non-sensitive errors exposed by Atlas Research."""

from __future__ import annotations

import re
from typing import Final

_CODE_RE: Final = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_MAX_MESSAGE_CHARS: Final = 512


class AtlasResearchError(Exception):
    """Base error with a bounded public code and message.

    Callers must use fixed messages.  Hostile paths, JSON fragments, model
    responses, and operating-system error strings are intentionally not
    interpolated into this exception.
    """

    code: str
    message: str

    def __init__(self, code: str, message: str) -> None:
        if _CODE_RE.fullmatch(code) is None:
            raise ValueError("error code must use the public error-code format")
        if not message or len(message) > _MAX_MESSAGE_CHARS:
            raise ValueError("error message must be non-empty and bounded")
        if any(ord(character) < 0x20 and character not in "\t" for character in message):
            raise ValueError("error message contains a control character")
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")

    def as_dict(self) -> dict[str, str]:
        """Return the bounded public representation used in result artifacts."""

        return {"code": self.code, "message": self.message}


class ValidationError(AtlasResearchError, ValueError):
    """Untrusted input failed deterministic validation."""


class ConflictError(AtlasResearchError):
    """An immutable identifier already names different bytes."""


class ResourceLimitError(AtlasResearchError):
    """A bounded resource limit was exceeded."""

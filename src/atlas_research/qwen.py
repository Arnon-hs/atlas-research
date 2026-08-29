# SPDX-License-Identifier: MIT
"""Fail-closed local Qwen hypothesis generation through loopback Ollama."""

from __future__ import annotations

import hashlib
import http.client
import json
import math
import re
import threading
import time
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final, Protocol, TypeAlias, cast

from .constants import (
    MAX_JSON_DEPTH,
    MAX_QWEN_RESPONSE_BYTES,
    MAX_QWEN_SHOW_BYTES,
    MAX_QWEN_TIMEOUT_SECONDS,
)
from .errors import AtlasResearchError

OLLAMA_BASE_URL: Final = "http://127.0.0.1:11434"
QWEN_MODEL: Final = "qwen3:8b"

ScalarInput: TypeAlias = Decimal | int | float

_MODEL_DIGEST_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_VARIABLE_RE: Final = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
_MAX_SCALAR_MAGNITUDE: Final = (1 << 53) - 1
_METRIC_NAMES: Final = frozenset(
    {
        "mae",
        "spearman",
        "pairwise_accuracy",
        "ndcg_at_10",
        "ndcg_at_50",
        "f1",
        "calibration_error",
    }
)
_UNSAFE_HYPOTHESIS_RE: Final = re.compile(
    r"(?:https?://|ftp://|file://|www\.|\.\./|~/|[\\/]|[`{}<>]|\$\(|"
    r"\*\*|__|\[[^\]]*\]\(|&(?:lt|gt|#\d+);|(?:^|\s)#{1,6}\s|"
    r"\b(?:bash|curl|wget|sudo|subprocess|os\.system|eval|exec|import|function_call|"
    r"tool_call|tools?|arguments|powershell|select|drop|insert|delete|rm|chmod|chown)\b|"
    r"\b(?:print|open)\s*\()",
    re.IGNORECASE,
)
_FIXED_INSTRUCTIONS: Final = (
    "You are an offline Atlas Research hypothesis generator. "
    "Propose exactly one new scalar value for the named allowlisted variable. "
    "Use only the supplied aggregate validation metrics. "
    "Do not emit code, commands, markup, URLs, paths, tools, or extra fields. "
    "Return only a JSON object matching the supplied schema."
)


class QwenError(AtlasResearchError):
    """A bounded local-model failure suitable for a worker error result."""


@dataclass(frozen=True, slots=True)
class QwenContext:
    """The complete, aggregate-only context permitted to cross the model boundary."""

    variable: str
    current_value: ScalarInput
    minimum: ScalarInput
    maximum: ScalarInput
    metrics: Mapping[str, ScalarInput]


@dataclass(frozen=True, slots=True)
class QwenProposal:
    """A validated one-variable proposal and its reproducibility identity."""

    variable: str
    old_value: int | float
    new_value: int | float
    hypothesis: str
    model: str
    model_sha256: str
    prompt_sha256: str

    def generator(self) -> dict[str, str]:
        """Return the exact candidate-artifact ``generator`` object."""

        return {
            "kind": "qwen",
            "model": self.model,
            "model_sha256": self.model_sha256,
            "prompt_sha256": self.prompt_sha256,
        }


@dataclass(frozen=True, slots=True)
class QwenHTTPResponse:
    """A bounded transport response, exposed to make tests network-free."""

    status: int
    content_type: str
    body: bytes


class QwenTransport(Protocol):
    """Transport limited to the two Ollama paths used by the proposer."""

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None,
        *,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> QwenHTTPResponse: ...


class QwenProposer:
    """Validate model identity and request one inert scalar hypothesis."""

    def __init__(
        self,
        *,
        timeout_seconds: float = MAX_QWEN_TIMEOUT_SECONDS,
        transport: QwenTransport | None = None,
    ) -> None:
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
            or timeout_seconds > MAX_QWEN_TIMEOUT_SECONDS
        ):
            raise QwenError("QWEN_TIMEOUT_INVALID", "Qwen timeout exceeds the operator ceiling")
        self._timeout_seconds = float(timeout_seconds)
        self._transport = transport if transport is not None else _LoopbackTransport()

    def available(self) -> bool:
        """Return whether the exact allowlisted model and digest are available."""

        try:
            self.model_sha256()
        except QwenError:
            return False
        return True

    def model_sha256(self, *, deadline: float | None = None) -> str:
        """Resolve the immutable digest for exactly ``qwen3:8b`` via ``/api/tags``."""

        response = self._request("GET", "/api/tags", None, MAX_QWEN_SHOW_BYTES, deadline=deadline)
        document = _strict_json_object(response.body, maximum_depth=MAX_JSON_DEPTH)
        models = document.get("models")
        if not isinstance(models, list) or len(models) > 1024:
            raise QwenError("QWEN_MODEL_INVENTORY_INVALID", "Ollama model inventory is invalid")
        matches: list[str] = []
        for item in models:
            if not isinstance(item, dict) or item.get("name") != QWEN_MODEL:
                continue
            digest = item.get("digest")
            if not isinstance(digest, str) or _MODEL_DIGEST_RE.fullmatch(digest) is None:
                raise QwenError("QWEN_MODEL_DIGEST_INVALID", "Qwen model digest is invalid")
            matches.append(digest)
        if len(matches) != 1:
            raise QwenError(
                "QWEN_MODEL_UNAVAILABLE", "exactly one allowlisted Qwen model is required"
            )
        return matches[0]

    def propose(self, context: QwenContext) -> QwenProposal:
        """Generate and strictly validate one bounded scalar proposal."""

        deadline = time.monotonic() + self._timeout_seconds
        normalized = _validate_context(context)
        model_digest = self.model_sha256(deadline=deadline)
        prompt = _build_prompt(normalized)
        prompt_digest = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        current_value = cast(int | float, normalized["current_value"])
        minimum = cast(int | float, normalized["minimum"])
        maximum = cast(int | float, normalized["maximum"])
        scalar_type = "integer" if isinstance(current_value, int) else "number"
        response_schema: dict[str, object] = {
            "type": "object",
            "additionalProperties": False,
            "required": ["hypothesis", "new_value"],
            "properties": {
                "hypothesis": {"type": "string", "minLength": 1, "maxLength": 512},
                "new_value": {
                    "type": scalar_type,
                    "minimum": minimum,
                    "maximum": maximum,
                },
            },
        }
        request_document: dict[str, object] = {
            "model": QWEN_MODEL,
            "prompt": prompt,
            "format": response_schema,
            "stream": False,
            "options": {"temperature": 0},
        }
        request_body = json.dumps(
            request_document,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(request_body) > MAX_QWEN_RESPONSE_BYTES:
            raise QwenError("QWEN_REQUEST_TOO_LARGE", "Qwen request exceeds the size limit")
        response = self._request(
            "POST",
            "/api/generate",
            request_body,
            MAX_QWEN_RESPONSE_BYTES,
            deadline=deadline,
        )
        envelope = _strict_json_object(response.body, maximum_depth=MAX_JSON_DEPTH)
        if envelope.get("model") != QWEN_MODEL or envelope.get("done") is not True:
            raise QwenError("QWEN_RESPONSE_INCOMPLETE", "Qwen response is incomplete")
        raw_proposal = envelope.get("response")
        if not isinstance(raw_proposal, str) or len(raw_proposal.encode("utf-8")) > 4096:
            raise QwenError("QWEN_RESPONSE_INVALID", "Qwen proposal payload is invalid")
        proposal = _strict_json_object(raw_proposal.encode("utf-8"), maximum_depth=2)
        if set(proposal) != {"hypothesis", "new_value"}:
            raise QwenError("QWEN_RESPONSE_FIELDS_INVALID", "Qwen proposal fields are invalid")
        hypothesis = proposal["hypothesis"]
        _validate_hypothesis(hypothesis)
        new_value = _validate_proposed_value(
            proposal["new_value"],
            current_value=current_value,
            minimum=minimum,
            maximum=maximum,
        )
        if self.model_sha256(deadline=deadline) != model_digest:
            raise QwenError("QWEN_MODEL_CHANGED", "Qwen model identity changed during generation")
        return QwenProposal(
            variable=context.variable,
            old_value=current_value,
            new_value=new_value,
            hypothesis=cast(str, hypothesis),
            model=QWEN_MODEL,
            model_sha256=model_digest,
            prompt_sha256=prompt_digest,
        )

    def _request(
        self,
        method: str,
        path: str,
        body: bytes | None,
        maximum: int,
        *,
        deadline: float | None = None,
    ) -> QwenHTTPResponse:
        timeout_seconds = self._timeout_seconds
        if deadline is not None:
            timeout_seconds = min(timeout_seconds, deadline - time.monotonic())
            if timeout_seconds <= 0:
                raise QwenError("QWEN_TIMEOUT", "local Qwen request exceeded its total deadline")
        try:
            response = self._transport.request(
                method,
                path,
                body,
                timeout_seconds=timeout_seconds,
                max_response_bytes=maximum,
            )
        except QwenError:
            raise
        except (OSError, TimeoutError, http.client.HTTPException) as exc:
            raise QwenError("QWEN_CONNECTION_FAILED", "local Ollama request failed") from exc
        if response.status != 200:
            # This also fails closed on every redirect status.
            raise QwenError("QWEN_HTTP_ERROR", "local Ollama returned a non-success status")
        media_type = response.content_type.partition(";")[0].strip().lower()
        if media_type != "application/json":
            raise QwenError("QWEN_CONTENT_TYPE_INVALID", "local Ollama response is not JSON")
        if len(response.body) > maximum:
            raise QwenError("QWEN_RESPONSE_TOO_LARGE", "local Ollama response exceeds the limit")
        return response


def qwen_available(
    *,
    timeout_seconds: float = 2.0,
    transport: QwenTransport | None = None,
) -> bool:
    """Probe only the local model inventory without loading or pulling a model."""

    return QwenProposer(timeout_seconds=timeout_seconds, transport=transport).available()


class _LoopbackTransport:
    _ALLOWED: Final = {
        ("GET", "/api/tags"),
        ("GET", "/api/version"),
        ("POST", "/api/generate"),
    }

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None,
        *,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> QwenHTTPResponse:
        if (method, path) not in self._ALLOWED:
            raise QwenError("QWEN_ENDPOINT_FORBIDDEN", "Ollama endpoint is not allowlisted")
        if method == "GET" and body is not None:
            raise QwenError("QWEN_REQUEST_INVALID", "GET request must not contain a body")
        connection = http.client.HTTPConnection("127.0.0.1", 11434, timeout=timeout_seconds)
        deadline = time.monotonic() + timeout_seconds
        expired = threading.Event()

        def abort() -> None:
            expired.set()
            connection.close()

        timer = threading.Timer(timeout_seconds, abort)
        timer.daemon = True
        timer.start()
        headers = {"Accept": "application/json", "Connection": "close"}
        if body is not None:
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(body))
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            content_length = response.getheader("Content-Length")
            if content_length is not None:
                try:
                    announced = int(content_length, 10)
                except ValueError as exc:
                    raise QwenError(
                        "QWEN_RESPONSE_INVALID", "local Ollama content length is invalid"
                    ) from exc
                if announced < 0 or announced > max_response_bytes:
                    raise QwenError(
                        "QWEN_RESPONSE_TOO_LARGE", "local Ollama response exceeds the limit"
                    )
            chunks: list[bytes] = []
            remaining_bytes = max_response_bytes + 1
            while remaining_bytes > 0:
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0 or expired.is_set():
                    raise TimeoutError("Qwen total deadline expired")
                if connection.sock is not None:
                    connection.sock.settimeout(remaining_seconds)
                chunk = response.read1(min(65_536, remaining_bytes))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining_bytes -= len(chunk)
            if expired.is_set() or time.monotonic() > deadline:
                raise TimeoutError("Qwen total deadline expired")
            response_body = b"".join(chunks)
            return QwenHTTPResponse(
                status=response.status,
                content_type=response.getheader("Content-Type", ""),
                body=response_body,
            )
        finally:
            timer.cancel()
            connection.close()


def _validate_context(context: QwenContext) -> dict[str, object]:
    if not isinstance(context.variable, str) or _VARIABLE_RE.fullmatch(context.variable) is None:
        raise QwenError("QWEN_CONTEXT_INVALID", "allowlisted variable identity is invalid")
    current = _finite_scalar(context.current_value)
    minimum = _finite_scalar(context.minimum)
    maximum = _finite_scalar(context.maximum)
    if minimum > maximum or current < minimum or current > maximum:
        raise QwenError("QWEN_CONTEXT_INVALID", "variable bounds are invalid")
    if len(context.metrics) == 0 or len(context.metrics) > len(_METRIC_NAMES):
        raise QwenError("QWEN_CONTEXT_INVALID", "aggregate metric set is invalid")
    normalized_metrics: dict[str, int | float] = {}
    for name, value in context.metrics.items():
        if name not in _METRIC_NAMES:
            raise QwenError("QWEN_CONTEXT_INVALID", "aggregate metric name is not allowlisted")
        normalized_metrics[name] = _finite_scalar(value)
    return {
        "variable": context.variable,
        "current_value": current,
        "minimum": minimum,
        "maximum": maximum,
        "validation_metrics": normalized_metrics,
    }


def _build_prompt(context: Mapping[str, object]) -> str:
    aggregate_json = json.dumps(
        context,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"{_FIXED_INSTRUCTIONS}\nAggregate context: {aggregate_json}"


def _finite_scalar(
    value: object,
    *,
    code: str = "QWEN_CONTEXT_INVALID",
    message: str = "aggregate values must be bounded finite numeric scalars",
) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float)):
        raise QwenError(code, message)
    if isinstance(value, Decimal):
        if not value.is_finite() or abs(value) > _MAX_SCALAR_MAGNITUDE:
            raise QwenError(code, message)
        if value == value.to_integral_value():
            return int(value)
        normalized = float(value)
        if normalized == 0.0 and value != 0:
            raise QwenError(code, message)
    elif isinstance(value, int):
        if abs(value) > _MAX_SCALAR_MAGNITUDE:
            raise QwenError(code, message)
        return value
    else:
        normalized = value
    if not math.isfinite(normalized) or abs(normalized) > _MAX_SCALAR_MAGNITUDE:
        raise QwenError(code, message)
    return normalized


def _validate_proposed_value(
    value: object,
    *,
    current_value: int | float,
    minimum: int | float,
    maximum: int | float,
) -> int | float:
    normalized = _finite_scalar(
        value,
        code="QWEN_VALUE_INVALID",
        message="Qwen proposed value is not a bounded finite scalar",
    )
    if isinstance(current_value, int) and not isinstance(normalized, int):
        raise QwenError("QWEN_VALUE_TYPE_INVALID", "Qwen changed the variable scalar type")
    if normalized < minimum or normalized > maximum:
        raise QwenError("QWEN_VALUE_OUT_OF_RANGE", "Qwen proposed value is out of range")
    if normalized == current_value:
        raise QwenError("QWEN_VALUE_UNCHANGED", "Qwen did not change the allowlisted variable")
    return normalized


def _validate_hypothesis(value: object) -> None:
    if not isinstance(value, str):
        raise QwenError("QWEN_HYPOTHESIS_INVALID", "Qwen hypothesis must be text")
    encoded = value.encode("utf-8")
    if not 1 <= len(encoded) <= 512 or value != value.strip() or "\n" in value or "\r" in value:
        raise QwenError("QWEN_HYPOTHESIS_INVALID", "Qwen hypothesis is not bounded plain text")
    if _UNSAFE_HYPOTHESIS_RE.search(value):
        raise QwenError("QWEN_HYPOTHESIS_UNSAFE", "Qwen hypothesis contains forbidden content")
    for character in value:
        category = unicodedata.category(character)
        if category in {"Cc", "Cf", "Cs"}:
            raise QwenError(
                "QWEN_HYPOTHESIS_UNSAFE", "Qwen hypothesis contains unsafe Unicode controls"
            )


def _strict_json_object(data: bytes, *, maximum_depth: int) -> dict[str, object]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise QwenError("QWEN_JSON_INVALID", "Qwen JSON contains duplicate keys")
            result[key] = value
        return result

    try:
        value = json.loads(
            data,
            object_pairs_hook=no_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("constant")),
        )
    except QwenError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise QwenError("QWEN_JSON_INVALID", "Qwen returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise QwenError("QWEN_JSON_INVALID", "Qwen JSON must be an object")
    _require_depth(value, maximum_depth=maximum_depth)
    return cast(dict[str, object], value)


def _require_depth(value: object, *, maximum_depth: int, depth: int = 0) -> None:
    if depth > maximum_depth:
        raise QwenError("QWEN_JSON_TOO_DEEP", "Qwen JSON exceeds the nesting limit")
    if isinstance(value, dict):
        for child in value.values():
            _require_depth(child, maximum_depth=maximum_depth, depth=depth + 1)
    elif isinstance(value, list):
        for child in value:
            _require_depth(child, maximum_depth=maximum_depth, depth=depth + 1)

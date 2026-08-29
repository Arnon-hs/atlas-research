# SPDX-License-Identifier: MIT
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

import atlas_research.qwen as qwen_module
from atlas_research.canonical import strict_json_loads
from atlas_research.qwen import (
    QWEN_MODEL,
    QwenContext,
    QwenError,
    QwenHTTPResponse,
    QwenProposer,
    qwen_available,
)

EXAMPLE_CONTEXT = Path(__file__).parents[1] / "examples" / "qwen-context.json"


@dataclass
class FakeTransport:
    proposal: str = (
        '{"hypothesis":"Increase the weight to improve ranking stability.","new_value":2}'
    )
    tag_status: int = 200
    generate_status: int = 200
    model_digest: str = "a" * 64
    calls: list[tuple[str, str, bytes | None]] = field(default_factory=list)

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None,
        *,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> QwenHTTPResponse:
        del timeout_seconds, max_response_bytes
        self.calls.append((method, path, body))
        if path == "/api/tags":
            response = {
                "models": [{"name": QWEN_MODEL, "model": QWEN_MODEL, "digest": self.model_digest}]
            }
            return QwenHTTPResponse(
                self.tag_status, "application/json; charset=utf-8", json.dumps(response).encode()
            )
        response = {
            "model": QWEN_MODEL,
            "done": True,
            "response": self.proposal,
        }
        return QwenHTTPResponse(
            self.generate_status, "application/json", json.dumps(response).encode()
        )


def _context() -> QwenContext:
    return QwenContext(
        variable="ranking.weight",
        current_value=1,
        minimum=0,
        maximum=4,
        metrics={"ndcg_at_10": 0.71, "mae": 0.2},
    )


def test_propose_uses_only_tags_and_generate_with_bounded_context() -> None:
    transport = FakeTransport()
    proposal = QwenProposer(transport=transport).propose(_context())

    assert proposal.new_value == 2
    assert proposal.old_value == 1
    assert proposal.model == QWEN_MODEL
    assert proposal.model_sha256 == "a" * 64
    assert proposal.generator() == {
        "kind": "qwen",
        "model": QWEN_MODEL,
        "model_sha256": "a" * 64,
        "prompt_sha256": proposal.prompt_sha256,
    }
    assert [(method, path) for method, path, _body in transport.calls] == [
        ("GET", "/api/tags"),
        ("POST", "/api/generate"),
        ("GET", "/api/tags"),
    ]
    request = json.loads(transport.calls[1][2] or b"{}")
    assert request["model"] == QWEN_MODEL
    assert request["stream"] is False
    assert request["options"] == {"temperature": 0}
    assert request["format"]["additionalProperties"] is False
    assert set(request["format"]["required"]) == {"hypothesis", "new_value"}
    assert "tools" not in request
    assert "PRIVATE_CANARY_7f11" not in (transport.calls[1][2] or b"").decode()


def test_example_context_with_decimal_metrics_reaches_fake_transport() -> None:
    value = strict_json_loads(EXAMPLE_CONTEXT.read_bytes())
    assert isinstance(value, dict)
    metrics = value["metrics"]
    assert isinstance(metrics, Mapping)
    transport = FakeTransport()

    proposal = QwenProposer(transport=transport).propose(
        QwenContext(
            variable=cast(str, value["variable"]),
            current_value=cast(Decimal | int | float, value["current_value"]),
            minimum=cast(Decimal | int | float, value["minimum"]),
            maximum=cast(Decimal | int | float, value["maximum"]),
            metrics=cast(Mapping[str, Decimal | int | float], metrics),
        )
    )

    assert proposal.new_value == 2
    assert [(method, path) for method, path, _body in transport.calls] == [
        ("GET", "/api/tags"),
        ("POST", "/api/generate"),
        ("GET", "/api/tags"),
    ]


@pytest.mark.parametrize(
    "value",
    [True, 10**1_000, float("inf"), Decimal("1e10000"), Decimal("NaN")],
)
def test_rejects_unbounded_or_nonfinite_context_scalars_before_transport(value: object) -> None:
    transport = FakeTransport()

    with pytest.raises(QwenError) as captured:
        QwenProposer(transport=transport).propose(
            QwenContext("ranking.weight", cast(Decimal | int | float, value), 0, 4, {"mae": 1})
        )

    assert captured.value.code == "QWEN_CONTEXT_INVALID"
    assert transport.calls == []


def test_rejects_huge_integer_proposal_without_overflow() -> None:
    proposal = '{"hypothesis":"ok","new_value":' + str(10**1_000) + "}"

    with pytest.raises(QwenError) as captured:
        QwenProposer(transport=FakeTransport(proposal=proposal)).propose(_context())

    assert captured.value.code == "QWEN_VALUE_INVALID"


def test_qwen_availability_requires_exact_immutable_digest() -> None:
    assert qwen_available(transport=FakeTransport())
    assert not qwen_available(transport=FakeTransport(model_digest="latest"))


@pytest.mark.parametrize(
    "proposal",
    [
        '{"hypothesis":"ok","new_value":2,"other":1}',
        '{"hypothesis":"ok","new_value":{"value":2}}',
        '{"hypothesis":"ok","new_value":NaN}',
        '{"hypothesis":"ok","new_value":8}',
        '{"hypothesis":"ok","new_value":1}',
        '{"hypothesis":"ok","new_value":2.5}',
    ],
)
def test_rejects_unknown_nested_nonfinite_range_unchanged_and_type_changes(
    proposal: str,
) -> None:
    with pytest.raises(QwenError):
        QwenProposer(transport=FakeTransport(proposal=proposal)).propose(_context())


@pytest.mark.parametrize(
    "hypothesis",
    [
        "Run curl https://example.invalid",
        "Use ../private/data",
        "<script>alert(1)</script>",
        "Call tool_call with arguments",
        "```python import os```",
        "first line\nsecond line",
        "Bidi \u202esecret",
    ],
)
def test_rejects_code_paths_urls_markup_tools_and_controls(hypothesis: str) -> None:
    payload = json.dumps({"hypothesis": hypothesis, "new_value": 2})

    with pytest.raises(QwenError):
        QwenProposer(transport=FakeTransport(proposal=payload)).propose(_context())


def test_rejects_unknown_metric_before_any_request() -> None:
    transport = FakeTransport()
    context = QwenContext("ranking.weight", 1, 0, 4, {"PRIVATE_CANARY_7f11": 1})

    with pytest.raises(QwenError) as captured:
        QwenProposer(transport=transport).propose(context)

    assert captured.value.code == "QWEN_CONTEXT_INVALID"
    assert transport.calls == []


def test_redirect_is_not_followed() -> None:
    transport = FakeTransport(tag_status=302)

    with pytest.raises(QwenError) as captured:
        QwenProposer(transport=transport).propose(_context())

    assert captured.value.code == "QWEN_HTTP_ERROR"
    assert len(transport.calls) == 1


def test_duplicate_exact_model_entries_fail_closed() -> None:
    class DuplicateTransport(FakeTransport):
        def request(
            self,
            method: str,
            path: str,
            body: bytes | None,
            *,
            timeout_seconds: float,
            max_response_bytes: int,
        ) -> QwenHTTPResponse:
            if path != "/api/tags":
                return super().request(
                    method,
                    path,
                    body,
                    timeout_seconds=timeout_seconds,
                    max_response_bytes=max_response_bytes,
                )
            document = {
                "models": [
                    {"name": QWEN_MODEL, "digest": "a" * 64},
                    {"name": QWEN_MODEL, "digest": "a" * 64},
                ]
            }
            return QwenHTTPResponse(200, "application/json", json.dumps(document).encode())

    with pytest.raises(QwenError) as captured:
        QwenProposer(transport=DuplicateTransport()).propose(_context())

    assert captured.value.code == "QWEN_MODEL_UNAVAILABLE"


def test_model_digest_change_during_generation_fails_closed() -> None:
    class ChangingTransport(FakeTransport):
        tag_calls = 0

        def request(
            self,
            method: str,
            path: str,
            body: bytes | None,
            *,
            timeout_seconds: float,
            max_response_bytes: int,
        ) -> QwenHTTPResponse:
            if path == "/api/tags":
                self.tag_calls += 1
                self.model_digest = ("a" if self.tag_calls == 1 else "b") * 64
            return super().request(
                method,
                path,
                body,
                timeout_seconds=timeout_seconds,
                max_response_bytes=max_response_bytes,
            )

    with pytest.raises(QwenError) as captured:
        QwenProposer(transport=ChangingTransport()).propose(_context())

    assert captured.value.code == "QWEN_MODEL_CHANGED"


def test_loopback_transport_enforces_endpoint_headers_and_bounded_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[object] = []

    class FakeSocket:
        def __init__(self) -> None:
            self.timeouts: list[float] = []

        def settimeout(self, value: float) -> None:
            self.timeouts.append(value)

    class FakeResponse:
        status = 200

        def __init__(self) -> None:
            self.chunks = [b'{"ok":true}', b""]

        def getheader(self, name: str, default: str | None = None) -> str | None:
            if name == "Content-Length":
                return "11"
            if name == "Content-Type":
                return "application/json"
            return default

        def read1(self, _maximum: int) -> bytes:
            return self.chunks.pop(0)

    class FakeConnection:
        def __init__(self, host: str, port: int, *, timeout: float) -> None:
            assert (host, port) == ("127.0.0.1", 11434)
            assert timeout == 1
            self.sock = FakeSocket()
            self.closed = False
            self.request_args: tuple[object, ...] | None = None
            connections.append(self)

        def request(
            self, method: str, path: str, *, body: bytes | None, headers: dict[str, str]
        ) -> None:
            self.request_args = (method, path, body, headers)

        def getresponse(self) -> FakeResponse:
            return FakeResponse()

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(qwen_module.http.client, "HTTPConnection", FakeConnection)
    transport = qwen_module._LoopbackTransport()

    response = transport.request(
        "POST",
        "/api/generate",
        b"{}",
        timeout_seconds=1,
        max_response_bytes=64,
    )

    assert response.body == b'{"ok":true}'
    connection = cast(FakeConnection, connections[0])
    assert connection.closed is True
    assert connection.sock.timeouts
    assert connection.request_args is not None
    headers = cast(dict[str, str], connection.request_args[3])
    assert headers["Connection"] == "close"
    assert headers["Content-Length"] == "2"


def test_loopback_transport_rejects_forbidden_shape_before_connect() -> None:
    transport = qwen_module._LoopbackTransport()
    with pytest.raises(QwenError, match="QWEN_ENDPOINT_FORBIDDEN"):
        transport.request("GET", "/api/pull", None, timeout_seconds=1, max_response_bytes=64)
    with pytest.raises(QwenError, match="QWEN_REQUEST_INVALID"):
        transport.request("GET", "/api/tags", b"{}", timeout_seconds=1, max_response_bytes=64)

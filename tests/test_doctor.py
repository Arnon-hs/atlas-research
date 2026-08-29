# SPDX-License-Identifier: MIT
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from atlas_research.doctor import (
    OLLAMA_VERSION_URL,
    QWEN_MODEL,
    DoctorCheck,
    _get_json,
    _ollama_listener_check,
    _physical_memory_bytes,
    run_doctor,
)
from atlas_research.qwen import QwenHTTPResponse


def test_doctor_endpoint_is_fixed() -> None:
    try:
        _get_json("http://example.invalid/api/version")
    except ValueError as error:
        assert str(error) == "doctor endpoint is not allowlisted"
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("remote endpoint was accepted")


def test_doctor_uses_bounded_loopback_transport() -> None:
    class FakeTransport:
        def request(
            self,
            method: str,
            path: str,
            body: bytes | None,
            *,
            timeout_seconds: float,
            max_response_bytes: int,
        ) -> QwenHTTPResponse:
            assert (method, path, body) == ("GET", "/api/version", None)
            assert timeout_seconds == 1.5
            assert max_response_bytes == 128
            return QwenHTTPResponse(200, "application/json; charset=utf-8", b'{"version":"1"}')

    with patch("atlas_research.doctor._LoopbackTransport", return_value=FakeTransport()):
        assert _get_json(OLLAMA_VERSION_URL, max_bytes=128, timeout=1.5) == {"version": "1"}


def test_doctor_optional_ollama() -> None:
    with (
        patch("atlas_research.doctor._physical_memory_bytes", return_value=16 << 30),
        patch("atlas_research.doctor._get_json", side_effect=OSError),
    ):
        report = run_doctor(require_qwen=False)
    assert report.ok
    assert report.to_dict()["ok"] is True


def test_doctor_required_qwen_fails_closed() -> None:
    with (
        patch("atlas_research.doctor._physical_memory_bytes", return_value=16 << 30),
        patch("atlas_research.doctor._get_json", side_effect=OSError),
    ):
        report = run_doctor(require_qwen=True)
    assert not report.ok
    assert DoctorCheck("x", True, "y").ok


def test_doctor_required_qwen_accepts_exact_digest_and_loopback_listener() -> None:
    responses = iter(
        [
            {"version": "0.11.0"},
            {"models": [{"name": QWEN_MODEL, "digest": "a" * 64}]},
        ]
    )
    with (
        patch("atlas_research.doctor._physical_memory_bytes", return_value=16 << 30),
        patch("atlas_research.doctor._get_json", side_effect=lambda _url: next(responses)),
        patch(
            "atlas_research.doctor._ollama_listener_check",
            return_value=DoctorCheck("ollama_listener", True, "loopback-only"),
        ),
    ):
        report = run_doctor(require_qwen=True)
    assert report.ok
    assert {check.name: check.ok for check in report.checks}["qwen_model"] is True


def test_doctor_rejects_nonloopback_listener() -> None:
    completed = SimpleNamespace(
        returncode=0,
        stdout=(
            "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\n"
            "ollama 1 neo 10u IPv4 0 0t0 TCP *:11434 (LISTEN)\n"
        ),
    )
    with (
        patch("atlas_research.doctor.os.path.isfile", return_value=True),
        patch("atlas_research.doctor.subprocess.run", return_value=completed),
    ):
        check = _ollama_listener_check()
    assert not check.ok
    assert check.detail == "listener is not loopback-only"


def test_doctor_accepts_ipv4_loopback_listener_and_reads_host_memory() -> None:
    completed = SimpleNamespace(
        returncode=0,
        stdout=(
            "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\n"
            "ollama 1 neo 10u IPv4 0 0t0 TCP 127.0.0.1:11434 (LISTEN)\n"
        ),
    )
    with (
        patch("atlas_research.doctor.os.path.isfile", return_value=True),
        patch("atlas_research.doctor.subprocess.run", return_value=completed),
    ):
        check = _ollama_listener_check()
    assert check.ok
    assert _physical_memory_bytes() is None or _physical_memory_bytes() > 0


def test_doctor_reports_invalid_inventory_without_leaking_response() -> None:
    responses = iter([{"version": "0.11.0"}, {"models": "invalid"}])
    with (
        patch("atlas_research.doctor._physical_memory_bytes", return_value=16 << 30),
        patch("atlas_research.doctor._get_json", side_effect=lambda _url: next(responses)),
        patch(
            "atlas_research.doctor._ollama_listener_check",
            return_value=DoctorCheck("ollama_listener", True, "loopback-only"),
        ),
    ):
        report = run_doctor(require_qwen=True)
    assert not report.ok
    assert report.checks[-1].detail == "qwen3:8b unavailable"

# SPDX-License-Identifier: MIT
from __future__ import annotations

import hashlib
import http.client
import stat
import threading
from pathlib import Path
from typing import cast

import pytest

from atlas_research.receipts import canonical_result_sha256
from atlas_research.report import ReportError, render_report, serve_report, write_report


def _artifact(role: str) -> dict[str, object]:
    contracts = {
        "dataset_manifest": (
            "application/vnd.atlas-research.dataset-manifest+json",
            "urn:atlasrepo:atlas-research:schema:v1:dataset-manifest",
        ),
        "benchmark_manifest": (
            "application/vnd.atlas-research.benchmark-manifest+json",
            "urn:atlasrepo:atlas-research:schema:v1:benchmark-manifest",
        ),
        "evaluation_payload": (
            "application/json",
            "urn:atlasrepo:atlas-research:fixture:v1:linear-evaluator",
        ),
        "candidate": (
            "application/vnd.atlas-research.candidate+json",
            "urn:atlasrepo:atlas-research:schema:v1:candidate-artifact",
        ),
    }
    media_type, schema_id = contracts[role]
    return {
        "uri": f"{role}.json",
        "role": role,
        "media_type": media_type,
        "sha256": hashlib.sha256(role.encode()).hexdigest(),
        "size_bytes": 1,
        "producer": {"name": "atlas-research", "version": "0.1.0"},
        "external_schema": {"id": schema_id, "version": "1.0.0"},
    }


def _receipt(*, receipt_id: str = "receipt-1") -> dict[str, object]:
    result: dict[str, object] = {
        "metrics": {
            "ndcg_at_10": {
                "baseline": "0.7",
                "candidate": "0.72",
                "candidate_minus_baseline": "0.02",
                "passed": True,
            },
            "mae": {
                "baseline": "0.3",
                "candidate": "0.2",
                "candidate_minus_baseline": "-0.1",
                "passed": True,
            },
        },
        "all_gates_passed": True,
        "decision": "KEEP",
        "reason_codes": ["ALL_GATES_PASSED"],
    }
    receipt: dict[str, object] = {
        "schema_version": "1.0.0",
        "receipt_id": receipt_id,
        "previous_receipt_sha256": None,
        "created_at": "2026-08-30T00:00:01Z",
        "started_at": "2026-08-30T00:00:00Z",
        "finished_at": "2026-08-30T00:00:01Z",
        "experiment_id": "experiment-report",
        "job_id": "job-report",
        "attempt": 1,
        "idempotency_key": "report-idempotency-0001",
        "job_spec_sha256": "1" * 64,
        "evaluation_split": "validation",
        "canonical_result": result,
        "canonical_result_sha256": canonical_result_sha256(result),
        "dataset_manifest": _artifact("dataset_manifest"),
        "benchmark_manifest": _artifact("benchmark_manifest"),
        "baseline_evaluation_payload": _artifact("evaluation_payload"),
        "candidate": _artifact("candidate"),
        "resource_usage": {
            "wall_milliseconds": 1,
            "records_evaluated": 1,
            "peak_rss_bytes": 1,
        },
        "provenance": {
            "atlas_research_version": "0.1.0",
            "git_commit": "1" * 40,
            "source_revision_kind": "verified_checkout",
            "python_version": "3.11.0",
            "platform": "Test arm64",
            "worker_id": "worker-test",
            "worker_session_id": "session-test",
        },
    }
    return receipt


def test_report_is_static_escaped_and_aggregate_only() -> None:
    canary = "PRIVATE_CANARY_7f11"
    receipt = _receipt()
    rendered = render_report(
        [receipt], title='<svg onload="alert(2)">\u202e' + ("界" * 1000)
    ).decode()

    assert "&lt;svg onload=&quot;" in rendered
    assert "<img" not in rendered
    assert "<svg" not in rendered
    assert "<script" not in rendered.lower()
    assert "<style" not in rendered.lower()
    assert "href=" not in rendered.lower()
    assert "PRIVATE_CANARY_7f11" not in rendered
    assert "example.invalid" not in rendered
    assert "\u202e" not in rendered
    assert "default-src 'none'" in rendered
    assert "ndcg_at_10" in rendered
    assert "0.72" in rendered

    receipt["untrusted_raw_payload"] = {"prompt": canary}
    with pytest.raises(ReportError) as captured:
        render_report([receipt])
    assert captured.value.code == "REPORT_RECEIPT_INVALID"


def test_report_counts_decisions() -> None:
    keep = _receipt(receipt_id="keep")
    discard = _receipt(receipt_id="discard")
    result = discard["canonical_result"]
    assert isinstance(result, dict)
    result["decision"] = "DISCARD"
    result["all_gates_passed"] = False
    metrics = result["metrics"]
    assert isinstance(metrics, dict)
    metric = metrics["ndcg_at_10"]
    assert isinstance(metric, dict)
    metric["passed"] = False
    result["reason_codes"] = ["NDCG_AT_10_GATE_FAILED"]
    discard["canonical_result_sha256"] = canonical_result_sha256(result)

    rendered = render_report([keep, discard]).decode()

    assert "Receipts: 2" in rendered
    assert "KEEP</th><td>1" in rendered
    assert "DISCARD</th><td>1" in rendered


def test_write_report_is_private_and_exclusive(tmp_path: Path) -> None:
    path = tmp_path / "report.html"
    written = write_report([_receipt()], path)

    assert written == path
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with pytest.raises(ReportError) as captured:
        write_report([_receipt()], path)
    assert captured.value.code == "REPORT_WRITE_FAILED"


def test_loopback_server_supports_only_single_file_get_and_head() -> None:
    body = render_report([_receipt()])
    server = serve_report(body)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    path = cast(str, server.__dict__["report_path"])
    try:
        connection = http.client.HTTPConnection(host, port, timeout=2)
        connection.request("GET", path)
        response = connection.getresponse()
        assert response.status == 200
        assert response.read() == body
        assert response.getheader("X-Content-Type-Options") == "nosniff"
        assert response.getheader("Content-Security-Policy") == (
            "default-src 'none'; base-uri 'none'; form-action 'none'; "
            "frame-ancestors 'none'; sandbox"
        )
        assert response.getheader("X-Frame-Options") == "DENY"
        assert response.getheader("Access-Control-Allow-Origin") is None
        connection.close()

        connection = http.client.HTTPConnection(host, port, timeout=2)
        connection.request("HEAD", path)
        response = connection.getresponse()
        assert response.status == 200
        assert response.read() == b""
        assert int(response.getheader("Content-Length", "-1")) == len(body)
        connection.close()

        connection = http.client.HTTPConnection(host, port, timeout=2)
        connection.request("GET", "/anything")
        response = connection.getresponse()
        assert response.status == 404
        assert response.read() == b""
        connection.close()

        connection = http.client.HTTPConnection(host, port, timeout=2)
        connection.request("POST", path)
        response = connection.getresponse()
        assert response.status == 405
        assert response.getheader("Allow") == "GET, HEAD"
        assert response.getheader("Access-Control-Allow-Origin") is None
        response.read()
        connection.close()

        connection = http.client.HTTPConnection(host, port, timeout=2)
        connection.request("GET", path, headers={"Host": "evil.example"})
        response = connection.getresponse()
        assert response.status == 421
        response.read()
        connection.close()

        connection = http.client.HTTPConnection(host, port, timeout=2)
        connection.request("GET", path, headers={"Origin": "https://evil.example"})
        response = connection.getresponse()
        assert response.status == 403
        response.read()
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_report_server_refuses_nonliteral_loopback() -> None:
    with pytest.raises(ReportError) as captured:
        serve_report(b"ok", host="localhost")

    assert captured.value.code == "REPORT_HOST_FORBIDDEN"


def test_report_server_refuses_untrusted_html_even_on_loopback() -> None:
    with pytest.raises(ReportError) as captured:
        serve_report(b"<script>alert(1)</script>")

    assert captured.value.code == "REPORT_INVALID"


def test_report_server_refuses_meta_refresh_with_valid_csp_marker() -> None:
    body = render_report([_receipt()]).replace(
        b"</head>",
        b'<meta http-equiv="refresh" content="0;url=http://127.0.0.1:9999/">\n</head>',
    )

    with pytest.raises(ReportError) as captured:
        serve_report(body)

    assert captured.value.code == "REPORT_INVALID"

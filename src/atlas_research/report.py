# SPDX-License-Identifier: MIT
"""Static, aggregate-only HTML reports and a loopback single-file server."""

from __future__ import annotations

import hashlib
import html
import os
import secrets
import stat
import unicodedata
from collections import Counter
from collections.abc import Mapping, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Final, cast

from .canonical import canonical_json_bytes
from .errors import AtlasResearchError
from .receipts import validate_receipt_document

_CSP: Final = (
    "default-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'; sandbox"
)
_DECISIONS: Final = ("KEEP", "DISCARD", "ERROR")
_METRICS: Final = (
    "mae",
    "spearman",
    "pairwise_accuracy",
    "ndcg_at_10",
    "ndcg_at_50",
    "f1",
    "calibration_error",
)
_MAX_RECEIPTS: Final = 1000
_MAX_REPORT_BYTES: Final = 8 << 20
_MAX_TEXT_BYTES: Final = 512


class ReportError(AtlasResearchError):
    """A safe, bounded report-generation or serving failure."""


def render_report(
    receipts: Sequence[Mapping[str, object]],
    *,
    title: str = "Atlas Research experiment report",
) -> bytes:
    """Render receipt aggregates and digests without raw experiment data."""

    if len(receipts) > _MAX_RECEIPTS:
        raise ReportError("REPORT_TOO_MANY_RECEIPTS", "report receipt limit exceeded")
    safe_title = _escape_text(title)
    decisions: Counter[str] = Counter()
    rows: list[str] = []
    metric_rows: list[str] = []

    for receipt in receipts:
        try:
            validate_receipt_document(receipt)
        except AtlasResearchError as exc:
            raise ReportError(
                "REPORT_RECEIPT_INVALID", "receipt contract validation failed"
            ) from exc
        decision, canonical_result = _canonical_result(receipt)
        decisions[decision] += 1
        try:
            receipt_digest = hashlib.sha256(canonical_json_bytes(dict(receipt)) + b"\n").hexdigest()
        except (AtlasResearchError, TypeError, ValueError, UnicodeError) as exc:
            raise ReportError(
                "REPORT_RECEIPT_INVALID", "receipt cannot be canonically hashed"
            ) from exc
        receipt_id = _escape_text(receipt.get("receipt_id", "unknown"))
        result_digest = receipt.get("canonical_result_sha256", "unknown")
        if not isinstance(result_digest, str) or len(result_digest) != 64:
            result_digest = "unknown"
        split = receipt.get("evaluation_split", "unknown")
        if split not in {"validation", "test"}:
            split = "unknown"
        reason_codes = canonical_result.get("reason_codes")
        safe_reasons = _reason_codes(reason_codes)
        metrics = canonical_result.get("metrics")
        metric_count = len(metrics) if isinstance(metrics, Mapping) else 0
        rows.append(
            "<tr>"
            f"<td>{receipt_id}</td>"
            f"<td>{decision}</td>"
            f"<td>{split}</td>"
            f"<td>{metric_count}</td>"
            f"<td><code>{receipt_digest}</code></td>"
            f"<td><code>{html.escape(result_digest)}</code></td>"
            f"<td>{safe_reasons}</td>"
            "</tr>"
        )
        metric_rows.extend(_render_metrics(receipt_digest, metrics))

    decision_rows = "".join(
        f'<tr><th scope="row">{decision}</th><td>{decisions[decision]}</td></tr>'
        for decision in _DECISIONS
    )
    receipt_rows = "".join(rows) or '<tr><td colspan="7">No receipts</td></tr>'
    rendered_metric_rows = "".join(metric_rows) or '<tr><td colspan="6">No metrics</td></tr>'
    document = (
        "<!doctype html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '<meta charset="utf-8">\n'
        f'<meta http-equiv="Content-Security-Policy" content="{_CSP}">\n'
        '<meta name="referrer" content="no-referrer">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{safe_title}</title>\n"
        "</head>\n"
        "<body>\n"
        f"<h1>{safe_title}</h1>\n"
        "<p>Validated offline receipt evidence. KEEP never authorizes production activation.</p>\n"
        f"<p>Receipts: {len(receipts)}</p>\n"
        "<table>\n"
        "<caption>Decision aggregates</caption>\n"
        '<thead><tr><th scope="col">Decision</th><th scope="col">Count</th></tr></thead>\n'
        f"<tbody>{decision_rows}</tbody>\n"
        "</table>\n"
        "<table>\n"
        "<caption>Receipt evidence</caption>\n"
        "<thead><tr>"
        '<th scope="col">Receipt</th><th scope="col">Decision</th>'
        '<th scope="col">Split</th><th scope="col">Metrics</th>'
        '<th scope="col">Receipt SHA-256</th>'
        '<th scope="col">Result SHA-256</th><th scope="col">Reason codes</th>'
        "</tr></thead>\n"
        f"<tbody>{receipt_rows}</tbody>\n"
        "</table>\n"
        "<table>\n"
        "<caption>Metric aggregates</caption>\n"
        "<thead><tr>"
        '<th scope="col">Receipt SHA-256</th><th scope="col">Metric</th>'
        '<th scope="col">Baseline</th><th scope="col">Candidate</th>'
        '<th scope="col">Delta</th><th scope="col">Passed</th>'
        "</tr></thead>\n"
        f"<tbody>{rendered_metric_rows}</tbody>\n"
        "</table>\n"
        "</body>\n"
        "</html>\n"
    ).encode()
    if len(document) > _MAX_REPORT_BYTES:
        raise ReportError("REPORT_TOO_LARGE", "rendered report exceeds the size limit")
    return document


def write_report(
    receipts: Sequence[Mapping[str, object]],
    output_path: Path,
    *,
    title: str = "Atlas Research experiment report",
) -> Path:
    """Create one private report file without following or replacing a link."""

    data = render_report(receipts, title=title)
    path = Path(output_path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as exc:
        raise ReportError("REPORT_WRITE_FAILED", "cannot create report output") from exc
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fsync(fd)
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise OSError("unsafe report output")
    except OSError as exc:
        raise ReportError("REPORT_WRITE_FAILED", "cannot persist report output") from exc
    finally:
        os.close(fd)
    return path


def serve_report(
    report: bytes,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
) -> ThreadingHTTPServer:
    """Bind an aggregate report as one loopback-only GET/HEAD resource.

    The returned server is not started. Callers control ``serve_forever`` and
    ``shutdown`` so lifecycle remains explicit and testable.
    """

    if host != "127.0.0.1":
        raise ReportError("REPORT_HOST_FORBIDDEN", "report server must use literal loopback")
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65_535:
        raise ReportError("REPORT_PORT_INVALID", "report server port is invalid")
    if not isinstance(report, bytes) or len(report) > _MAX_REPORT_BYTES:
        raise ReportError("REPORT_TOO_LARGE", "served report exceeds the size limit")
    body = bytes(report)
    _validate_served_report(body)
    token_path = f"/report/{secrets.token_urlsafe(32)}"

    class SingleReportHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "AtlasResearchReport"
        sys_version = ""

        def do_GET(self) -> None:
            self._send(include_body=True)

        def do_HEAD(self) -> None:
            self._send(include_body=False)

        def do_POST(self) -> None:
            self._method_not_allowed()

        def do_PUT(self) -> None:
            self._method_not_allowed()

        def do_DELETE(self) -> None:
            self._method_not_allowed()

        def do_OPTIONS(self) -> None:
            self._method_not_allowed()

        def _send(self, *, include_body: bool) -> None:
            if not self._trusted_request():
                return
            if self.path != token_path:
                self.send_response(404)
                self._security_headers(content_length=0)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self._security_headers(content_length=len(body))
            self.end_headers()
            if include_body:
                self.wfile.write(body)

        def _method_not_allowed(self) -> None:
            if not self._trusted_request():
                return
            self.send_response(405)
            self.send_header("Allow", "GET, HEAD")
            self._security_headers(content_length=0)
            self.end_headers()

        def _trusted_request(self) -> bool:
            address = cast(tuple[str, int], self.server.server_address)
            expected_host = f"127.0.0.1:{address[1]}"
            if self.headers.get("Host") != expected_host:
                self.send_response(421)
                self._security_headers(content_length=0)
                self.end_headers()
                return False
            origin = self.headers.get("Origin")
            if origin is not None and origin != f"http://{expected_host}":
                self.send_response(403)
                self._security_headers(content_length=0)
                self.end_headers()
                return False
            return True

        def _security_headers(self, *, content_length: int) -> None:
            self.send_header("Content-Length", str(content_length))
            self.send_header("Content-Security-Policy", _CSP)
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Cross-Origin-Resource-Policy", "same-origin")
            self.send_header("Cache-Control", "no-store")

        def log_message(self, _format: str, *args: object) -> None:
            del args

    class SingleReportServer(ThreadingHTTPServer):
        daemon_threads = True
        allow_reuse_address = False
        report_path: str

    try:
        server = SingleReportServer((host, port), SingleReportHandler)
        server.report_path = token_path
        return server
    except OSError as exc:
        raise ReportError("REPORT_BIND_FAILED", "cannot bind loopback report server") from exc


def _canonical_result(receipt: Mapping[str, object]) -> tuple[str, Mapping[str, object]]:
    value = receipt.get("canonical_result")
    if not isinstance(value, Mapping):
        raise ReportError("REPORT_RECEIPT_INVALID", "receipt canonical result is invalid")
    result = cast(Mapping[str, object], value)
    decision = result.get("decision")
    if decision not in _DECISIONS:
        raise ReportError("REPORT_RECEIPT_INVALID", "receipt decision is invalid")
    return decision, result


def _reason_codes(value: object) -> str:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return "unknown"
    codes = [_escape_text(item) for item in value[:32] if isinstance(item, str)]
    return ", ".join(codes) if codes else "unknown"


def _render_metrics(receipt_digest: str, value: object) -> list[str]:
    if not isinstance(value, Mapping):
        return []
    metrics = cast(Mapping[str, object], value)
    rows: list[str] = []
    for name in _METRICS:
        metric = metrics.get(name)
        if not isinstance(metric, Mapping):
            continue
        aggregate = cast(Mapping[str, object], metric)
        baseline = _escape_text(aggregate.get("baseline", "unknown"))
        candidate = _escape_text(aggregate.get("candidate", "unknown"))
        delta = _escape_text(aggregate.get("candidate_minus_baseline", "unknown"))
        passed = aggregate.get("passed")
        passed_text = "yes" if passed is True else "no" if passed is False else "unknown"
        rows.append(
            "<tr>"
            f"<td><code>{receipt_digest}</code></td>"
            f"<td>{name}</td><td>{baseline}</td><td>{candidate}</td>"
            f"<td>{delta}</td><td>{passed_text}</td>"
            "</tr>"
        )
    return rows


def _escape_text(value: object) -> str:
    if not isinstance(value, str):
        return "unknown"
    cleaned = "".join(
        character
        if unicodedata.category(character) not in {"Cc", "Cf", "Cs"}
        else "\N{REPLACEMENT CHARACTER}"
        for character in value
    )
    encoded = cleaned.encode("utf-8")
    if len(encoded) > _MAX_TEXT_BYTES:
        shortened = encoded[:_MAX_TEXT_BYTES]
        while True:
            try:
                cleaned = shortened.decode("utf-8") + "\N{HORIZONTAL ELLIPSIS}"
                break
            except UnicodeDecodeError:
                shortened = shortened[:-1]
    return html.escape(cleaned, quote=True)


def _validate_served_report(report: bytes) -> None:
    try:
        text = report.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ReportError("REPORT_INVALID", "served report must be valid UTF-8") from exc
    lowered = text.lower()
    forbidden = ("<script", "<style", " href=", " src=", "url(")
    expected_meta = f'<meta http-equiv="content-security-policy" content="{_CSP}">'.lower()
    without_expected_meta = lowered.replace(expected_meta, "", 1)
    if (
        any(token in lowered for token in forbidden)
        or lowered.count(expected_meta) != 1
        or "http-equiv" in without_expected_meta
    ):
        raise ReportError("REPORT_INVALID", "served report violates the static report policy")

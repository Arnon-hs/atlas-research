# SPDX-License-Identifier: MIT
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from atlas_research import cli
from atlas_research.doctor import DoctorCheck, DoctorReport
from atlas_research.qwen import QwenProposal
from atlas_research.worker import WorkerOutcome


def _output(capfd: pytest.CaptureFixture[str]) -> dict[str, object]:
    stdout, stderr = capfd.readouterr()
    assert stderr == ""
    return cast(dict[str, object], json.loads(stdout))


def test_doctor_command_emits_structured_report(
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    report = DoctorReport(
        ok=True,
        platform="Test arm64",
        python="3.11.0",
        checks=(DoctorCheck("python", True, "3.11.0"),),
    )
    monkeypatch.setattr(cli, "run_doctor", lambda *, require_qwen: report)

    assert cli.main(["doctor", "--require-qwen"]) == 0
    assert _output(capfd)["ok"] is True


def test_dataset_freeze_command_creates_a_verified_private_bundle(
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "source.jsonl"
    source.write_text(
        "\n".join(
            json.dumps({"id": f"repo-{index:03d}", "features": {"quality": index}, "label": index})
            for index in range(30)
        )
        + "\n",
        encoding="utf-8",
    )
    output = tmp_path / "bundle"

    assert (
        cli.main(
            [
                "dataset",
                "freeze",
                "--source",
                str(source),
                "--output-root",
                str(output),
                "--dataset-id",
                "sample-v1",
                "--seed",
                "7",
                "--source-producer",
                "atlasrepo-scout",
                "--source-producer-version",
                "test",
                "--source-schema-id",
                "urn:atlasrepo:test:source",
                "--source-schema-version",
                "1.0.0",
            ]
        )
        == 0
    )
    response = _output(capfd)
    assert response["record_count"] == 30
    assert (output / "sample-v1.manifest.json").is_file()
    assert (output.stat().st_mode & 0o777) == 0o700
    assert all((path.stat().st_mode & 0o777) == 0o600 for path in output.iterdir())


def test_qwen_command_emits_only_validated_proposal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    context = tmp_path / "context.json"
    context.write_text(
        json.dumps(
            {
                "variable": "weights.quality",
                "current_value": 1,
                "minimum": 0,
                "maximum": 2,
                "metrics": {"mae": 1.5},
            }
        ),
        encoding="utf-8",
    )

    class FakeProposer:
        def __init__(self, *, timeout_seconds: float) -> None:
            assert timeout_seconds == 5

        def propose(self, _context: object) -> QwenProposal:
            return QwenProposal(
                variable="weights.quality",
                old_value=1,
                new_value=2,
                hypothesis="Raise the bounded quality weight.",
                model="qwen3:8b",
                model_sha256="a" * 64,
                prompt_sha256="b" * 64,
            )

    monkeypatch.setattr(cli, "QwenProposer", FakeProposer)
    assert cli.main(["qwen", "propose", "--context", str(context), "--timeout", "5"]) == 0
    assert _output(capfd)["new_value"] == 2


def test_receipt_verify_and_report_render_commands(
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    receipt_root = tmp_path / "receipts"
    receipt_root.mkdir(mode=0o700)
    assert cli.main(["receipt", "verify", "--root", str(receipt_root)]) == 0
    assert _output(capfd)["entry_count"] == 0

    report = tmp_path / "report.html"
    assert (
        cli.main(
            [
                "report",
                "render",
                "--receipt-root",
                str(receipt_root),
                "--output",
                str(report),
            ]
        )
        == 0
    )
    assert _output(capfd)["receipt_count"] == 0
    assert b"Atlas Research experiment report" in report.read_bytes()


def test_worker_command_forwards_identity_and_reports_noncompleted_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    result_path = tmp_path / "result.json"
    captured: dict[str, object] = {}

    def fake_run(**kwargs: object) -> WorkerOutcome:
        captured.update(kwargs)
        return WorkerOutcome(result={"status": "rejected"}, path=result_path, replayed=False)

    monkeypatch.setattr(cli, "run_isolated_job", fake_run)
    exit_code = cli.main(
        [
            "worker",
            "run",
            "--job",
            "job.json",
            "--artifact-root",
            "artifacts",
            "--output-root",
            "output",
            "--result-uri",
            "result.json",
            "--worker-id",
            "worker-a",
            "--session-id",
            "session-a",
        ]
    )
    assert exit_code == 1
    assert cast(SimpleNamespace, captured["identity"]).worker_id == "worker-a"
    assert _output(capfd)["result"] == {"status": "rejected"}


def test_cli_errors_are_bounded_json(
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    missing = tmp_path / "does-not-exist.json"
    assert cli.main(["qwen", "propose", "--context", str(missing)]) == 2
    stdout, stderr = capfd.readouterr()
    assert stdout == ""
    error = json.loads(stderr)["error"]
    assert error == {
        "code": "INPUT_OPEN_FAILED",
        "message": "Input file could not be opened safely",
    }


def test_cli_rejects_fifo_input_without_waiting_for_a_writer(tmp_path: Path) -> None:
    fifo = tmp_path / "context.json"
    os.mkfifo(fifo, mode=0o600)

    completed = subprocess.run(
        [sys.executable, "-m", "atlas_research", "qwen", "propose", "--context", str(fifo)],
        check=False,
        capture_output=True,
        timeout=2,
    )

    assert completed.returncode == 2
    assert json.loads(completed.stderr)["error"]["code"] == "INPUT_FILE_UNSAFE"


def test_qwen_context_type_error_is_bounded_json(
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    context = tmp_path / "context.json"
    context.write_text(
        json.dumps(
            {
                "variable": 123,
                "current_value": 1,
                "minimum": 0,
                "maximum": 2,
                "metrics": {"mae": 1},
            }
        ),
        encoding="utf-8",
    )

    assert cli.main(["qwen", "propose", "--context", str(context)]) == 2
    stdout, stderr = capfd.readouterr()
    assert stdout == ""
    assert json.loads(stderr)["error"]["code"] == "QWEN_CONTEXT_INVALID"


def test_unexpected_cli_error_never_leaks_traceback(
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli, "run_doctor", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError()))

    assert cli.main(["doctor"]) == 3
    stdout, stderr = capfd.readouterr()
    assert stdout == ""
    assert json.loads(stderr) == {
        "error": {
            "code": "INTERNAL_ERROR",
            "message": "Operation failed without exposing internal details",
        }
    }

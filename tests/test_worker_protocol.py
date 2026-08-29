# SPDX-License-Identifier: MIT
from __future__ import annotations

import json
import os
import resource
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import atlas_research.worker as worker
from atlas_research.errors import ValidationError
from atlas_research.limits import EffectiveLimits


def _limits() -> EffectiveLimits:
    return EffectiveLimits(
        wall_seconds=10,
        max_records=10,
        max_input_bytes=1 << 20,
        max_output_bytes=1 << 20,
        max_workspace_bytes=1 << 20,
        max_peak_rss_bytes=1 << 30,
        max_open_files=32,
        max_json_depth=16,
        max_string_bytes=4_096,
    )


def test_child_main_emits_success_and_safe_error_packets(
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    packet = worker._EvaluationPacket(
        started_at="2026-08-30T00:00:00Z",
        wall_milliseconds=1,
        records_evaluated=1,
        peak_rss_bytes=2,
        canonical_result={
            "metrics": {},
            "all_gates_passed": False,
            "decision": "DISCARD",
            "reason_codes": ["TEST"],
        },
    )
    admitted = object()
    monkeypatch.setattr(worker, "parse_job", lambda _value: admitted)
    monkeypatch.setattr(worker, "_evaluate_loaded_job", lambda _job, _root: packet)
    assert worker.child_main(b"{}\n", Path("root")) == 0
    assert json.loads(capfd.readouterr().out)["ok"] is True

    def rejected(_job: object, _root: Path) -> object:
        raise ValidationError("TEST_REJECTED", "Test rejection")

    monkeypatch.setattr(worker, "_evaluate_loaded_job", rejected)
    assert worker.child_main(b"{}\n", Path("root")) == 2
    error = json.loads(capfd.readouterr().out)["error"]
    assert error == {"code": "TEST_REJECTED", "message": "Test rejection"}

    def crashed(_job: object, _root: Path) -> object:
        raise RuntimeError("sensitive details")

    monkeypatch.setattr(worker, "_evaluate_loaded_job", crashed)
    assert worker.child_main(b"{}\n", Path("root")) == 3
    crash = json.loads(capfd.readouterr().out)
    assert "sensitive" not in json.dumps(crash)


def test_malformed_child_stdout_is_normalized_to_protocol_failure() -> None:
    with pytest.raises(ValidationError) as captured:
        worker._packet_or_error(b"{not-json", _limits())

    assert captured.value.code == "WORKER_PROTOCOL_INVALID"


@pytest.mark.parametrize("system", ["Darwin", "Linux"])
def test_preexec_applies_resource_ceilings_without_using_input(
    system: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, tuple[int, int]]] = []
    monkeypatch.setattr(worker.os, "umask", lambda _mask: 0)
    monkeypatch.setattr(worker.platform, "system", lambda: system)
    monkeypatch.setattr(
        worker.resource,
        "setrlimit",
        lambda name, values: calls.append((cast(int, name), cast(tuple[int, int], values))),
    )

    worker._preexec_limits(_limits())

    assert len(calls) == (5 if system == "Linux" else 4)
    assert calls[0][1] == (0, 0)


def test_linux_preexec_fails_closed_when_address_space_limit_cannot_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker.os, "umask", lambda _mask: 0)
    monkeypatch.setattr(worker.platform, "system", lambda: "Linux")

    def fail_address_space(name: int, _values: tuple[int, int]) -> None:
        if name == resource.RLIMIT_AS:
            raise OSError("setrlimit denied")

    monkeypatch.setattr(worker.resource, "setrlimit", fail_address_space)

    with pytest.raises(OSError, match="setrlimit denied"):
        worker._preexec_limits(_limits())


def test_worker_identity_and_output_path_validation_fail_closed() -> None:
    generated = worker.WorkerIdentity().normalized()
    assert generated.worker_id == "local-worker"
    assert generated.session_id.startswith("session-")
    with pytest.raises(ValidationError, match="WORKER_IDENTITY_INVALID"):
        worker.WorkerIdentity(worker_id="../bad").normalized()
    with pytest.raises(ValidationError, match="OUTPUT_PATH_INVALID"):
        worker._safe_output_relative("../result.json", field="result_uri")


def test_source_provenance_ignores_environment_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ATLAS_RESEARCH_GIT_COMMIT", "b" * 40)
    monkeypatch.setattr(worker, "_INSTALLED_PROVENANCE", tmp_path / "missing")
    monkeypatch.setattr(worker, "_git_commit", lambda: "c" * 40)
    provenance = worker._source_provenance()

    assert provenance.git_commit == "c" * 40
    assert provenance.revision_kind == "verified_checkout"


@pytest.mark.parametrize(
    ("commit", "kind", "digest"),
    [
        ("z" * 40, "verified_checkout", None),
        ("a" * 40, "unknown", None),
        ("a" * 40, "verified_checkout", "b" * 64),
        ("a" * 40, "declared_wheel_revision", None),
        ("a" * 40, "declared_wheel_revision", "z" * 64),
    ],
)
def test_source_provenance_contract_rejects_ambiguous_identity(
    commit: str,
    kind: str,
    digest: str | None,
) -> None:
    with pytest.raises(ValidationError, match="PROVENANCE_UNAVAILABLE"):
        worker.SourceProvenance(commit, kind, digest).normalized()


def test_installed_wheel_provenance_is_bounded_and_root_owned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "source-provenance"
    path.write_text(f"{'a' * 40}\n{'b' * 64}\n", encoding="ascii")
    path.chmod(0o444)
    monkeypatch.setattr(worker, "_INSTALLED_PROVENANCE", path)
    original_fstat = os.fstat

    def root_owned_fstat(descriptor: int) -> SimpleNamespace:
        metadata = original_fstat(descriptor)
        return SimpleNamespace(
            st_mode=metadata.st_mode,
            st_nlink=metadata.st_nlink,
            st_uid=0,
            st_size=metadata.st_size,
        )

    with monkeypatch.context() as context:
        context.setattr(worker.os, "fstat", root_owned_fstat)
        provenance = worker._source_provenance()

    assert provenance == worker.SourceProvenance("a" * 40, "declared_wheel_revision", "b" * 64)

    path.chmod(0o666)
    with pytest.raises(ValidationError, match="unsafe"):
        worker._source_provenance()

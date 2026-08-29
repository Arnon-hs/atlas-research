# SPDX-License-Identifier: MIT
"""Command-line interface for bounded local Atlas Research workflows."""

from __future__ import annotations

import argparse
import hashlib
import os
import stat
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO, Final, cast

from . import __version__
from .artifacts import (
    ArtifactRef,
    atomic_write_private,
    build_artifact_ref,
    ensure_private_directory,
)
from .canonical import canonical_json_bytes, strict_json_loads
from .constants import (
    DATASET_MANIFEST_SCHEMA,
    MAX_ARTIFACT_BYTES,
    MAX_JOB_BYTES,
    MAX_TOTAL_INPUT_BYTES,
    SCHEMA_VERSION,
    SCORING_EXAMPLE_SCHEMA,
)
from .dataset import SplitName, build_dataset_manifest, freeze_jsonl_sources
from .doctor import run_doctor
from .errors import AtlasResearchError, ResourceLimitError, ValidationError
from .qwen import QwenContext, QwenProposer
from .receipts import ReceiptLog
from .report import serve_report, write_report
from .worker import WorkerIdentity, run_isolated_job

_MAX_REPORT_BYTES: Final = 8 << 20


def _safe_read(path: Path, *, maximum: int) -> bytes:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ValidationError(
            "INPUT_OPEN_FAILED", "Input file could not be opened safely"
        ) from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise ValidationError("INPUT_FILE_UNSAFE", "Input must be one regular file")
        if before.st_size > maximum:
            raise ResourceLimitError("INPUT_BYTES_EXCEEDED", "Input file exceeds the byte limit")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(descriptor)
        if len(data) > maximum:
            raise ResourceLimitError("INPUT_BYTES_EXCEEDED", "Input file exceeds the byte limit")
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise ValidationError("INPUT_CHANGED", "Input changed while being read")
        return data
    finally:
        os.close(descriptor)


def _json_mapping(path: Path, *, maximum: int = MAX_JOB_BYTES) -> Mapping[str, object]:
    value = strict_json_loads(_safe_read(path, maximum=maximum), max_bytes=maximum)
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ValidationError("INPUT_JSON_INVALID", "Input JSON must be an object")
    return cast(Mapping[str, object], value)


def _emit(value: object, *, stream: BinaryIO | None = None) -> None:
    output = stream if stream is not None else sys.stdout.buffer
    output.write(canonical_json_bytes(value) + b"\n")
    output.flush()


def _timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _doctor(args: argparse.Namespace) -> int:
    report = run_doctor(require_qwen=cast(bool, args.require_qwen))
    _emit(report.to_dict())
    return 0 if report.ok else 1


def _dataset_freeze(args: argparse.Namespace) -> int:
    output_root = ensure_private_directory(Path(cast(str, args.output_root)))
    source_paths = [Path(value) for value in cast(list[str], args.source)]
    if not 1 <= len(source_paths) <= 64:
        raise ValidationError("DATASET_SOURCES_INVALID", "Dataset source count is invalid")
    source_bytes: list[bytes] = []
    remaining_bytes = MAX_TOTAL_INPUT_BYTES
    for path in source_paths:
        data = _safe_read(path, maximum=min(MAX_ARTIFACT_BYTES, remaining_bytes))
        source_bytes.append(data)
        remaining_bytes -= len(data)
    frozen = freeze_jsonl_sources(source_bytes, seed=cast(int, args.seed))

    source_refs: list[ArtifactRef] = []
    for index, data in enumerate(source_bytes, start=1):
        uri = f"source-{index:03d}-{hashlib.sha256(data).hexdigest()[:12]}.jsonl"
        atomic_write_private(output_root, uri, data)
        source_refs.append(
            build_artifact_ref(
                uri=uri,
                role="source_export",
                media_type="application/x-ndjson",
                data=data,
                producer_name=cast(str, args.source_producer),
                producer_version=cast(str, args.source_producer_version),
                external_schema_id=cast(str, args.source_schema_id),
                external_schema_version=cast(str, args.source_schema_version),
            )
        )

    split_refs: dict[SplitName, ArtifactRef] = {}
    dataset_id = cast(str, args.dataset_id)
    for name, split in frozen.splits.items():
        uri = f"{dataset_id}.{name}.jsonl"
        atomic_write_private(output_root, uri, split.data)
        split_refs[name] = build_artifact_ref(
            uri=uri,
            role="dataset_split",
            media_type="application/x-ndjson",
            data=split.data,
            external_schema_id=SCORING_EXAMPLE_SCHEMA,
            external_schema_version=SCHEMA_VERSION,
        )
    manifest = build_dataset_manifest(
        frozen,
        dataset_id=dataset_id,
        created_at=_timestamp(),
        source_artifacts=source_refs,
        split_artifacts=split_refs,
    )
    manifest_uri = f"{dataset_id}.manifest.json"
    manifest_data = canonical_json_bytes(manifest) + b"\n"
    atomic_write_private(output_root, manifest_uri, manifest_data)
    manifest_ref = build_artifact_ref(
        uri=manifest_uri,
        role="dataset_manifest",
        media_type="application/vnd.atlas-research.dataset-manifest+json",
        data=manifest_data,
        external_schema_id=DATASET_MANIFEST_SCHEMA,
        external_schema_version=SCHEMA_VERSION,
    )
    _emit(
        {
            "dataset_manifest": manifest_ref.to_mapping(),
            "record_count": frozen.source_record_count,
            "splits": {
                name: {"record_count": split.record_count, "sealed": split.sealed}
                for name, split in frozen.splits.items()
            },
        }
    )
    return 0


def _qwen_propose(args: argparse.Namespace) -> int:
    value = _json_mapping(Path(cast(str, args.context)))
    if set(value) != {"variable", "current_value", "minimum", "maximum", "metrics"}:
        raise ValidationError("QWEN_CONTEXT_INVALID", "Qwen context fields are invalid")
    metrics = value["metrics"]
    if not isinstance(metrics, Mapping):
        raise ValidationError("QWEN_CONTEXT_INVALID", "Qwen metrics must be an object")
    context = QwenContext(
        variable=cast(str, value["variable"]),
        current_value=cast(int | float, value["current_value"]),
        minimum=cast(int | float, value["minimum"]),
        maximum=cast(int | float, value["maximum"]),
        metrics=cast(Mapping[str, int | float], metrics),
    )
    proposal = QwenProposer(timeout_seconds=cast(float, args.timeout)).propose(context)
    _emit(
        {
            "variable": proposal.variable,
            "old_value": proposal.old_value,
            "new_value": proposal.new_value,
            "hypothesis": proposal.hypothesis,
            "generator": proposal.generator(),
        }
    )
    return 0


def _worker_run(args: argparse.Namespace) -> int:
    outcome = run_isolated_job(
        job_path=Path(cast(str, args.job)),
        artifact_root=Path(cast(str, args.artifact_root)),
        output_root=Path(cast(str, args.output_root)),
        result_uri=cast(str, args.result_uri),
        receipt_dir=cast(str, args.receipt_dir),
        identity=WorkerIdentity(
            worker_id=cast(str, args.worker_id),
            session_id=cast(str, args.session_id),
        ),
    )
    _emit({"replayed": outcome.replayed, "result": outcome.result})
    return 0 if outcome.result.get("status") == "completed" else 1


def _receipt_verify(args: argparse.Namespace) -> int:
    verification = ReceiptLog(Path(cast(str, args.root))).verify(recover=cast(bool, args.recover))
    _emit(
        {
            "entry_count": verification.entry_count,
            "head_sha256": verification.head_sha256,
            "recovered": verification.recovered,
        }
    )
    return 0


def _report_render(args: argparse.Namespace) -> int:
    receipts = ReceiptLog(Path(cast(str, args.receipt_root))).verified_receipts()
    output = write_report(
        receipts,
        Path(cast(str, args.output)),
        title=cast(str, args.title),
    )
    _emit({"output": str(output), "receipt_count": len(receipts)})
    return 0


def _report_serve(args: argparse.Namespace) -> int:
    report = _safe_read(Path(cast(str, args.file)), maximum=_MAX_REPORT_BYTES)
    server = serve_report(report, port=cast(int, args.port))
    port = cast(tuple[str, int], server.server_address)[1]
    report_path = getattr(server, "report_path", None)
    if not isinstance(report_path, str):  # pragma: no cover - internal invariant
        raise ValidationError("REPORT_INVALID", "Report server path is unavailable")
    _emit({"url": f"http://127.0.0.1:{port}{report_path}"})
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="atlas-research",
        description="Bounded offline research and evaluation for AtlasRepo",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    doctor = commands.add_parser("doctor", help="check local worker readiness")
    doctor.add_argument("--require-qwen", action="store_true")
    doctor.set_defaults(handler=_doctor)

    dataset = commands.add_parser("dataset", help="dataset tooling")
    dataset_commands = dataset.add_subparsers(dest="dataset_command", required=True)
    freeze = dataset_commands.add_parser("freeze", help="freeze deterministic data splits")
    freeze.add_argument("--source", action="append", required=True)
    freeze.add_argument("--output-root", required=True)
    freeze.add_argument("--dataset-id", required=True)
    freeze.add_argument("--seed", type=int, required=True)
    freeze.add_argument("--source-producer", required=True)
    freeze.add_argument("--source-producer-version", required=True)
    freeze.add_argument("--source-schema-id", required=True)
    freeze.add_argument("--source-schema-version", required=True)
    freeze.set_defaults(handler=_dataset_freeze)

    qwen = commands.add_parser("qwen", help="local Qwen hypothesis tooling")
    qwen_commands = qwen.add_subparsers(dest="qwen_command", required=True)
    propose = qwen_commands.add_parser("propose", help="propose one bounded scalar change")
    propose.add_argument("--context", required=True)
    propose.add_argument("--timeout", type=float, default=60.0)
    propose.set_defaults(handler=_qwen_propose)

    worker = commands.add_parser("worker", help="offline experiment worker")
    worker_commands = worker.add_subparsers(dest="worker_command", required=True)
    run = worker_commands.add_parser("run", help="run one isolated JSON job")
    run.add_argument("--job", required=True)
    run.add_argument("--artifact-root", required=True)
    run.add_argument("--output-root", required=True)
    run.add_argument("--result-uri", required=True)
    run.add_argument("--receipt-dir", default="receipt-log")
    run.add_argument("--worker-id", default="local-worker")
    run.add_argument("--session-id", default="")
    run.set_defaults(handler=_worker_run)

    receipt = commands.add_parser("receipt", help="receipt log tooling")
    receipt_commands = receipt.add_subparsers(dest="receipt_command", required=True)
    verify = receipt_commands.add_parser("verify", help="verify the receipt hash-chain")
    verify.add_argument("--root", required=True)
    verify.add_argument("--recover", action="store_true")
    verify.set_defaults(handler=_receipt_verify)

    report = commands.add_parser("report", help="aggregate static reports")
    report_commands = report.add_subparsers(dest="report_command", required=True)
    render = report_commands.add_parser("render", help="render receipt files to HTML")
    render.add_argument("--receipt-root", required=True)
    render.add_argument("--output", required=True)
    render.add_argument("--title", default="Atlas Research experiment report")
    render.set_defaults(handler=_report_render)
    serve = report_commands.add_parser("serve", help="serve one report on loopback")
    serve.add_argument("--file", required=True)
    serve.add_argument("--port", type=int, default=0)
    serve.set_defaults(handler=_report_serve)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run one CLI command with bounded, non-sensitive error output."""

    try:
        args = _parser().parse_args(argv)
        handler = cast(object, args.handler)
        return cast(int, handler(args))  # type: ignore[operator]
    except AtlasResearchError as error:
        _emit({"error": error.as_dict()}, stream=sys.stderr.buffer)
        return 2
    except KeyboardInterrupt:
        _emit(
            {"error": {"code": "INTERRUPTED", "message": "Operation interrupted"}},
            stream=sys.stderr.buffer,
        )
        return 130
    except Exception:
        _emit(
            {
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "Operation failed without exposing internal details",
                }
            },
            stream=sys.stderr.buffer,
        )
        return 3


__all__ = ["main"]

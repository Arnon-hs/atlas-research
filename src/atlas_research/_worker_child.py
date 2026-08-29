# SPDX-License-Identifier: MIT
"""Private entry point for the resource-limited evaluator subprocess."""

from __future__ import annotations

import sys
from pathlib import Path

from .constants import MAX_JOB_BYTES
from .worker import child_main, preflight_main


def main() -> int:
    if len(sys.argv) != 3 or sys.argv[1] not in {"preflight", "evaluate"}:
        return 64
    job_snapshot = sys.stdin.buffer.read(MAX_JOB_BYTES + 1)
    if len(job_snapshot) > MAX_JOB_BYTES:
        return 65
    handler = preflight_main if sys.argv[1] == "preflight" else child_main
    return handler(job_snapshot, Path(sys.argv[2]))


raise SystemExit(main())

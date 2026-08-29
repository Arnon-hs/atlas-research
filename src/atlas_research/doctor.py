# SPDX-License-Identifier: MIT
"""Read-only host readiness checks for the offline research worker."""

from __future__ import annotations

import json
import os
import platform
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from typing import Final

from atlas_research.constants import MAX_RSS_BYTES
from atlas_research.qwen import QwenError, _LoopbackTransport

OLLAMA_VERSION_URL: Final = "http://127.0.0.1:11434/api/version"
OLLAMA_TAGS_URL: Final = "http://127.0.0.1:11434/api/tags"
QWEN_MODEL: Final = "qwen3:8b"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class DoctorCheck:
    name: str
    ok: bool
    detail: str


@dataclass(frozen=True)
class DoctorReport:
    ok: bool
    platform: str
    python: str
    checks: tuple[DoctorCheck, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "platform": self.platform,
            "python": self.python,
            "checks": [asdict(check) for check in self.checks],
        }


def _physical_memory_bytes() -> int | None:
    try:
        return int(os.sysconf("SC_PHYS_PAGES")) * int(os.sysconf("SC_PAGE_SIZE"))
    except (OSError, TypeError, ValueError):
        return None


def _get_json(url: str, *, max_bytes: int = 262_144, timeout: float = 3.0) -> object:
    paths = {OLLAMA_VERSION_URL: "/api/version", OLLAMA_TAGS_URL: "/api/tags"}
    path = paths.get(url)
    if path is None:
        raise ValueError("doctor endpoint is not allowlisted")
    response = _LoopbackTransport().request(
        "GET",
        path,
        None,
        timeout_seconds=timeout,
        max_response_bytes=max_bytes,
    )
    if response.status != 200:
        raise OSError("Ollama health request failed")
    media_type = response.content_type.partition(";")[0].strip().lower()
    if media_type != "application/json":
        raise OSError("Ollama health response is not JSON")
    if len(response.body) > max_bytes:
        raise OSError("Ollama health response exceeded the byte limit")
    return json.loads(response.body)


def _ollama_listener_check() -> DoctorCheck:
    lsof = "/usr/sbin/lsof"
    if not os.path.isfile(lsof):
        return DoctorCheck("ollama_listener", False, "listener inspection unavailable")
    try:
        completed = subprocess.run(
            [lsof, "-nP", "-iTCP:11434", "-sTCP:LISTEN"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
            env={"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"},
        )
    except (OSError, subprocess.SubprocessError):
        return DoctorCheck("ollama_listener", False, "listener inspection failed")
    if completed.returncode != 0:
        return DoctorCheck("ollama_listener", False, "Ollama listener not found")
    lines = [line for line in completed.stdout.splitlines()[1:] if line.strip()]
    if not lines:
        return DoctorCheck("ollama_listener", False, "Ollama listener not found")
    addresses = [line.split()[-2] for line in lines if len(line.split()) >= 2]
    loopback = all(
        address.startswith(("127.0.0.1:11434", "[::1]:11434", "localhost:11434"))
        for address in addresses
    )
    return DoctorCheck(
        "ollama_listener",
        loopback,
        "loopback-only" if loopback else "listener is not loopback-only",
    )


def run_doctor(*, require_qwen: bool = False) -> DoctorReport:
    checks: list[DoctorCheck] = []
    python_ok = sys.version_info >= (3, 11)
    checks.append(DoctorCheck("python", python_ok, platform.python_version()))

    memory = _physical_memory_bytes()
    memory_ok = memory is not None and memory > MAX_RSS_BYTES + (2 << 30)
    memory_detail = "unknown" if memory is None else f"{memory // (1 << 30)} GiB physical"
    checks.append(DoctorCheck("memory_headroom", memory_ok, memory_detail))

    ollama_ok = False
    try:
        version = _get_json(OLLAMA_VERSION_URL)
        if not isinstance(version, dict) or not isinstance(version.get("version"), str):
            raise ValueError("invalid Ollama version response")
        ollama_ok = True
        checks.append(DoctorCheck("ollama", True, f"version {version['version']}"))
    except (OSError, QwenError, ValueError, TypeError, json.JSONDecodeError):
        checks.append(DoctorCheck("ollama", not require_qwen, "unavailable (optional)"))

    if ollama_ok:
        checks.append(_ollama_listener_check())
        try:
            tags = _get_json(OLLAMA_TAGS_URL)
            models = tags.get("models") if isinstance(tags, dict) else None
            found = False
            digest = ""
            if isinstance(models, list):
                for model in models:
                    if not isinstance(model, dict):
                        continue
                    if model.get("name") == QWEN_MODEL or model.get("model") == QWEN_MODEL:
                        value = model.get("digest")
                        if isinstance(value, str) and _DIGEST.fullmatch(value):
                            found = True
                            digest = value
                            break
            checks.append(
                DoctorCheck(
                    "qwen_model",
                    found or not require_qwen,
                    f"{QWEN_MODEL} digest {digest[:12]}" if found else "qwen3:8b unavailable",
                )
            )
        except (OSError, QwenError, ValueError, TypeError, json.JSONDecodeError):
            checks.append(
                DoctorCheck("qwen_model", not require_qwen, "model inventory unavailable")
            )

    return DoctorReport(
        ok=all(check.ok for check in checks),
        platform=f"{platform.system()} {platform.machine()}",
        python=platform.python_version(),
        checks=tuple(checks),
    )

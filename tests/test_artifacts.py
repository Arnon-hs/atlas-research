# SPDX-License-Identifier: MIT
from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from atlas_research.artifacts import (
    ArtifactExpectation,
    ArtifactRef,
    ArtifactResolver,
    atomic_write_private,
    build_artifact_ref,
    ensure_private_directory,
)
from atlas_research.constants import SCHEMA_VERSION, SCORING_EXAMPLE_SCHEMA
from atlas_research.errors import ConflictError, ValidationError


def _opaque_ref(uri: str, data: bytes) -> ArtifactRef:
    return build_artifact_ref(
        uri=uri,
        role="opaque",
        media_type="application/octet-stream",
        data=data,
        producer_name="fixture",
        producer_version="1.0.0",
    )


def _private_dir(path: Path) -> Path:
    path.mkdir(mode=0o700)
    return path


def test_artifact_reference_enforces_role_media_schema_and_size() -> None:
    data = b"{}\n"
    reference = build_artifact_ref(
        uri="splits/train.jsonl",
        role="dataset_split",
        media_type="application/x-ndjson",
        data=data,
        external_schema_id=SCORING_EXAMPLE_SCHEMA,
        external_schema_version=SCHEMA_VERSION,
    )
    assert ArtifactRef.from_mapping(reference.to_mapping()) == reference
    changed = reference.to_mapping()
    changed["media_type"] = "application/json"
    with pytest.raises(ValidationError, match="ARTIFACT_MEDIA_TYPE_MISMATCH"):
        ArtifactRef.from_mapping(changed)


@pytest.mark.parametrize(
    "uri,code",
    [
        ("../outside", "ARTIFACT_URI_INVALID"),
        ("safe/%2e%2e/outside", "ARTIFACT_URI_INVALID"),
        ("safe/file?query=1", "ARTIFACT_URI_INVALID"),
        ("safe\\file", "ARTIFACT_URI_INVALID"),
        ("bundle.zip/member", "ARTIFACT_ARCHIVE_REJECTED"),
    ],
)
def test_artifact_reference_rejects_unsafe_locators(uri: str, code: str) -> None:
    with pytest.raises(ValidationError, match=code):
        _opaque_ref(uri, b"data")


def test_resolver_verifies_same_open_file_bytes_and_json(tmp_path: Path) -> None:
    root = _private_dir(tmp_path / "inputs")
    data = b'{"ok":true}'
    (root / "item.json").write_bytes(data)
    reference = build_artifact_ref(
        uri="item.json",
        role="evaluation_payload",
        media_type="application/json",
        data=data,
        producer_name="fixture",
        producer_version="1.0.0",
        external_schema_id="urn:example:evaluator",
        external_schema_version="1.0.0",
    )
    with ArtifactResolver(root) as resolver:
        resolved = resolver.resolve(
            reference,
            ArtifactExpectation(
                role="evaluation_payload",
                media_type="application/json",
                external_schema_id="urn:example:evaluator",
                external_schema_version="1.0.0",
            ),
            parse_json=True,
        )
    assert resolved.data == data
    assert resolved.json_value == {"ok": True}


def test_resolver_rejects_role_digest_size_links_and_non_regular_files(tmp_path: Path) -> None:
    root = _private_dir(tmp_path / "inputs")
    data = b"evidence"
    source = root / "source.bin"
    source.write_bytes(data)
    reference = _opaque_ref("source.bin", data)
    with (
        ArtifactResolver(root) as resolver,
        pytest.raises(ValidationError, match="ARTIFACT_ROLE_MISMATCH"),
    ):
        resolver.resolve(reference, ArtifactExpectation(role="report"))

    bad_digest = reference.to_mapping()
    bad_digest["sha256"] = "0" * 64
    with (
        ArtifactResolver(root) as resolver,
        pytest.raises(ValidationError, match="ARTIFACT_DIGEST_MISMATCH"),
    ):
        resolver.resolve(bad_digest, ArtifactExpectation(role="opaque"))

    hardlink = root / "hard.bin"
    os.link(source, hardlink)
    with (
        ArtifactResolver(root) as resolver,
        pytest.raises(ValidationError, match="ARTIFACT_LINK_REJECTED"),
    ):
        resolver.resolve(_opaque_ref("hard.bin", data), ArtifactExpectation(role="opaque"))

    symlink = root / "link.bin"
    symlink.symlink_to(source)
    with (
        ArtifactResolver(root) as resolver,
        pytest.raises(ValidationError, match="ARTIFACT_OPEN_FAILED"),
    ):
        resolver.resolve(_opaque_ref("link.bin", data), ArtifactExpectation(role="opaque"))

    fifo = root / "pipe.bin"
    os.mkfifo(fifo)
    with (
        ArtifactResolver(root) as resolver,
        pytest.raises(ValidationError, match="ARTIFACT_NOT_REGULAR"),
    ):
        resolver.resolve(_opaque_ref("pipe.bin", b""), ArtifactExpectation(role="opaque"))


def test_resolver_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    root = _private_dir(tmp_path / "inputs")
    data = b'{"a":1,"a":2}'
    (root / "duplicate.json").write_bytes(data)
    reference = build_artifact_ref(
        uri="duplicate.json",
        role="evaluation_payload",
        media_type="application/json",
        data=data,
        producer_name="fixture",
        producer_version="1.0.0",
        external_schema_id="urn:example:evaluator",
        external_schema_version="1.0.0",
    )
    with (
        ArtifactResolver(root) as resolver,
        pytest.raises(ValidationError, match="JSON_DUPLICATE_KEY"),
    ):
        resolver.resolve(
            reference,
            ArtifactExpectation(role="evaluation_payload"),
            parse_json=True,
        )


def test_private_atomic_write_is_exclusive_and_does_not_follow_target(tmp_path: Path) -> None:
    root = ensure_private_directory(tmp_path / "outputs")
    target = atomic_write_private(root, "result.json", b"first")
    assert target.read_bytes() == b"first"
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    with pytest.raises(ConflictError, match="OUTPUT_EXISTS"):
        atomic_write_private(root, "result.json", b"second")

    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"outside")
    link = root / "link.json"
    link.symlink_to(outside)
    with pytest.raises(ConflictError, match="OUTPUT_EXISTS"):
        atomic_write_private(root, "link.json", b"inside")
    assert outside.read_bytes() == b"outside"
    atomic_write_private(root, "link.json", b"inside", overwrite=True)
    assert not link.is_symlink()
    assert link.read_bytes() == b"inside"
    assert outside.read_bytes() == b"outside"


def test_private_directory_rejects_public_permissions(tmp_path: Path) -> None:
    root = tmp_path / "public"
    root.mkdir(mode=0o755)
    root.chmod(0o755)
    with pytest.raises(ValidationError, match="OUTPUT_ROOT_NOT_PRIVATE"):
        ensure_private_directory(root)

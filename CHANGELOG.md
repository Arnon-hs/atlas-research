# Changelog

All notable changes to Atlas Research are documented here. The project follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-08-30

### Added

- Versioned artifact, dataset, benchmark, candidate, job, result, and receipt
  JSON Schemas.
- Deterministic `sha256-id-v1` train/validation/sealed-test dataset tooling.
- Research-only single-variable linear evaluator and seven bounded metrics.
- Parent-watchdog offline worker with immutable JSON result artifacts.
- Private append-only hash-chained receipts in the offline evaluator with exact
  idempotent replay.
- Optional loopback-only `qwen3:8b` structured hypothesis generation.
- Static aggregate-only review report and loopback GET/HEAD server.
- Public CI, CodeQL, dependency review, Dependabot, DCO, and community policy.
- Experimental outbound Scout worker client with strict session, claim,
  heartbeat, cancellation, same-origin artifact, and fenced terminal-result
  contracts. The client remains inert without operator configuration and a
  compatible Scout-owned controller.
- Atomic, sanitized worker telemetry derived only from Scout queue state and
  confirmed execution receipts for an explicitly configured private path.
- Remote protocol v1 transfers canonical result JSON and its receipt reference,
  not the receipt body; durable cross-host receipt-body retention is outside
  v0.1.
- Reproducible wheel and source-distribution release builds under the
  `atlasrepo-research` distribution name while preserving the
  `atlas_research` import and `atlas-research` command.
- Immutable GitHub Releases with checksums, an exact image-digest record, a
  source SPDX JSON SBOM, GitHub attestations, and a digest-addressable
  `linux/amd64` plus `linux/arm64` GHCR image. PyPI and mutable `latest` image
  publication remain intentionally absent.

[Unreleased]: https://github.com/Arnon-hs/atlas-research/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Arnon-hs/atlas-research/releases/tag/v0.1.0

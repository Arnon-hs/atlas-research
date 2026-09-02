# Atlas Research

[![Public quality gates](https://github.com/Arnon-hs/atlas-research/actions/workflows/ci.yml/badge.svg)](https://github.com/Arnon-hs/atlas-research/actions/workflows/ci.yml)
[![CodeQL](https://github.com/Arnon-hs/atlas-research/actions/workflows/codeql.yml/badge.svg)](https://github.com/Arnon-hs/atlas-research/actions/workflows/codeql.yml)
[![GitHub release](https://img.shields.io/github/v/release/Arnon-hs/atlas-research?display_name=tag)](https://github.com/Arnon-hs/atlas-research/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Atlas Research is the open-source, artifact-first research plane for the
AtlasRepo ecosystem. It turns pinned evidence into reproducible datasets,
evaluates one-variable scoring hypotheses, and emits immutable experiment
receipts for human review.

> **Status:** alpha, versioned as v0.2.0. The portable core, isolated worker,
> receipts, and local review report are tested. The outbound client is
> experimental, disabled by default, and inert without explicit enrollment by a
> compatible Scout controller; production import and activation remain
> intentionally absent. A commit or container tag is not a release unless the
> matching immutable GitHub Release is published. Nothing produced by this
> repository authorizes automatic production activation.

## Why this exists

AtlasRepo already has clear runtime owners:

- **AtlasRepo Scout** owns discovery, production queues, scoring definitions,
  embeddings, previews, activation, rollback, and backfills.
- **Atlas Engine** produces deterministic repository evidence through a pinned
  command-line boundary.
- **AtlasRepo Schema** owns deployment topology, service profiles, networks, and
  secret names.
- **Atlas Research** owns immutable research artifacts and offline evaluation.

This repository adds a bounded experimentation loop without creating a second
search stack, worker control plane, vector store, or scoring authority.

## Intended workflow

```text
Scout/Engine pinned evidence
          |
          v
immutable dataset -> one-variable candidate -> bounded evaluation
                                                |
                                  KEEP / DISCARD / ERROR receipt
                                                |
                                     explicit human review
                                                |
                              optional Scout import and preview
```

`KEEP` means that a candidate passed the declared offline gates. It does not
mean deploy, publish, activate, or replace a production scoring model.

## Current scope

- content-addressed artifact references;
- deterministic dataset splits and manifests;
- benchmark and one-variable candidate manifests;
- bounded, multi-metric offline evaluation;
- append-only, hash-chained experiment receipts;
- an offline JSON-in/JSON-out worker for a least-privilege Mac mini;
- a single-concurrency outbound client for Scout-owned research leases;
- a separate single-concurrency client for Scout-owned production description
  and Atlas Score generation leases;
- optional local Qwen/Ollama hypothesis generation;
- a static local report for review.

Out of scope are production databases, Redis/BullMQ, embeddings, a remote
worker controller, model activation, public API changes, and deployment
topology. Scout owns the controller; AtlasRepo Schema owns service topology.

## Quick start

Requirements: Python 3.11+ and
[uv](https://docs.astral.sh/uv/getting-started/installation/). Ollama and
`qwen3:8b` are optional for the research hypothesis command and required only
when an operator deliberately runs the production-generation client.

```bash
git clone https://github.com/Arnon-hs/atlas-research.git
cd atlas-research
uv sync --locked --all-groups
uv run atlas-research --version
uv run atlas-research doctor
make check
```

Freeze a private, deterministic dataset bundle from the synthetic example:

```bash
mkdir -m 700 /tmp/atlas-research-bundle
uv run atlas-research dataset freeze \
  --source examples/scoring-records.jsonl \
  --output-root /tmp/atlas-research-bundle \
  --dataset-id example-v1 --seed 7 \
  --source-producer demo-fixture --source-producer-version 1.0.0 \
  --source-schema-id urn:atlasrepo:example:scoring-export \
  --source-schema-version 1.0.0
```

See [Getting started](docs/getting-started.md) for Qwen, worker, receipt, report,
and Docker commands. Public JSON Schemas live in [`schemas/v1`](schemas/v1).

## Release artifacts

The installable Python distribution is named `atlasrepo-research`; the Python
import remains `atlas_research` and the command remains `atlas-research`. The
project is not published to PyPI. A tagged release publishes only immutable
GitHub Release assets and the matching multi-architecture image:

- wheel and source distribution;
- `SHA256SUMS`, an exact image-digest record, and a source SPDX JSON SBOM;
- GitHub build-provenance and SBOM attestations;
- `ghcr.io/arnon-hs/atlas-research:v0.2.0` for `linux/amd64` and
  `linux/arm64`, with BuildKit provenance and SBOM attestations.

No mutable `latest` image tag is published. Operators should verify the GitHub
attestation and use the checksummed `sha256` identity shipped with the release.
See [Getting started](docs/getting-started.md#verify-a-published-release) for
exact commands.

## Safety model

- artifact paths are confined, no-follow, digest-pinned, and size-bounded;
- JSON parsing rejects duplicate keys, non-finite values, deep trees, and
  oversized strings;
- the worker accepts data only and never executes repository scripts, hooks,
  lifecycle commands, candidate code, or arbitrary binaries;
- one subprocess runs at a time under wall-time, file-size, open-file, and
  platform-appropriate memory ceilings;
- receipt and result commits are admitted only when their atomic temporary and
  durable bytes fit the reduced workspace ceiling;
- the container profile is designed for `--network none`; local Qwen runs as a
  separate loopback-only proposal step;
- the outbound supervisor accepts only same-origin, digest-pinned data objects,
  keeps its enrollment credential outside the repository, and invokes one
  operator-pinned local executor rather than a server-supplied command;
- the production-generation supervisor accepts only two fixed workloads, binds
  Scout's assignment digest, uses exact loopback Qwen, and keeps every database
  write, fallback, scheduling, and ScoreCard identity decision in Scout;
- remote completion sends canonical result JSON and a receipt reference, not
  the receipt body; v0.1 does not claim durable cross-host receipt retention;
- receipts are private, append-only, hash-chained, and idempotency-bound;
- sealed test evaluation fails closed in v0.1 until an external operator
  capability is implemented;
- `KEEP` means only “all offline benchmark gates passed.”

## Architecture and contracts

- [Atlas Intelligence architecture](docs/architecture/atlas-intelligence-platform.md)
- [ADR-001: artifact-first research plane](docs/architecture/adr-001-artifact-first-research-plane.md)
- [ADR-002: KEEP is not promotion](docs/architecture/adr-002-keep-is-not-promotion.md)
- [ADR-003: Scout owns the remote worker control plane](docs/architecture/adr-003-scout-owned-remote-worker-control-plane.md)
- [Portable contract policy](docs/contracts/README.md)
- [Getting started](docs/getting-started.md)
- [Worker and threat boundaries](docs/worker-security.md)
- [Scout worker client](docs/scout-worker-client.md)
- [Production generation worker](docs/production-generation-worker.md)

## Inspiration

The bounded loop is inspired by
[karpathy/autoresearch](https://github.com/karpathy/autoresearch): change one
controlled variable, run for a fixed budget, measure, and keep or discard the
result. Atlas Research independently implements that pattern for AtlasRepo's
multi-metric, artifact-based workflow; it does not copy autoresearch source
code. Lease and fencing ideas were independently adapted from
[jit-runner-kit](https://github.com/Arnon-hs/jit-runner-kit), without copying
its provider or runner implementation.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md), the
[Code of Conduct](CODE_OF_CONDUCT.md), and [SECURITY.md](SECURITY.md) before
opening a change. `make check` is the local parity command for lint, formatting,
strict typing, tests with branch coverage, dependency audit, and package build.

Project process: [Roadmap](ROADMAP.md) · [Governance](GOVERNANCE.md) ·
[Maintainers](MAINTAINERS.md)

## License

[MIT](LICENSE). This is technical software, not a legal or operational promise.

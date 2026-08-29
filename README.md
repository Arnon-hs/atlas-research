# Atlas Research

Atlas Research is the open-source, artifact-first research plane for the
AtlasRepo ecosystem. It turns pinned evidence into reproducible datasets,
evaluates one-variable scoring hypotheses, and emits immutable experiment
receipts for human review.

> **Status:** pre-0.1 and experimental. The architecture and portable contracts
> are being established before any production integration. Nothing produced by
> this repository is authorized for automatic production activation.

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
                                      KEEP or DISCARD receipt
                                                |
                                     explicit human review
                                                |
                              optional Scout import and preview
```

`KEEP` means that a candidate passed the declared offline gates. It does not
mean deploy, publish, activate, or replace a production scoring model.

## v0.1 scope

- content-addressed artifact references;
- deterministic dataset splits and manifests;
- benchmark and one-variable candidate manifests;
- bounded, multi-metric offline evaluation;
- append-only, hash-chained experiment receipts;
- an offline JSON-in/JSON-out worker for a least-privilege Mac mini;
- optional local Qwen/Ollama hypothesis generation;
- a static local report for review.

Out of scope are production databases, Redis/BullMQ, embeddings, a generic
remote worker controller, model activation, public API changes, and deployment
topology.

## Architecture and contracts

- [Atlas Intelligence architecture](docs/architecture/atlas-intelligence-platform.md)
- [ADR-001: artifact-first research plane](docs/architecture/adr-001-artifact-first-research-plane.md)
- [ADR-002: KEEP is not promotion](docs/architecture/adr-002-keep-is-not-promotion.md)
- [ADR-003: Scout owns the remote worker control plane](docs/architecture/adr-003-scout-owned-remote-worker-control-plane.md)
- [Portable contract policy](docs/contracts/README.md)

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
opening a change. Developer setup and commands will be added with the first
executable v0.1 implementation.

Project process: [Roadmap](ROADMAP.md) · [Governance](GOVERNANCE.md) ·
[Maintainers](MAINTAINERS.md)

## License

[MIT](LICENSE). This is technical software, not a legal or operational promise.

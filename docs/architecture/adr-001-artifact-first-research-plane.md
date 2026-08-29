# ADR-001: Artifact-first research plane

Status: accepted
Date: 2026-08-30

## Context

AtlasRepo needs reproducible experiments across service boundaries. Direct
database access or a new shared service would duplicate ownership, expand the
failure domain, and make experiments depend on mutable production state.

## Decision

Atlas Research is an artifact-first Python 3.11+ application. Its runtime uses
the standard library for the initial portable core; `uv` manages development
and repeatable commands. Inputs and outputs are immutable files identified by
SHA-256 and versioned JSON Schemas.

The initial worker is offline JSON-in/JSON-out. It has no production database,
Redis, deployment, or generic queue integration. Other services exchange only
pinned artifacts and retain their runtime schemas and mutable state.

## Consequences

- Experiments can be reproduced and audited without production access.
- The same worker can run on macOS or Linux with a small supply-chain surface.
- Large artifacts need an external content store in later phases.
- Live production feedback requires an explicit export step.
- Remote orchestration is deferred until the canonical Scout control plane owns
  it.

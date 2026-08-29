# ADR-003: Scout owns the remote worker control plane

Status: accepted
Date: 2026-08-30

## Context

A persistent Mac mini can provide economical local inference, but a second job
controller would duplicate Scout scheduling and introduce ambiguous authority.
Patterns from `jit-runner-kit` demonstrate useful lease and fencing semantics,
while its GitHub JIT/provider implementation and privileged runner model do not
fit this trust boundary.

## Decision

Atlas Research defines portable `research.experiment` job and result payloads
and implements an offline runner. It does not expose claim, heartbeat, cancel,
or result-commit endpoints.

If remote execution is later approved, Scout owns those endpoints and mutable
job state. The protocol must use atomic claim, worker/session identity,
monotonic attempt and fencing tokens, heartbeat renewal, idempotent terminal
commit, cancellation, expiry rejection, and independent reconciliation.

The Mac mini initiates outbound traffic, runs one job at a time, uses loopback
Ollama, and receives no production DB, Redis, deploy, billing, or GitHub
administration secrets.

## Consequences

- v0.1 can be verified without inventing a premature distributed system.
- Research payloads remain reusable when Scout later adds remote orchestration.
- Remote execution cannot ship until the Scout API and topology pass their own
  design, security, and failure-mode review.

# ADR-002: KEEP is not promotion

Status: accepted
Date: 2026-08-30

## Context

A bounded experiment needs a simple terminal decision. Reusing that decision as
a deployment signal would collapse offline evidence, editorial review, runtime
compatibility, rollout, and rollback into one unsafe action.

## Decision

`KEEP` means only that every declared offline benchmark gate passed for the
pinned candidate and dataset. Atlas Research has no activation API and never
sets an active production scorer.

Production use requires a separate human-reviewed Scout workflow: canonical
parsing, preview, approval, activation, observation, and rollback. Scout records
its own audit evidence and may reject any Research candidate.

## Consequences

- Offline iteration remains fast without weakening production approval.
- A candidate can be `KEEP` and still be rejected for compatibility, policy,
  fairness, security, editorial, or operational reasons.
- Receipts must use unambiguous research vocabulary and cannot expose an
  `active` status.

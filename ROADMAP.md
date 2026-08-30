# Roadmap

The roadmap describes direction, not a delivery promise.

## v0.1 — local artifact loop

- versioned portable schemas;
- deterministic dataset freezing;
- bounded candidate evaluation and immutable receipts;
- offline JSON-in/JSON-out worker;
- opt-in Scout-owned leased worker client and sanitized telemetry projection;
- optional loopback Qwen proposer;
- static local review report;
- public CI, security, and license checks.

## v0.2 — Scout-reviewed exchange

- Scout-owned export and import adapters;
- explicit preview-only candidate handoff;
- human review receipts and rejection reasons;
- durable receipt-body export and retention before remote results are treated
  as a long-lived human-review archive;
- schema compatibility fixtures shared by artifact, not copied runtime logic.
- operational hardening of least-privilege outbound Mac/Linux enrollment;
- stable AtlasRepo Schema launchd rollout around the network-free one-shot
  evaluator.

## Later, only after separate approval

- production inference workload routing and server fallback policy;
- Admin research review surfaces;
- public Web score explanations based only on published Platform APIs.

Production auto-promotion, a second vector store, and direct worker access to
production databases remain non-goals.

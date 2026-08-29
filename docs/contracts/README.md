# Portable contract policy

Schemas in `schemas/v1/` describe only artifacts owned by Atlas Research. They
use JSON Schema Draft 2020-12 and stable `urn:atlasrepo:atlas-research:...`
identifiers.

## Compatibility

- Adding an optional property is backward-compatible.
- Removing or renaming a property, changing meaning, or narrowing accepted
  values requires a new major schema version.
- Every manifest declares `schema_version` and rejects unknown properties.
- Digests are lowercase SHA-256 hex over the exact referenced bytes.
- Timestamps use RFC 3339 UTC (`Z`) in canonical output.
- Consumers must validate both the schema and content digests.
- External schema IDs are opaque provenance; validators use only bundled,
  allowlisted schemas and never fetch a `$ref` over the network.

`experiment-receipt.v1.canonical_result_sha256` is SHA-256 over the UTF-8
[RFC 8785 JSON Canonicalization Scheme](https://www.rfc-editor.org/rfc/rfc8785)
bytes of the nested `canonical_result` value, with no trailing newline. That
subtree contains no JSON number values: metric numbers use one normalized
decimal string spelling (zero is exactly `0`; no exponent, leading zero,
negative zero, or trailing fractional zero; at most 12 fractional digits).

Metric arithmetic uses decimal context precision 50 with round-half-even.
Baseline and candidate values are quantized to `1e-12`, then trailing fractional
zeros are removed. `candidate_minus_baseline` is recomputed from those two
quantized decimal values, quantized once more by the same rule, and never trusted
from a producer without verification. Gate improvement is
`candidate_minus_baseline` for a `higher` metric and its negation for `lower`.

An offline result with `status: completed` means that a terminal receipt was
committed; its receipt decision may be `KEEP`, `DISCARD`, or `ERROR`. Rejected,
cancelled, and expired jobs are pre-admission/execution failures and contain a
bounded error but no receipt.

In `dataset-manifest.v1`, `sealed` describes research visibility rather than
content mutability: train and validation are `false`, while test is always
`true`. v0.1 uses the single deterministic `0.8 / 0.1 / 0.1` split policy.
Every split artifact remains immutable and digest-pinned.
The exact record identity, seed encoding, hash mapping, boundaries, ordering,
and verification algorithm are defined in
[Deterministic dataset split v1](dataset-split-v1.md).
Checks that cannot be expressed completely in JSON Schema are mandatory and
versioned in [Semantic validation v1](semantic-validation-v1.md).

## Owned schemas

| Schema | Purpose |
| --- | --- |
| `artifact-ref.v1` | Reference immutable bytes, producer identity, and an optional external schema |
| `dataset-manifest.v1` | Pin sources and deterministic split artifacts |
| `benchmark-manifest.v1` | Declare metrics, gates, budgets, and baseline |
| `candidate-artifact.v1` | Propose one target-variable change over an offline evaluation payload |
| `experiment-receipt.v1` | Record immutable evaluation evidence and decision |
| `research-experiment-job.v1` | Request one bounded offline experiment |
| `research-experiment-result.v1` | Return a bounded offline result |

## Deliberately external contracts

This repository does not redefine Scout events, queue rows, scoring runtime
definitions, embeddings, search indexes, golden-set databases, Atlas Engine
output schemas, production activation, or deployment topology. An `artifact-ref`
may name one of those external schemas by ID and version without copying it.

The job/result pair is not a generic ecosystem worker protocol and does not
define a network control plane. Mutable lease lifecycle remains Scout-owned.

See the [cross-repository registry](cross-repository-registry.md) and the
[non-canonical Atlas Score/feedback proposal](atlas-score-and-feedback-proposal.md)
for phase-1 ownership and compatibility decisions.

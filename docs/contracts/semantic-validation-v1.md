# Semantic validation v1

JSON Schema validates shape. A v0.1 consumer must also run these deterministic
checks before evaluation or receipt commit.

## Artifact roles

Each field accepts only its declared `artifact-ref.role`. The resolver verifies
role, media type, producer identity, external schema ID/version where required,
size, and digest before parsing. An artifact that is structurally valid for a
different role is rejected.

## Dataset

- Source IDs are unique.
- The fixed ratios and integer bucket algorithm match
  `dataset-split-v1` exactly.
- Recomputed membership, output ordering, counts, and split digests match the
  manifest.
- Train, validation, and test ID sets are pairwise-disjoint.
- Test is sealed; train and validation are not sealed.

## Benchmark

Metric keys are structurally unique and their direction/range is fixed by the
schema. Parameters are allowlisted by metric:

| Metric | Allowed parameters |
| --- | --- |
| `mae`, `spearman`, `ndcg_at_10`, `ndcg_at_50` | none |
| `pairwise_accuracy` | `max_pairs` only |
| `f1` | `threshold` only |
| `calibration_error` | `bins` only |

Minimum record counts must be satisfied for every split. Job limits are reduced
to `min(job, benchmark, operator ceiling, host-safe ceiling)` and can never
raise an operator limit.

## Candidate

The validator selects a bundled, allowlisted offline evaluator from the
`evaluation_payload.external_schema` identity, loads the strict parent and
proposed payloads, recursively compares them, and requires exactly one differing
scalar leaf. The leaf path
must equal `changed_variable.path`; its parent value and proposed value must
equal `old_value` and `new_value`, have the same permitted scalar type, differ
after canonical comparison, be finite where numeric, and fall within the
allowlisted variable range. Any added/removed key, type change, zero change, or
second hidden change is rejected.

The v0.1 synthetic linear evaluator format is research-only test/evaluation
data. It is not a Scout `ScoringModelDefinition`, is not a production contract,
and cannot be imported or activated. Unknown payload schemas fail closed.

## Job and receipt

- The job's dataset and baseline evaluation refs exactly equal the refs inside
  the admitted benchmark; its split equals the benchmark split.
- The admitted candidate ref resolves to bytes whose parent evaluation ref
  exactly equals the job baseline ref. The candidate bytes and digest are the
  same bytes named by the job, not a lookup by mutable ID.
- Job and benchmark evaluation splits match.
- A test job has explicit review authorization; a validation job has none.
- Deadline is valid before work and immediately before receipt commit.
- Receipt metric keys match the benchmark exactly.
- Every metric uses decimal precision 50 and round-half-even quantization from
  the contract policy. The validator recomputes baseline, candidate,
  `candidate_minus_baseline`, direction-adjusted improvement, and gate result;
  it never trusts producer arithmetic.
- `KEEP` requires every gate to pass; `DISCARD` requires at least one failed
  gate; `ERROR` is never marked as all-gates-passed.
- Result `job_id`, `attempt`, `idempotency_key`, and `job_spec_sha256` match the
  exact job. Receipt job identity/digest, evaluation split, and every input ref
  match that job; the result receipt ref matches the exact committed bytes.
- `created_at <= started_at <= finished_at <= deadline` and receipt creation is
  within the execution interval. These are parsed as RFC 3339 UTC instants.
- Non-completed results have no receipt in either `receipt` or `artifacts`;
  completed results have exactly the dedicated receipt ref.
- `canonical_result_sha256`, job digest, artifact digests, and previous receipt
  digest are recomputed from exact bytes.

# Atlas Intelligence Platform architecture

Status: accepted for the isolated Atlas Research v0.1 after independent
architecture and security review; cross-repository integrations remain proposed
Date: 2026-08-30

## Outcome

Atlas Intelligence is a staged capability inside the existing AtlasRepo
ownership model, not a new monolith. Scout remains the production intelligence
control plane, Atlas Engine remains a deterministic evidence tool, AtlasRepo
Schema remains the topology authority, and Atlas Research adds reproducible
offline experimentation.

## Current-state evidence

This is a public-safe discovery snapshot, not a deployment claim.

| Repository/capability | Observed baseline | Dependency decision |
| --- | --- | --- |
| Atlas Research | Public repository, no prior implementation | Build the artifact-first v0.1 here |
| Atlas Engine | Public `v0.4.2`; subprocess JSON/JSONL contracts | Reuse the release; extend only for a demonstrated deterministic evidence gap |
| Scout | Existing control plane `1.52.0`, scoring, queues, search evaluation, embeddings, leases | Reuse; do not duplicate active semantic-coverage or embedding retry/quota work |
| AtlasRepo Schema | Bootstrap is being established in a separate task | Wait for its stable topology contract; provide requirements only |
| jit-runner-kit | Public `v0.3.1` | Adapt lease/fencing ideas independently; do not copy its privileged JIT runner model |
| open-source catalog | Public projection repository, not a runtime store | Only sanitized, approved public projections may be added later |

Exact remote revisions and private worktree state are operational evidence and
do not belong in this public architecture document.

## Design principles

1. Preserve the canonical owner of every mutable production concern.
2. Move evidence between owners through immutable, digest-verified artifacts.
3. Select stages from material capabilities; do not use one pipeline blindly.
4. Change exactly one declared variable per research candidate.
5. Evaluate pinned data against explicit resource budgets and multiple gates.
6. Treat `KEEP` as offline evidence, never as production authority.
7. Keep local inference least-privilege, outbound-only, and replaceable.
8. Prefer deterministic code for measurement; an LLM may only interpret data or
   propose a bounded hypothesis.

## Ownership and authority

| Capability | Canonical owner | Boundary |
| --- | --- | --- |
| Discovery, normalization, classification, safety guard, moderation | Scout | Research consumes only a pinned structured export |
| Production queues, scheduling, leases, retries, fallback | Scout | Research defines job/result payloads, not a controller |
| Repository facts, code/security/structure evidence | Atlas Engine | No subjective score and no production persistence |
| Semantic interpretation and enrichment | Qwen primary / server LLM fallback, orchestrated by Scout | Structured output only; never production authority |
| Scoring definitions, Atlas Score, preview, activation, rollback | Scout with Platform persistence | Research proposes; a human authorizes promotion |
| Canonical intelligence document | Scout | Versioned input to chunking; raw material is not embedded directly |
| Embeddings, vector indexes, generations | Existing Scout/Platform Search path | Gemini initially, configurable; no Research vector store |
| Feedback persistence and trusted labels | Scout/Platform | Admin owns interaction; Research consumes a versioned export |
| Datasets, benchmarks, candidates, experiment receipts | Atlas Research | Immutable content-addressed artifacts only |
| Operator controls and review | AtlasRepo Admin | Stable APIs only; no browser-to-worker access |
| Public Atlas Score presentation | AtlasRepo Web | Sanitized stable Platform API only |
| Service profiles, networks, secret names, topology | AtlasRepo Schema | No business logic or contract copies |
| Final feedback and model promotion authority | Human | Never delegated to an LLM or research agent |

## Target flow

```text
External sources
      |
      v
Scout discovery -> raw evidence -> normalization -> candidate classification
      |                                      |
      |                         prompt-injection / safety guard
      |                                      |
      |             +------------------------+----------------------+
      |             |                        |                      |
      |       media enrichment       Atlas Engine evidence    metadata/security
      |             +------------------------+----------------------+
      |                                      |
      |                        Qwen / server LLM enrichment
      |                                      |
      |                         versioned explainable Atlas Score
      |                                      |
      |                         moderation / publication decision
      |                                      |
      |                         canonical intelligence document
      |                                      |
      |                          chunk -> embed -> generation index
      |                                      |
      |                     searchable public/private projection
      |                                      |
      |                           Admin human feedback labels
      |                                      |
      +---------------- versioned sanitized export ----------------+
                                             |
                                             v
                    Atlas Research dataset -> candidate -> benchmark
                                             |
                                  KEEP / DISCARD / ERROR receipt
                                             |
                                      explicit human review
                                             |
                               optional Scout import -> preview
                                             |
                                  explicit activation / rollback
```

## Capability and stage planning

Scout computes a versioned stage plan from entity type and observed
capabilities. Missing or inapplicable stages are recorded, not fabricated.

| Material | Required stages | Conditional stages | Explicitly skipped |
| --- | --- | --- | --- |
| GitHub repository | Scout normalization, safety guard, Atlas Engine, semantic enrichment, scoring, canonical document, embedding | Media enrichment for linked artifacts | Package scripts, hooks, builds, arbitrary binaries |
| Article | Scout normalization, safety guard, content analysis, semantic enrichment, scoring, canonical document, embedding | Repository analysis only for separately discovered repository entities | Repository execution |
| Video | Scout normalization, safety guard, media analysis, transcript, semantic enrichment, scoring, canonical document, embedding | Repository analysis for separately linked entities | Untrusted media-provided commands |
| MCP server/catalog entry | Scout normalization, safety guard, metadata/security analysis, semantic enrichment, scoring, canonical document, embedding | Atlas Engine only for a separately acquired source repository | Connecting to or executing an untrusted MCP server |

Stage output records contract version, producer version, input/output digests,
outcome, duration, and bounded error code. External text remains data and cannot
become system instructions.

## Cross-repository contracts

The phase-1 producer/consumer registry is
[versioned separately](../contracts/cross-repository-registry.md). Existing
Scout and Engine contracts are referenced, not copied. Proposed Atlas Score and
feedback semantics are in
[the domain contract proposal](../contracts/atlas-score-and-feedback-proposal.md);
their canonical schema/OpenAPI belongs in Scout/Platform.

## Atlas Score

Atlas Score is a versioned, explainable ScoreCard rather than one opaque number.
It supports an overall value, applicable subscores, confidence, explanations,
evidence references, scorer version, analysis version, Atlas Engine version
where applicable, creation time, and history. Inapplicable fields are `null`
with an explicit `not_applicable` reason.

A security value cannot come only from subjective LLM output. Deterministic
evidence is mapped to features and a versioned rubric before an LLM may provide
a bounded interpretation. Previous ScoreCards are append-only; activation
changes the current pointer but does not overwrite history.

Public projection contains only active values, safe explanations, aggregate
evidence, versions, and analysis date. It excludes raw findings, secrets, unsafe
excerpts, operator notes, proposed candidates, and local-LLM transcripts.

## Feedback and active learning

Admin captures fast labels (`useful`, `not_useful`, `exceptional`) and optional
detailed ratings. Each record binds reviewer identity, material ID, ScoreCard
version, structured labels, creation time, and an audit reference. Reviews do
not mutate the active model.

Before Research receives feedback, Scout separates:

- **instructions** — fixed policy from trusted code/configuration;
- **data** — external material represented as inert bounded fields;
- **labels** — trusted, versioned operator judgments.

Comments remain untrusted data even when a label is trusted. A future
Scout-owned `needs_review` queue prioritizes low confidence, model disagreement,
large Scout/Engine/LLM disagreement, borderline publication, novel categories,
and high-impact repositories. Research may improve material scoring, search
ranking, candidate ranking, or discovery policy, but each has a separate
candidate and rollout.

## Canonical document, embeddings, and generations

Scout builds the canonical enriched representation from source metadata,
normalized Scout evidence, safe media analysis, Atlas Engine evidence summary,
active Atlas Score, topics, technologies, capabilities, use cases, and semantic
description. Audit-only fields are not embedded.

Embedding identity includes normalized chunk, provider/model, dimensions,
chunking version, and canonical schema version. Source, analysis, enrichment,
chunk, and embedding digests prevent paying for unchanged content. Gemini at 768
dimensions is the initial target, but the provider remains configurable.

Live indexing uses realtime or micro-batches; bulk work uses provider batches
where supported. `REBUILD_VECTOR_INDEX` reuses enrichment, while
`RERUN_FULL_INTELLIGENCE_PIPELINE` reruns the costly analysis path. Full reindex
never deletes the active index first: generation `N+1` progresses through
`BUILDING -> READY -> ACTIVE`, then `N` becomes `RETIRED`; failure leaves `N`
active. Existing Scout scheduling and locking remain canonical.

The `open-source` catalog may later receive a reviewed, sanitized public
intelligence projection. It is never a production database, queue, or private
artifact store, and Research does not write to it directly.

## Research maturity

| Level | Capability | Entry gate |
| --- | --- | --- |
| 1 | Prompt and rubric optimization | Versioned labeled dataset, sealed test split, reproducible baseline |
| 2 | Feature and score-weight optimization | Level 1 stable plus benchmark-declared minimum records and coverage |
| 3 | Lightweight ranker/classifier | Separately approved model contract, stronger holdout and serving plan |
| 4 | LoRA/Qwen adaptation | Explicit security, privacy, licensing, model-evaluation, rollback, and compute review |

The v0.1 contract reserves Levels 1 and 2, while the bundled numeric linear
evaluator implements Level 2 feature and score-weight experiments only. A
separate reviewed prompt/rubric artifact and benchmark are required before
claiming Level 1 runtime support. Each benchmark declares minimum total and
per-split record counts; advancing maturity requires a reviewed ADR backed by
coverage and benchmark reliability. Levels 3 and 4 are not implemented claims.

## Artifact model

Every portable artifact has producer release/commit identity, media type, byte
length, SHA-256 digest, and URI. External Scout, Engine, or Platform artifacts
also carry schema/contract ID and version. Schema identity may be omitted only
for explicitly opaque Research-owned bytes.

External schema IDs are opaque provenance and never trigger a network fetch.
Validation uses a bundled allowlist. Manifests do not copy another service's
runtime schema.

The v0.1 Research-owned contracts are `artifact-ref.v1`,
`dataset-manifest.v1`, `benchmark-manifest.v1`, `candidate-artifact.v1`,
`experiment-receipt.v1`, `research-experiment-job.v1`, and
`research-experiment-result.v1`.

## Dataset and evaluation integrity

- Source exports are pinned before splitting; duplicate IDs and duplicate JSON
  keys are rejected.
- Splits derive deterministically from record ID, seed, and ratios.
- The sealed test split is unavailable to hypothesis generation and routine
  validation experiments.
- Test data remains sealed: the v0.1 worker rejects test evaluation until a
  separately protected operator capability, bound outside the untrusted job,
  is implemented. Job review metadata is audit provenance only.
- Metrics are deterministic code: MAE, Spearman, bounded pairwise accuracy,
  NDCG@10/50, threshold F1, and calibration error.
- A benchmark declares direction, minimum delta, absolute threshold, minimum
  records, and hard resource limits.

Operator ceilings cannot be raised by a job:

| Resource | v0.1 hard ceiling |
| --- | --- |
| Worker concurrency | 1 |
| Job JSON | 1 MiB |
| Referenced artifacts | 64 |
| One artifact / total input | 256 MiB / 1 GiB |
| Records / one JSONL line | 1,000,000 / 1 MiB |
| Features per record | 256 |
| Wall time / resident memory | 3,600 seconds / 6 GiB |
| Output / workspace bytes | 256 MiB / 1 GiB |
| Open file descriptors | 128 |
| JSON depth / one string | 32 / 64 KiB |
| Qwen response / supplied context | 64 KiB / aggregate metrics only |
| Qwen request timeout | 60 seconds |
| Candidate complexity | exactly one allowlisted scalar change |

Pairwise metrics use a deterministic bounded sample, not unbounded quadratic
comparison.

## Candidate discipline and local Qwen

A candidate records one allowlisted target-variable change, parent offline
evaluation payload, maturity level, rationale, generator, payload digest, and
target canonical-owner contract. Its status is always `proposed` here. The
evaluation payload is never a Scout scoring definition or direct import;
Scout must map an approved hypothesis into its own definition and parse it
canonically before preview.

Qwen receives only aggregate validation metrics, allowlisted variable metadata,
and the current scalar value. It never receives raw records, source text, sealed
test IDs, secrets, private paths, or executable instructions. The accepted
response is one small strict JSON object. Unknown keys, non-finite/out-of-range
values, URLs, paths, commands, markup, tool calls, or multiple changes fail
closed. Model output is inert: no `eval`, shell, dynamic import, plugin,
model-selected file, or executable candidate.

Ollama is fixed to a literal loopback endpoint with redirects and model pulling
disabled during a job. The model name, immutable canonical model-manifest
digest, and prompt digest are recorded.

## Decisions, receipts, and promotion

The evaluator emits `KEEP` when every gate passes, `DISCARD` when a gate fails,
and `ERROR` for invalid or incomplete work. All outcomes are append-only.

The receipt binds input digests to a nested canonical result containing
deterministic decimal-string metrics, gates, decision, and reason codes. Its
`canonical_result_sha256` hashes exactly the Atlas canonical JSON v1 bytes
defined in the contract policy. The outer envelope adds execution time, bounded
resource usage, worker/session identity, previous receipt digest, and runner
provenance. A completed offline result always has a receipt, including a
terminal `ERROR`; pre-admission rejection/cancellation/expiry does not.

Exact replay returns existing receipt bytes. Reusing an idempotency key with
different job bytes conflicts. Locking, exclusive creation, atomic rename,
`fsync`, and recovery prevent partial/forked appends. A local chain detects
accidental mutation relative to a trusted head; a full rewrite is detectable
only after a head is anchored or signed outside the writable directory.

Production promotion remains separate: human selects a `KEEP`; Scout parses it
canonically, creates a preview and audit record; a privileged human approves;
Scout owns activation, observation, rollback, and backfill.

## Offline worker boundary

v0.1 is JSON-in/JSON-out. The job is trusted operator input; referenced artifacts
remain hostile. The worker resolves only relative regular files beneath roots,
rejects absolute paths, traversal, links, devices, FIFOs, sockets, and archives,
verifies/parses one stable descriptor, and writes atomic exclusive outputs under
a `0700` workspace.

The task enum and executable paths are fixed. No repository scripts, lifecycle
hooks, builds, arbitrary binaries, shell, inherited `PYTHONPATH`, loader
variables, credential environment, artifact URL fetch, or general network is
allowed. The only v0.1 network exception is fixed loopback Ollama. See the
[security model](../security-model.md).

## Scout-owned dual-workload worker

The research-only outbound client is implemented as an unreleased candidate;
remote execution stays disabled until Scout exposes reviewed authenticated
claim, heartbeat, cancellation, and terminal-result endpoints. The envelope binds job digest,
worker/session, attempt, fence, lease deadline, heartbeat sequence, cancellation
generation, and result digest. Expired leases cannot revive; stale attempts
cannot commit.

| Priority | Work |
| --- | --- |
| P0 | User/live operations |
| P1 | New materials and production inference |
| P2 | Retry and reconciliation |
| P3 | Backfill |
| P4 | Full reindex |
| P5 | Research |

Research runs only when the production queue is below threshold, resources are
available, and budget permits. For production inference, Scout tries local Qwen
for configurable `primary_attempts` with bounded timeout/delay. Repeated health
failure opens a circuit breaker; after `fallback_delay`, an allowlisted server
LLM handles production work. Half-open probes restore local service. Research
does not use paid fallback without a separate budget policy.

## Deployment topology

```text
Production networks                       Mac mini
+------------------------------+          +-------------------------+
| Scout controller / APIs      | <------- | outbound worker client  |
| Platform data/search         |  HTTPS   | local Qwen on loopback  |
| Admin and Web API consumers  |          | isolated workspaces     |
+------------------------------+          +-------------------------+
            |
            | immutable exact-object capabilities
            v
+------------------------------+
| Research artifact storage    |
| no direct production DB path |
+------------------------------+
```

AtlasRepo Schema describes the optional launchd supervisor and the existing
one-shot image boundary. It may later add production-inference topology, image digests,
health checks, resources, secret names, and network edges. Research stays off
the data network and gets no production PostgreSQL, Redis, deploy, billing,
GitHub administration, or long-lived artifact-store credentials.

## Observability contract proposal

Owners expose bounded counters, histograms, or gauges for Scout queues, stage
duration, Engine runs/failures, LLM primary/fallback, worker heartbeat, embedding
tokens/cache/failures, ScoreCard generation, feedback, experiments/resources,
model disagreement, active-learning queue, and index generations. Allowed
labels are bounded enums such as service, stage, provider, outcome, priority,
entity type, and generation state. IDs, repository names, prompts, paths, and
digests are forbidden metric labels.

## Independently reviewable phases

| Phase | Owner | Artifact and definition of done | Prerequisite/status |
| --- | --- | --- | --- |
| 0 Discovery | Cross-repo | Factual inventory, versions, overlap decisions | Complete for this plan |
| 1 Architecture/contracts | Research plus canonical owners | ADRs, registry, Research schemas, Score/feedback proposals | In progress |
| 2 Engine evidence gaps | Atlas Engine | One bounded deterministic evidence change/schema | Only after a measured gap; reuse current release |
| 3 Intelligence pipeline | Scout | Stage planner, safety, canonical outputs, idempotent queue tests | Depends on phase 1 and active Scout work |
| 4 Mac worker | Scout/worker; Schema | Lease/fence/heartbeat/failure tests | Research-only client in review; production inference/fallback still depends on phase 3 |
| 5 Atlas Score | Scout/Platform | ScoreCard/history/provenance/activation audit | Depends on evidence/stage contracts |
| 6 Canonical document/embeddings | Scout/Platform | Canonical schema, digest identity, live/batch path | Extend existing embedding path only |
| 7 Index generations/reindex | Scout/Admin | Estimate, confirmation, build/verify/atomic switch | Depends on phase 6 |
| 8 Human feedback | Scout/Platform/Admin | Versioned labels/audit/no model mutation | Depends on ScoreCard identity |
| 9 Research baseline | Atlas Research | Freeze/evaluator/receipts/Qwen proposer | Local work may proceed; integration waits for phase 8 export |
| 10 Active learning | Scout/Admin/Research | Bounded `needs_review` ranking/export | Depends on feedback/baseline |
| 11 Admin Intelligence UI | Admin | Stable API operator surfaces/privileged activation | Depends on relevant prior phases |
| 12 Public Atlas Score UI | Platform/Web | Sanitized stable API/browser-tested view | Depends on active ScoreCard/publication policy |

No phase silently authorizes the next. Each uses its own repository, branch,
tests, and review.

## Atlas Research v0.1 definition of done

- Schemas validate positive/negative fixtures without remote refs.
- Same inputs produce identical splits, metrics, and canonical result bodies;
  exact replay returns identical receipt bytes.
- Tampered, oversized, duplicate, stale, traversal, link, device, malformed,
  deep, or non-finite inputs fail closed without outside read/write.
- A candidate cannot change zero or multiple variables.
- Test/private canaries never enter Qwen, reports, receipts, logs, or public CI.
- Every resource ceiling and watchdog has a negative test.
- `KEEP`, `DISCARD`, and `ERROR` append safely; conflict, crash, concurrent
  writer, and chain-verification tests pass.
- Qwen is optional; hostile model outputs are inert and fail closed.
- Static HTML has context-correct escaping, no script/external resource, and a
  restrictive CSP; a real-browser XSS/egress test passes.
- The full suite passes on the development Mac and Mac mini.
- Socraticode retrieves ownership, worker, and promotion boundaries.
- Public CI passes on the exact remote commit.

# Cross-repository contract registry

Status: phase-1 proposal
Snapshot date: 2026-08-30

This registry names authority and compatibility without copying another
repository's schema. Paths are repository-relative identifiers, not runtime
instructions to fetch source.

| Producer -> consumer | Canonical owner | Contract/version | Current status | Compatibility rule |
| --- | --- | --- | --- | --- |
| Atlas Engine -> Scout | Atlas Engine | release `v0.4.2`; consumer manifest plus `analyzer-v1`, `security-report-v1`, `security-coverage-v1`, `security-finding-v1`, `execution-signal-v1`, `bounded-dataflow-signal-v1`, `diagnostic-event-v1`, `index-event-v2`, `index-manifest-v2` | Existing public subprocess boundary | Scout pins binary/release, validates outer and every nested schema required by the pinned consumer manifest, and records commit/artifact digest |
| Scout internal stages/events | Scout | `scout-events.v1`; control plane `1.50.1` | Existing canonical runtime | Extend in Scout; Research must not copy it |
| Scout -> Atlas Research | Scout | target `scout-research-export.v1` artifact | Not implemented; blocked by dataset/feedback ownership | Immutable sanitized export; bundled compatibility fixture and digest |
| Atlas Research -> Scout | Research hypothesis/evidence, Scout import authority | `candidate-artifact.v1`, `experiment-receipt.v1`; target Scout import OpenAPI | Research schemas proposed; import absent | Scout maps an approved one-variable hypothesis into its own definition, validates with its canonical parser, and creates preview only; Research evaluation payloads are never direct production imports |
| Scout -> Admin | Scout | target Intelligence OpenAPI v1 for queues, ScoreCards, feedback, experiments | New surface absent | Admin never accesses DB, Redis, artifacts, or workers directly |
| Admin -> Scout | Scout persistence, Admin interaction | target `atlas-feedback.v1` request/audit response | Proposal only | Bind reviewer/material/ScoreCard version; comments remain data |
| Scout -> Platform Search | Scout/Platform boundary | target canonical intelligence document v1 plus existing embedding identity | Existing embedding path; canonical enrichment extension absent | Extend current implementation; no parallel vector store |
| Platform -> Web | Platform | target `atlas-score-public.v1` stable API | Proposal only | Active sanitized ScoreCard and safe evidence only |
| Scout -> Mac worker | Scout | future authenticated worker protocol | Blocked; no v0.1 network control plane | Scout owns lease, attempt, fence, heartbeat, cancel, fallback, terminal CAS |
| Research job -> offline worker | Atlas Research | `research-experiment-job.v1` / `research-experiment-result.v1` | Proposed for v0.1 | File exchange only; not a generic production protocol |

## Change policy

Each canonical owner publishes its schema/OpenAPI and compatibility tests in its
own repository. Consumers pin supported versions and fixtures. Breaking changes
receive a new major version. A proposal here does not become runtime authority
merely because it is public.

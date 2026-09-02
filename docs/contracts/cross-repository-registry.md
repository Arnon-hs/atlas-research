# Cross-repository contract registry

Status: phase-1 proposal
Snapshot date: 2026-08-30

This registry names authority and compatibility without copying another
repository's schema. Paths are repository-relative identifiers, not runtime
instructions to fetch source.

| Producer -> consumer | Canonical owner | Contract/version | Current status | Compatibility rule |
| --- | --- | --- | --- | --- |
| Atlas Engine -> Scout | Atlas Engine | release `v0.4.2`; consumer manifest plus `analyzer-v1`, `security-report-v1`, `security-coverage-v1`, `security-finding-v1`, `execution-signal-v1`, `bounded-dataflow-signal-v1`, `diagnostic-event-v1`, `index-event-v2`, `index-manifest-v2` | Existing public subprocess boundary | Scout pins binary/release, validates outer and every nested schema required by the pinned consumer manifest, and records commit/artifact digest |
| Scout internal stages/events | Scout | `scout-events.v1`; control plane `1.52.0` | Existing canonical runtime; stage planner and retained LLM failure evidence are deployed | Extend in Scout; Research must not copy it |
| Scout -> Atlas Research | Scout | target `scout-research-export.v1` artifact | Not implemented; blocked by dataset/feedback ownership | Immutable sanitized export; bundled compatibility fixture and digest |
| Atlas Research -> Scout | Research hypothesis/evidence, Scout import authority | `candidate-artifact.v1`, `experiment-receipt.v1`; target Scout import OpenAPI | Research schemas implemented for local v0.1; Scout import absent | Scout maps an approved one-variable hypothesis into its own definition, validates with its canonical parser, and creates preview only; Research evaluation payloads are never direct production imports |
| Scout -> Admin | Scout | target Intelligence OpenAPI v1 for queues, ScoreCards, feedback, experiments | Admin read-only operations shell is deployed; canonical feature APIs remain absent | Admin never accesses DB, Redis, artifacts, or workers directly |
| Admin -> Scout | Scout persistence, Admin interaction | target `atlas-feedback.v1` request/audit response | Proposal only | Bind reviewer/material/ScoreCard version; comments remain data |
| Scout -> Platform Search | Scout/Platform boundary | target canonical intelligence document v1 plus existing embedding identity | Existing embedding path; canonical enrichment extension absent | Extend current implementation; no parallel vector store |
| Platform -> Web | Platform | target `atlas-score-public.v1` stable API | Web presentation shell exists behind a disabled gate; canonical API absent | Active sanitized ScoreCard and safe evidence only |
| Scout -> Mac worker | Scout | research worker v1 plus `atlas-production-generation-job.v1` / `atlas-production-generation-result.v1` | Research and production-generation clients are implemented; both Scout features remain disabled by default and no permanent-worker launch is implied | Scout owns priority, lease, attempt, fence, heartbeat, cancel, fallback, terminal CAS, ScoreCard construction, and every production write; clients accept data, never a server command |
| Research job -> offline worker | Atlas Research | `research-experiment-job.v1` / `research-experiment-result.v1` | Implemented for v0.1; committed fixture verified. The v0.1.0 release contract records its exact multi-architecture image in the checksummed `atlasrepo-research-0.1.0-image-digest.txt` asset; no release image identity is claimed before that immutable release verifies | File exchange only; not a generic production protocol |

## Change policy

Each canonical owner publishes its schema/OpenAPI and compatibility tests in its
own repository. Consumers pin supported versions and fixtures. Breaking changes
receive a new major version. A proposal here does not become runtime authority
merely because it is public.

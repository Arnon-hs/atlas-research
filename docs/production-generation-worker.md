# Production generation worker

Status: implemented client and executor; disabled until an exact release is
allowlisted and enrolled by Scout.

## Ownership and data flow

Scout owns the queue, priority, worker/release allowlist, enrollment, sessions,
leases, fences, cancellation, server-LLM fallback, terminal compare-and-set,
ScoreCard construction, persistence, and every production write. This package
owns only an outbound single-concurrency client and deterministic validation of
local Qwen output. The worker has no PostgreSQL, Redis, deploy, billing, GitHub
administration, or embedding credential and exposes no inbound listener.

```text
Scout /api/production-generation-worker/v1
        ^ outbound HTTPS
        |
production-worker serve -> 127.0.0.1:11434 -> qwen3:8b
```

The current Scout release reserves and tests the bounded retry/circuit state
machine but deliberately fails startup when production generation is enabled:
the dedicated paid fallback executor, atomic budget ledger, and transactional
outbox/recovery path are not implemented yet. This client never substitutes its
own fallback or calls a paid provider.

The protocol accepts exactly `content.description.regenerate` and
`atlas.score.generate`. A score job is repository-only. Scout normalizes the
source, supplies exactly one Engine and one semantic stage-evidence proof, and
provides immutable ScoreCard context. The worker runs a final deterministic
prompt-injection guard before any model call and returns only `overall` and the
complete subscore set; Scout creates the
`atlas-scorecard.v1` identity with its canonical builder.

## Configuration

The private JSON file has these exact required fields:

```json
{
  "protocol_version": "1",
  "controller_url": "https://scout-api.atlasrepo.com",
  "worker_id": "atlasrepo-generation",
  "release_id": "pgr_release_00000000000000000000000000000000",
  "model_revision": "0000000000000000000000000000000000000000000000000000000000000000",
  "enrollment_token_file": "/absolute/private/enrollment-token",
  "state_root": "/absolute/private/production-generation-worker"
}
```

Optional `poll_seconds` is bounded to 0.1 through 300 seconds and
`request_timeout_seconds` to 1 through 60 seconds. The config and enrollment
token are private regular files; the state root is `0700`. The token is never
placed in JSON, launchd, logs, status, result, or the model request.

Run one integration boundary or the persistent service:

```text
atlas-research production-worker once --config /absolute/private/worker.json
atlas-research production-worker serve --config /absolute/private/worker.json
```

AtlasRepo Schema owns service installation. Do not install a second scheduler
or place service lifecycle code in this repository.

## Exact execution and replay

The client recomputes `assignment_sha256` over canonical JSON of
`{protocol_version,job,worker_id,release_id,attempt,fence,cancellation_generation}`.
It also recomputes the exact UTF-8 source digest and canonical job-input digest.
Attempt is fixed to one; concurrency is fixed to one; one heartbeat supervisor
runs throughout generation. A cancelled, expired, stale, or ambiguous lease is
never converted into a new terminal decision.

Ollama is fixed to `127.0.0.1:11434`, exact model `qwen3:8b`, the configured
64-hex model revision, `/api/tags` and
`/api/generate`, temperature zero, structured JSON, and a 60-second hard job
ceiling. Pulls, redirects, remote model endpoints, tools, plugins, shell, URL
fetching, repository commands, and arbitrary executors are unavailable.

Description results are limited to 8192 bytes and score results to 131072 bytes;
the Ollama response itself remains capped at 65536 bytes. Before terminal
submission, the worker records the complete canonical result in its private
state. If Scout's acknowledgement is ambiguous, an identical lease replays
those exact bytes without another model call. After a restart, the current
authenticated session reads the immutable receipt bound to the old assignment.
Only a matching receipt or accepted terminal response removes that exact private
cache file.

## Activation gates

Production enablement requires all of the following outside this repository:

1. an immutable Atlas Research release and exact worker release ID;
2. Scout allowlist plus one scoped enrollment credential;
3. AtlasRepo Schema launchd configuration with private modes;
4. local Ollama bound only to loopback with the expected model digest;
5. exact-head CI for Scout, Atlas Research, Platform, Admin, Web, and Schema;
6. one fenced description canary whose Scout receipt and Platform apply audit
   bind the same job, entity version, worker release, model revision, policies,
   input digest, and output digest;
7. separate verification of server fallback and rollback behavior.

Until these gates pass, keep Scout's production-generation feature disabled.

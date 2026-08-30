# Scout worker client

Status: experimental, disabled until the matching Scout control plane is
reviewed and enabled.

## Ownership

Scout owns enrollment, sessions, mutable job state, priority, claim, attempt,
fence, lease, heartbeat, cancellation, reconciliation, terminal compare-and-set,
fallback policy, and every production write. Atlas Research owns the portable
`research.experiment` payload and this outbound client. AtlasRepo Schema owns
the installed service and network-free executor topology.

The Mac or Linux host receives no database, Redis, deploy, billing, GitHub
administration, or cloud-provider credential. It has no inbound listener.

## Protocol v1

The client uses these Scout-owned routes:

- `POST /api/worker/v1/session` with the enrollment credential;
- `POST /api/worker/v1/claim` with the short-lived session token;
- `GET /api/worker/v1/objects/<opaque-id>` for claimed objects only;
- `POST /api/worker/v1/heartbeat`;
- `POST /api/worker/v1/complete`;
- `POST /api/worker/v1/fail`.

Session, claim, heartbeat, completion, and failure request documents carry
`protocol_version: "1"` and use closed fields; session and claim responses also
carry it. Heartbeat responses are exactly `{cancelled, lease_expires_at}` and
terminal acknowledgements are exactly `{accepted, replayed}` without a protocol
field. A claim binds `job_id`, `attempt`, monotonic `fence`,
`cancellation_generation`, `lease_expires_at`, `workload_type`, `job_path`, and
at most 64 `{path, sha256, size_bytes, download_path}` objects. Version 1 accepts
only `research.experiment`, requires `job_path` to be `job.json`, caps each
artifact at 256 MiB, and caps the full claimed bundle at 1 GiB. Object paths are
exact lowercase UUID paths under `/api/worker/v1/objects/`; object requests send
only the short-lived Bearer credential and normal HTTP negotiation headers.

Heartbeat and terminal requests repeat the complete identity tuple plus
`worker_id` and `session_id`. A heartbeat has a strictly increasing sequence.
The completed `result_sha256` is SHA-256 over at most 256 KiB of RFC 8785
canonical JSON bytes without a trailing newline. Before completion, the client
loads the staged immutable job and checks the result schema plus exact
worker/session, job, attempt, idempotency key, and job-spec digest bindings.
Repeating the same terminal digest is idempotent; a different digest, stale
fence, expired lease, revoked session, or changed cancellation generation must
be rejected by Scout. A terminal HTTP 200 is accepted only with the exact
acknowledgement `{\"accepted\":true,\"replayed\":<boolean>}`; an empty,
malformed, negative, or extended acknowledgement remains operationally
ambiguous and is never converted into a second terminal decision.

One heartbeat supervisor starts immediately after claim and remains active
through staging, replay checks, and execution. The total deadline covers that
whole lifecycle. The default request timeout is 30 seconds and the default
claim deadline is 3000 seconds. A session is admitted only when its remaining
lifetime is at least `max_job_seconds + 2*lease_seconds +
2*request_timeout_seconds`; with Scout's 120-second lease this is 3300 seconds,
below the 3600-second session TTL.

## Failure behavior

The client fails closed on malformed control data, unsafe local paths, digest or
size drift, redirects, cross-origin paths, executor failure without a result,
lease expiry, cancellation, and heartbeat/authentication rejection. Controller,
heartbeat, lease, and ambiguous terminal errors are never converted into a
terminal job failure; Scout reconciles those leases. Only deterministic staged
input, executor, and result failures use the fenced failure endpoint. It never
falls back to a cloud model by itself. Server fallback and production priority
remain Scout policy; research work must not starve production ingestion.

A verified existing `result.json` under the exact
job/attempt/fence/cancellation-generation run root is replayed without
re-executing the experiment. Scout's terminal CAS decides whether that replay
is the same accepted result. The exact run tree is retained for operational or
ambiguous outcomes, but removed with symlink-safe bounded cleanup after an
accepted completion, accepted failure, or Scout-confirmed cancellation.
Cleanup opens each directory relative to an already pinned parent with
`O_DIRECTORY|O_NOFOLLOW`, verifies the device/inode, and changes permissions
only through that opened descriptor. It does not require Linux's unavailable
`chmod(..., follow_symlinks=False)` capability and fails closed if a directory
cannot be opened safely.
After the heartbeat supervisor starts for a new claim, the worker removes every
other direct run entry before staging; an empty claim response removes all stale
entries. Therefore only the currently replayable claim tuple may survive an
ambiguous outcome. Before creating its run tree, the worker also requires free
space for every declared artifact plus a fixed 1 GiB safety reserve. Pruning and
the free-space gate are operational checks and never create a terminal failure.
The reserve is checked again if the executor returns without a result. `ENOSPC`
or `EDQUOT` from any local staging, status, or atomic-commit step becomes
`WORKER_STORAGE_EXHAUSTED`; other OS-backed local write failures such as `EIO`,
`EROFS`, `EACCES`, or `EMFILE` become `WORKER_STORAGE_UNAVAILABLE`. Both are
operational: the run remains replayable and no failure endpoint is called. A
backoff status update suppresses only another OS-backed local failure, so it
cannot mask the typed error while pure validation or path-tampering errors stay
visible.

# Worker and threat boundaries

## Trust model

The operator-selected executable, job path, artifact root, output root, and
installed Atlas Research package are trusted. Job contents, source exports,
dataset records, benchmark metadata, candidate metadata, and model output are
untrusted data.

The v0.1 worker is not a generic remote executor. It exposes no shell, code
plugin, repository build, package lifecycle, hook, dynamic import, URL fetch,
database, Redis, or deployment path.

## Admission and evaluation

1. The parent reads the stable regular job file once, validates its identity,
   deadline, and hard ceilings, then canonicalizes it into one immutable job
   snapshot. Preflight and evaluation receive those exact bytes; the job path is
   not reopened between phases.
2. Every sealed test evaluation fails closed in v0.1. Job review metadata does
   not grant execution authority; an external operator capability must exist
   before the worker can admit the test split.
3. Benchmark metadata is untrusted and declares resource ceilings. A sanitized
   preflight subprocess resolves only the digest-pinned benchmark under the
   preliminary job/operator/host ceilings. The parent validates its packet and
   reduces the reported limits again before starting evaluation.
4. Artifact resolution uses directory file descriptors and rejects traversal,
   symlinks, hard links, archives, devices, FIFOs, digest drift, size drift, and
   role/schema/media mismatches.
5. Dataset membership, ordering, sealing, counts, digests, and fixed split
   ratios are recomputed.
6. The benchmark is bound exactly to the job dataset, baseline, and split.
7. The candidate is bound to the exact parent and proposed payload bytes and may
   change exactly one allowlisted scalar leaf.
8. A second sanitized isolated Python subprocess recomputes all declared
   metrics and gates. The parent watchdog enforces the reduced wall-time and
   output limit.
9. The receipt is canonical, append-only, hash-chained, and bound to job
   idempotency. The result references the exact committed receipt bytes.

Deadline enforcement is repeated at the exact state transitions: initial parent
admission; child preflight admission; before each child wait using the smaller
of absolute time remaining and the shared monotonic wall-time budget; child
evaluation admission; after evaluation; after provenance and chain-head reads;
and, under the receipt-log lock, immediately before append. Any expired check or
timeout rejects the terminal receipt.

## Resource isolation

The parent first reduces job, operator, and optional host ceilings, then runs the
benchmark preflight inside those limits. The final evaluation ceilings are the
field-wise minimum including the untrusted benchmark. POSIX subprocess limits
cover CPU time, output file size, open file descriptors, and core dumps. Linux
additionally applies an address space ceiling. macOS virtual-address semantics
make `RLIMIT_AS` unsafe for an already mapped interpreter, so host memory
isolation there must come from the container or process supervisor. Concurrency
is fixed to one per private output root.

The recommended hostile-input boundary is a non-root container with no network,
no capabilities, a read-only root filesystem, a PID limit, explicit CPU/memory
limits, read-only input, and a single private output mount.

## Qwen boundary

Qwen proposal generation is deliberately outside the offline evaluator
subprocess. It uses only `127.0.0.1:11434`, fixed paths `/api/tags` and
`/api/generate`, exact model `qwen3:8b`, structured output, bounded responses,
temperature zero, and no redirects. It cannot pull a model, call a tool, fetch a
URL, read an artifact, or emit an activation signal.

## Promotion boundary

`KEEP` is an offline gate result. Atlas Research has no production model write,
preview, activation, rollback, or publication endpoint. Scout remains the only
owner of those controls, and a human operator remains the promotion authority.

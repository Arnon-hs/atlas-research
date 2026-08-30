# Security and privacy model

Status: v0.1 offline evaluator and opt-in Scout-client requirements

## Trust classes

| Input | Trust |
| --- | --- |
| Packaged code, bundled schemas, operator ceilings | Trusted release input |
| Offline job file | Trusted operator request, still strictly parsed |
| Referenced artifacts and dataset rows | Hostile data |
| Repository, article, video, MCP, comments | Hostile external content |
| Qwen response | Hostile inert data |
| Human label | Trusted label; its comment remains hostile data |

## Artifact confinement

v0.1 accepts regular files beneath configured input roots. It rejects absolute
paths, `..`, symlinks, hardlinked files, devices, FIFOs, sockets, archives, and
non-local URI schemes. Files are opened without following links; digest, size,
and parsing use that descriptor after stable `fstat` checks. Workspaces are
private (`0700`), outputs are exclusive (`0600`), and atomic commits cannot use
locator-controlled names outside the output root.

Negative tests cover traversal, input/output link swaps, hard links, devices,
FIFOs, sockets, sparse oversize, archive paths, and outside writes.

## Parser and resource safety

JSON rejects duplicate keys, non-finite numbers, excessive depth, long strings,
and unknown fields. JSONL has bounded bytes, lines, records, features, and field
sizes. Operator ceilings override looser job values. A watchdog enforces wall
time and resource limits and records a bounded error after cleanup.

## Execution and environment

The offline evaluator dispatches a closed in-process task enum. It uses no shell, dynamic
imports, repository commands, hooks, builds, lifecycle scripts, model-selected
files, arbitrary binaries, or artifact downloads. It rebuilds a minimal
environment and ignores inherited credentials, `PATH`, `PYTHONPATH`, loader
variables, `.env`, and Git configuration. v0.1 network policy is deny-by-default
except literal loopback Ollama; redirects, remote pulls, tools, plugins, and
arbitrary hosts/schemes are disabled. The separate outer supervisor may use
only the configured Scout HTTPS origin (or literal loopback HTTP for tests) and
never passes its credential to the evaluator.

## Qwen boundary

The request contains aggregate validation metrics, one allowlisted variable,
its current scalar value/range, and fixed instructions. It excludes raw records,
source text, test IDs, paths, secrets, comments, and full prompts. The response
is one bounded JSON object with known keys. Code, shell text, markup, URLs,
paths, tool calls, unknown/nested keys, non-finite/out-of-range values, and
multiple changes fail closed.

## Receipts and replay

Atlas canonical JSON v1 binds exact job and input/output digests to the nested
canonical result. In the offline evaluator's local receipt log, append locking,
exclusive creation, `fsync`, atomic rename, and recovery prevent partial writes.
Exact replay returns existing bytes; key reuse with different bytes conflicts.
A chain head must be copied to a separately protected location or signed before
claiming detection of a full rewrite.

Scout terminal commits require current worker/session, attempt, fence, unexpired
lease, uncancelled generation, and matching result digest. Heartbeat cannot
revive an expired lease. Protocol v1 sends Scout canonical result JSON containing
a receipt reference, but not the receipt body. Accepted-run cleanup removes the
local run tree, so a terminal acknowledgement is not proof of durable cross-host
receipt-body retention.

## Reports

Reports contain aggregates and digests, never raw rows, prompts, credentials,
signed URLs, or full model responses. HTML renders untrusted values as text with
context-aware escaping, has no script/CSS/remote resource/untrusted link, and
uses `default-src 'none'` plus a no-capability `sandbox` CSP. The serving
validator rejects any second `http-equiv` directive, including meta refresh. A
local server is loopback-only, GET/HEAD-only, with no directory listing or CORS
and with `nosniff`.

Browser tests inject tag, attribute, URL, SVG, bidi, and oversized Unicode
payloads and assert no execution or external request.

## Data handling

Public fixtures are synthetic. Receipts and logs contain IDs, digests, bounded
aggregate metrics, and error codes. Raw private data stays in the restricted
input workspace. Failed work is quarantined with private permissions for a
configured TTL, then removed by an operator-owned cleanup command; public CI
artifacts are prohibited. Secret/private-data canaries must be absent from Qwen
requests, receipts, reports, logs, tracebacks, and CI.

At-rest encryption and backup are deployment responsibilities documented by
AtlasRepo Schema before any private artifact store is introduced.

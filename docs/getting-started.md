# Getting started

Atlas Research is a local, artifact-first evaluator. It does not connect to a
production database, Redis, Scout queues, an embedding provider, or a deployment
API.

## Install and verify

```bash
uv sync --locked --all-groups
uv run atlas-research --version
uv run atlas-research doctor
make check
```

`doctor` treats Ollama as optional. Use `doctor --require-qwen` only when the
local proposal step is part of the run. It verifies the fixed loopback listener,
the exact `qwen3:8b` tag, and its 64-character model digest; it never pulls or
updates a model.

## Freeze a dataset

The example records are synthetic and contain no production data.

```bash
mkdir -m 700 /tmp/atlas-research-bundle
uv run atlas-research dataset freeze \
  --source examples/scoring-records.jsonl \
  --output-root /tmp/atlas-research-bundle \
  --dataset-id example-v1 --seed 7 \
  --source-producer demo-fixture --source-producer-version 1.0.0 \
  --source-schema-id urn:atlasrepo:example:scoring-export \
  --source-schema-version 1.0.0
```

The command copies exact source bytes, writes canonical JSONL splits, seals the
test split in the manifest, and emits a digest-pinned dataset-manifest reference.
It refuses to replace existing artifacts.

## Ask local Qwen for one hypothesis

Install and start Ollama separately, bind it only to `127.0.0.1:11434`, and make
the exact `qwen3:8b` model available. Then run:

```bash
uv run atlas-research doctor --require-qwen
uv run atlas-research qwen propose \
  --context examples/qwen-context.json \
  --timeout 60
```

Only aggregate allowlisted metrics and scalar bounds enter the prompt. The
response must match a closed JSON Schema. URLs, paths, commands, code, markup,
tool calls, and extra fields fail closed. Qwen is a proposer; deterministic code
builds and evaluates the candidate.

## Run an experiment job

The repository ships one complete, synthetic, digest-pinned validation fixture
for offline smoke tests. Verify its committed byte inventory first, then choose
a private output directory:

```bash
job_path="examples/fixture-v1/job.json"
artifact_root="examples/fixture-v1"
output_root="/operator/controlled/atlas-research-output"

mkdir -m 700 "$output_root"
uv run atlas-research worker run \
  --job "$job_path" \
  --artifact-root "$artifact_root" \
  --output-root "$output_root" \
  --result-uri result.json \
  --worker-id mac-mini-research
```

The job file path is selected by the trusted operator. Every artifact referenced
inside the job is confined beneath `--artifact-root`, checked by role, media
type, schema, producer, byte length, and SHA-256 before parsing.

`examples/fixture-v1/bundle-manifest.json` pins the exact job spec and every
fixture file. Its long deadline exists only to keep this invented,
validation-only offline smoke reproducible; do not reuse it for operational
jobs. Real experiments must use short operator-selected deadlines and their own
digest-pinned artifacts.

The unreleased v0.1 candidate rejects every sealed test evaluation. No review
metadata in a job is treated as execution authority; test evaluation remains
fail-closed until an external operator capability is implemented.

Inside a checkout the worker verifies `HEAD` and refuses dirty source
provenance. The container records a root-owned, non-writable two-line
provenance file containing the build's declared Git revision and the exact
installed wheel SHA-256; receipts distinguish this from a verified checkout.

## Verify receipts and render a report

```bash
receipt_root="$output_root/receipt-log"

uv run atlas-research receipt verify \
  --root "$receipt_root"

uv run atlas-research report render \
  --receipt-root "$receipt_root" \
  --output /tmp/atlas-research-report.html

uv run atlas-research report serve \
  --file /tmp/atlas-research-report.html \
  --port 8765
```

Render reports from the verified receipt-log root, never from a guessed entry
name or an arbitrary receipt file. The report contains aggregate metrics,
decisions, reason codes, and digests—not raw records, comments, repository
excerpts, prompts, or secrets. The server is fixed to literal loopback and
supports only GET/HEAD.

## Container boundary

Build only from a clean checkout. The cleanliness check includes tracked and
untracked files and runs before `uv build`; the image receives the exact
40-character commit. Then run with no network, a read-only root filesystem,
dropped capabilities, and a private writable output mount:

```bash
test -z "$(git status --porcelain=v1 --untracked-files=all)" || {
  echo "refusing to build from a dirty checkout" >&2
  exit 1
}
vcs_ref="$(git rev-parse --verify 'HEAD^{commit}')"
test "${#vcs_ref}" -eq 40

uv build
docker build \
  --build-arg VCS_REF="$vcs_ref" \
  -t atlas-research:local .

artifact_root="/operator/supplied/artifact-bundle"
output_root="/operator/controlled/atlas-research-output"
job_relative="operator-job.json"

# For the public validation-only smoke instead:
# artifact_root="$PWD/examples/fixture-v1"
# job_relative="job.json"

docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges \
  --pids-limit 64 --memory 8g --cpus 2 \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --mount "type=bind,src=$artifact_root,dst=/artifacts,readonly" \
  --mount "type=bind,src=$output_root,dst=/work/output" \
  atlas-research:local worker run \
  --job "/artifacts/$job_relative" --artifact-root /artifacts \
  --output-root /work/output --result-uri result.json
```

Do not mount production secrets, Docker sockets, SSH agents, database
credentials, Redis credentials, deploy tokens, or GitHub administration tokens.

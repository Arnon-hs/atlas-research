# Contributing

Atlas Research welcomes focused issues, documentation improvements, contract
reviews, tests, and small implementation changes.

## Before you start

1. Read the architecture and ADRs under `docs/architecture/`.
2. Search existing issues and pull requests before proposing a new subsystem.
3. Open a design issue before changing a public schema, trust boundary, metric,
   or ownership boundary.
4. Never include production data, credentials, private repository contents, or
   personally identifiable information in fixtures, logs, or issues.

## Local setup

Use Python 3.11 or newer and `uv`:

```bash
git clone https://github.com/Arnon-hs/atlas-research.git
cd atlas-research
uv sync --locked --all-groups
uv run atlas-research doctor
make check
```

`make check` is the required local parity command. It runs Ruff lint and format
checks, strict mypy, the branch-coverage test suite, dependency audit, and the
sdist/wheel build. The public fixture under `examples/fixture-v1/` is synthetic;
never replace it with private or production data.

## Change rules

- Keep each pull request narrow and explain the user-visible outcome.
- Preserve Scout, Atlas Engine, and AtlasRepo Schema ownership boundaries.
- After the first tagged release, public schemas are append-only within a
  version. Breaking changes require a new major schema version and a migration
  note. Before that release, draft consumers must pin an exact commit.
- A candidate must change exactly one declared variable.
- An offline `KEEP` result must never trigger production activation.
- Add or update tests for behavioral changes.
- Run every documented check before requesting review.
- Follow the existing Ruff formatting and strict typing configuration rather
  than introducing a parallel style or test tool.

## Commit sign-off

Contributions use the
[Developer Certificate of Origin 1.1](https://developercertificate.org/).
Read it before contributing and use `git commit -s` to add your sign-off. The
sign-off certifies the statements in the DCO, including that you have the right
to submit the contribution under this repository's license.

## Pull requests

Describe:

- what changed and why;
- affected public contracts;
- security and privacy impact;
- exact checks run;
- limitations or follow-up work.

Maintainers may request smaller commits, additional evidence, or an ADR before
accepting a change.

Use a concise imperative pull-request title such as `fix: enforce workspace
budget`. Sign each commit with `git commit -s`; cryptographic signing is welcome
but is not currently required by project policy.

## Maintainer release preflight

The repository's immutable-release setting is exposed through an admin-only
endpoint that the short-lived Actions token cannot read. Before creating a
release tag, a repository administrator must verify the setting with their own
authenticated GitHub CLI session and bind the exact reviewed `main` commit to
the non-secret Actions variable consumed by the tagged workflow:

```bash
release_repo="Arnon-hs/atlas-research"
release_sha="$(git ls-remote \
  "https://github.com/${release_repo}.git" refs/heads/main | \
  awk 'NR == 1 { print $1 }')"
[[ "$release_sha" =~ ^[0-9a-f]{40}$ ]]
test "$(gh api "repos/${release_repo}/immutable-releases" --jq .enabled)" = true
gh variable set RELEASE_ADMIN_PREFLIGHT_SHA \
  --repo "$release_repo" \
  --body "$release_sha"
```

Create the annotated release tag only after the release pull request and the
post-merge quality and CodeQL runs are green on that same SHA. The workflow
rejects an absent or draft release unless `RELEASE_ADMIN_PREFLIGHT_SHA`, the
peeled tag, and current `main` all equal the tagged commit. It also verifies the
published release's `immutable` field, exact assets, image digest, and
attestations. An already immutable historical release uses its tag and
attestations for read-only verification and does not depend on the current
preflight variable or current `main`.

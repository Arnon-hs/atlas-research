# Governance

Atlas Research uses maintainer-led governance while the project is young.

## Roles

- **Contributors** submit issues, documentation, tests, and code.
- **Reviewers** provide evidence-based review but do not gain merge authority
  automatically.
- **Maintainers** triage work, approve releases, merge changes, and protect the
  project's ownership and security boundaries.

The current repository owner and initial maintainer is `@Arnon-hs`. Additional
maintainers may be added after sustained, trusted contributions and an explicit
public decision.

## Decisions

Routine changes use pull-request review. Public contract, security boundary,
metric definition, governance, and production-integration decisions require an
ADR or equivalent design record. Maintainers seek consensus and document the
decision; the repository owner resolves deadlocks during the bootstrap phase.

## Releases

Only maintainers may create a release. A release requires a reviewed commit,
green required checks, a changelog, versioned contracts, and reproducible build
evidence. A green offline experiment does not authorize a release or production
activation.

## Changes to governance

Governance changes require a public pull request, a clear transition plan, and
maintainer approval. They must not be bundled into an unrelated code change.

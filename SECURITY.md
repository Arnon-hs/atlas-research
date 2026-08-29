# Security Policy

## Supported versions

Atlas Research is an unreleased v0.1 implementation candidate. No version is a
supported production release. Security fixes are applied to the default branch
during candidate development. This repository must not be treated as a
production control plane until a supported release is explicitly documented.

## Reporting a vulnerability

GitHub private vulnerability reporting is disabled for this repository, and the
project does not currently publish a dedicated private security contact. Do not
publish exploit details, credentials, production data, or other sensitive
information in a public issue.

Open a minimal public issue titled `Security contact request` with no sensitive
details and ask a maintainer to establish a private channel. This bootstrap
request is not itself a private reporting channel. If private vulnerability
reporting is enabled later, this document will link directly to the repository's
advisory form.

Please include, through the private channel:

- affected version or commit;
- impact and required preconditions;
- minimal reproduction steps;
- suggested remediation, if known.

No response-time or disclosure-time SLA is promised before the project has a
documented security response team.

## Trust boundaries

- Research inputs are untrusted and must be size-bounded and digest-verified.
- A Mac mini worker receives no production database, Redis, deploy, or GitHub
  administration credentials.
- Local Ollama is loopback-only and may propose a bounded hypothesis; it cannot
  run arbitrary commands or authorize a result.
- Expired or stale work cannot commit a terminal result.
- Offline `KEEP` is never a production activation signal.

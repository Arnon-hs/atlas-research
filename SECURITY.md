# Security Policy

## Supported versions

Atlas Research is alpha software. Security fixes are applied to the default
branch. After an immutable release is published, only the latest release on the
current minor line receives best-effort security fixes; older tags and
unreleased commits are not supported release lines. No Atlas Research release
is a production control plane or a production-activation authority.

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/Arnon-hs/atlas-research/security/advisories/new).
Do not publish exploit details, credentials, production data, or other
sensitive information in a public issue.

Please include, through the private channel:

- affected version or commit;
- impact and required preconditions;
- minimal reproduction steps;
- suggested remediation, if known.

No response-time or disclosure-time SLA is promised. A maintainer will
coordinate validation, remediation, and disclosure through the private
advisory.

## Trust boundaries

- Research inputs are untrusted and must be size-bounded and digest-verified.
- A Mac mini worker receives no production database, Redis, deploy, or GitHub
  administration credentials.
- Local Ollama is loopback-only and may propose a bounded hypothesis; it cannot
  run arbitrary commands or authorize a result.
- Expired or stale work cannot commit a terminal result.
- Offline `KEEP` is never a production activation signal.

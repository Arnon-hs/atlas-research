# Synthetic fixture v1

This directory is a deterministic, digest-pinned validation fixture. It contains
only three invented scoring records and evaluates the public `validation` split.
The Level 2 candidate changes one synthetic score weight. Its MAE and
calibration-error gates both improve, so the expected outcome is `completed` /
`KEEP` over one record.

Use `job.json` as the job and this directory as the artifact root. Verify every
committed byte against `bundle-manifest.json` before an external smoke run.

The job has an intentionally long deadline so the immutable public fixture stays
runnable. This is safe only because it is synthetic, offline, validation-only,
has hard resource ceilings, and cannot authorize sealed-test evaluation. Never
copy that deadline into an operational job.

Regenerate into an empty directory and compare bytes:

```bash
uv run python scripts/build_example_fixture.py --output-root /tmp/fixture-v1
diff -ru -x README.md examples/fixture-v1 /tmp/fixture-v1
```

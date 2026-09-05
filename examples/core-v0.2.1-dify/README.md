# Dify Core v0.2.1 golden evaluation case

This single public case proves that Atlas Research can consume one immutable
AtlasRepo Core decision pack without adding Core, an LLM, or a network client
as a runtime dependency. It is a golden evaluation case, not a benchmark.
A representative benchmark requires a reviewed set of 50-100 cases.

The vendored decision pack is copied byte-for-byte from
`Arnon-hs/atlasrepo-core` tag `v0.2.1`, commit
`6bffb144add56d13de0c0bf9be9c39931ec0c9bb`. `case.json` pins its raw-byte and
canonical SHA-256 digests and records the immutable release package digest.
All accepted citations must be explicitly public. `restricted`, `private`, or
missing access classifications fail closed.

The copied Core artifact remains Apache-2.0 licensed. Exact upstream
`LICENSE` and `NOTICE` bytes are included beside it and pinned in `case.json`;
the Atlas Research project license remains MIT.

The evaluator is disabled by default:

```text
ATLAS_RESEARCH_CORE_EVAL_ENABLED=false
```

Run the local, offline proof explicitly:

```bash
ATLAS_RESEARCH_CORE_EVAL_ENABLED=true \
  uv run python scripts/evaluate_core_golden_case.py \
  --case-root examples/core-v0.2.1-dify
```

The command only reads committed fixture bytes and writes canonical JSON to
stdout. It does not fetch sources, call a model, execute Dify, publish a result,
or authorize deployment. The expected output is committed as
`expected-result.json` and is checked byte-for-byte in the test suite.

# Deterministic dataset split v1

`sha256-id-v1` is a byte-level algorithm. It does not use a language PRNG or
floating-point thresholds.

## Accepted record identity

Each UTF-8 JSONL record has one ASCII `id` matching
`^[a-z0-9][a-z0-9._:-]{0,127}$`. Duplicate IDs, duplicate JSON keys, invalid
UTF-8, and non-canonical identity forms are rejected before any output is
written. There is no Unicode normalization step because non-ASCII IDs are not
accepted.

The scoring-example v1 record has exactly `id`, `features`, and `label`.
`features` contains 1–256 unique ASCII keys matching
`^[a-z][a-z0-9_.-]{0,63}$` and finite numeric values in `[-1000000, 1000000]`.
`label` is finite and in `[0, 100]`. Booleans are not numbers. No source text,
comments, paths, instructions, or private metadata are part of this record.

## Bucket algorithm

For an unsigned 32-bit seed and ASCII record ID:

```text
input = UTF8("atlas-research:split:v1")
        || 0x00
        || seed encoded as exactly four unsigned big-endian bytes
        || 0x00
        || ASCII(record_id)

digest = SHA-256(input)
bucket = unsigned big-endian integer represented by digest[0:8]
```

The v0.1 ratios are fixed at `0.8 / 0.1 / 0.1` and use exact integer
boundaries over the 64-bit space:

```text
bucket < 14757395258967641292  -> train
bucket < 16602069666338596454  -> validation
otherwise                     -> test
```

Output rows are sorted by ASCII ID and encoded as RFC 8785 canonical JSON, one
record per line with a final newline. The manifest records exact counts and
digests. A verifier recomputes membership for every row, proves IDs are
pairwise-disjoint, and requires `sealed=false` for train/validation and
`sealed=true` for test.

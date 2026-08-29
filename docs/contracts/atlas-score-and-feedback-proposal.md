# Atlas Score and feedback domain contract proposal

Status: non-canonical phase-1 proposal
Canonical owner: Scout with Platform persistence/public projection

This document is input to the Scout/Platform contract task. Atlas Research does
not publish a competing runtime JSON Schema.

## ScoreCard

A ScoreCard binds stable material/analysis IDs, immutable ScoreCard version,
scorer/rubric/analysis/canonical-document versions, Atlas Engine version where
applicable, overall score, named subscores, confidence, bounded explanations,
safe evidence references, creation time, predecessor, and activation audit.

Each score value is either:

```json
{
  "value": 88,
  "confidence": 0.91,
  "applicability": "applicable",
  "explanation": "Bounded safe explanation",
  "evidence_refs": ["evidence:sha256:..."],
  "scorer_version": "atlas-scorer-v7",
  "analysis_version": "analysis-v3",
  "atlas_engine_version": "v0.4.2",
  "created_at": "2026-08-30T00:00:00Z"
}
```

or `value: null` with `applicability: not_applicable` and a reason. The canonical
schema must cap strings/arrays, reject unknown fields, retain history, and
prevent an LLM-only security score.

## Feedback

Feedback binds `reviewer`, `material_id`, `scorecard_version`, creation time,
and one or both of:

- fast label: `useful`, `not_useful`, or `exceptional`;
- detailed labels: bounded overall, relevance, usefulness, technical quality,
  quality, recommendation, and optional bounded comment.

Reviewer identity is access-controlled audit data. The comment is untrusted
data and never an instruction. Corrections refer to the prior record. Feedback
does not activate or mutate a scorer.

## Public projection

The stable public contract may include active score values, confidence, safe
explanations/evidence, versions, and analysis date. It excludes private
evidence, raw security findings, secrets, dangerous excerpts, reviewer identity
or comments, inactive candidates, prompts, and provider output.

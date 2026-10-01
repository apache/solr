# SOLR-13524 — Testing Handoff

> **UNCOMPILED / UNTESTED.** This patch was written against `apache/solr` main
> at `c3e18f1e455` without running a build or any tests. The reviewer (or CI)
> must compile and validate before merge.

## What changed

1. `solr/solrj-streaming/.../io/eval/RecursiveBooleanEvaluator.java`:
   extracted the arity/null/type validation from `doWork` into a new
   `protected Checker validateValues(Object...)` method. The base `doWork`
   reduction loop is byte-for-byte unchanged, so `AndEvaluator`,
   `EqualToEvaluator`, `LessThanEvaluator`, etc. behave exactly as before.
2. `solr/solrj-streaming/.../io/eval/OrEvaluator.java`: new `doWork` override
   with disjunctive reduction — validate, then return `true` on the first
   `true` value, else `false`.

**Bug**: `or(false, false, true)` returned `false`. The inherited pairwise
loop short-circuits on the first *failing* pair (`(false||false)` is false →
immediate `false`), which is only correct for conjunctive (AND-style)
checkers. OR over 3+ args now evaluates all values.

Also added: `changelog/unreleased/SOLR-13524.yml`.

## Suggested reviewer validation

```bash
./gradlew :solr:solrj-streaming:compileJava -Pvalidation.errorprone=true
./gradlew :solr:solrj-streaming:test --tests "org.apache.solr.client.solrj.io.stream.eval.OrEvaluatorTest"
./gradlew :solr:solrj-streaming:test --tests "org.apache.solr.client.solrj.io.stream.eval.AndEvaluatorTest"
```

Suggested new coverage (not included, extend `SolrTestCase` per repo rules):
`or(false,false,true)` → `true`, `or(false,false,false)` → `false`,
`or(true,false,false)` → `true`, a 4-arg mix; plus a guard that
`and(false,false,true)` still returns `false`.

## Limits / risks

- The `(Boolean)` cast in the new loop is safe: `validateValues` rejects
  nulls and non-Booleans before the loop runs.
- No other evaluator's semantics changed; only `OrEvaluator` overrides the
  reduction.
- No new tests were added in this phase per the contribution workflow.

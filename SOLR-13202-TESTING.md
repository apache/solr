# SOLR-13202 — Testing Handoff

> **UNCOMPILED / UNTESTED.** This patch was written against `apache/solr` main
> at `c3e18f1e455` without running a build or any tests. The reviewer (or CI)
> must compile and validate before merge.

## What changed

`solr/core/.../search/JoinQParserPlugin.java` — `Method.parseJoin()` now throws
`SolrException(BAD_REQUEST, ...)` when the `from` or `to` local param is
missing, instead of letting nulls flow into `new JoinQuery(...)` where they
later crashed with an uncaught NPE in `JoinQuery.hashCode()` (HTTP 500 via
`QueryResultKey`). This matches the constructor's own non-null contract
(`assert null != fromField/toField`) and the plugin's existing BAD_REQUEST
style. `fromIndex` remains legitimately nullable.

Also added: `changelog/unreleased/SOLR-13202.yml`.

## Suggested reviewer validation

```bash
./gradlew :solr:core:compileJava -Pvalidation.errorprone=true
./gradlew :solr:core:test --tests "org.apache.solr.search.TestJoin*"
```

Suggested new coverage (not included): `fq={!join}`, `fq={!join to=a}`,
`fq={!join from=b to=a}` (no `v`) should now return 400, not 500; also
`method=topLevelDV` variants, which share `parseJoin`.

## Limits / risks

- The `dvWithScore` method delegates to `ScoreJoinQParserPlugin` and does not
  go through `parseJoin`; its null handling was left untouched (out of scope).
- The public static `createJoinQuery(...)` helper still passes caller-provided
  args straight into `new JoinQuery(...)` — programmatic callers keep the old
  behavior; only malformed query strings are now rejected at parse time.
- No new tests were added in this phase per the contribution workflow.

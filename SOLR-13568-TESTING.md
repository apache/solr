# SOLR-13568 — Testing Handoff

> **UNCOMPILED / UNTESTED.** This patch was written against `apache/solr` main
> at `c3e18f1e455` without running a build or any tests. The reviewer (or CI)
> must compile and validate before merge.

## What changed

`solr/core/src/java/org/apache/solr/handler/component/ExpandComponent.java`

With `expand=true`, the per-page group query (rebuilt for every page of
results) was added to `newFilters` as a plain cacheable query, so every
distinct page inserted a single-use bitset into the filter cache — flooding it
with entries that are never hit again.

The fix wraps the group query in a `WrappedQuery` with `setCache(false)`
before adding it to `newFilters`. `SolrIndexSearcher.getProcessedFilter`
already honors `ExtendedQuery.getCache() == false` via its `notCached` list,
so the filter bypasses the filter cache while behaving identically otherwise.
This mirrors the existing precedent in `TermsQParserPlugin`.

Also added: `changelog/unreleased/SOLR-13568.yml`.

## Suggested reviewer validation

```bash
./gradlew :solr:core:compileJava -Pvalidation.errorprone=true

# expand / collapsing suites
./gradlew :solr:core:test --tests "org.apache.solr.search.TestExpandComponent"
./gradlew :solr:core:test --tests "org.apache.solr.search.TestCollapseQParserPlugin"
```

Suggested new coverage (not included): assert the filter ExpandComponent adds
is an `ExtendedQuery` with `getCache() == false`; integration check that two
`expand=true` queries on different pages insert no new filter-cache entries
while expanded results stay correct.

## Limits / risks

- Query semantics are unchanged; only filter-cache insertion behavior changes.
- No new tests were added in this phase per the contribution workflow.

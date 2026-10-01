# SOLR-12849 — Testing Handoff

> **UNCOMPILED / UNTESTED.** This patch was written against `apache/solr` main
> at `c3e18f1e455` without running a build or any tests. The reviewer (or CI)
> must compile and validate before merge.

## What changed

`solr/core/.../servlet/HttpSolrCall.java` — `addCollectionParamIfNeeded()` now
reads the existing `collection` param from the merged URL + body params
(`getQueryParams()`) instead of the URL query string only (`queryParams`).
Previously, a POST form body like `collection=<alias>` survived the
alias-resolution rewrite untouched (the early-return saw no URL param), and
the distributed query path then failed with `BAD_REQUEST "Could not find
collection : <alias>"` because it resolves the raw param without alias
handling. The identical request as GET worked because the URL param was
rewritten to the resolved collection list. POST now behaves exactly like GET.

Also added: `changelog/unreleased/SOLR-12849.yml`.

## Suggested reviewer validation

```bash
./gradlew :solr:core:compileJava -Pvalidation.errorprone=true
./gradlew :solr:core:test --tests "org.apache.solr.cloud.*Alias*"
```

Suggested new coverage (not included): mini-cloud test — create a collection
plus a single-collection alias, then POST `/solr/<alias>/select` with form
body `q=*:*&collection=<alias>` → expect 200 (was 400); mirror with GET to
confirm identical behavior; assert routed docs come from the aliased
collection. Regression checks: no `collection` param, and an already-resolved
comma list, behave as before.

## Limits / risks

- A POST body `collection` param pointing at a *different* collection than the
  resolved list is now overwritten with the resolved list — matching the
  long-standing GET behavior (the ticket's "silently ignored" observation).
- At both call sites (`HttpSolrCall.init`, `V2HttpCall.init`) `solrReq` is
  already created, so `getQueryParams()` returns the merged params; if
  `solrReq` were null it would fall back to `queryParams`, i.e. the old
  behavior.
- No new tests were added in this phase per the contribution workflow.

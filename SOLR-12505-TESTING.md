# SOLR-12505 - hypothetical-reproduction handoff

**Read this first: the regression test on this branch was written without being compiled or run.** The research/implement pipeline has
no Gradle access, so the test is a best-guess reproduction. Treat it as a hypothesis to confirm, not as proof.

- JIRA: https://issues.apache.org/jira/browse/SOLR-12505 - "Streaming expressions - fetch() does not work as expected" (reported against 7.3.1, still open)
- Branch: `solr-12505-submit`, based on `apache/solr` main `14c7aac0d15`
- Commits: (1) fix + test, (2) this file (kept separate so it can be dropped before opening a PR)

## The bug, as understood
`FetchStream` batches left-hand keys into a query of the form `{! df=<rightKey> q.op=OR cache=false } k1 k2 ...` and sends it to the target collection's
`/select` with no `defType`. `df`/`q.op` local params only take effect under the lucene parser. If the handler's default `defType` is something else
(reporters hit it with `defType=edismax` in `/select`; Eric Pugh confirmed in the ticket), the keys are parsed by that parser instead, nothing matches, and
`fetch()` silently returns the un-enriched input tuples (leftOuterJoin works because it uses a different path).
The ticket discussion (Joel Bernstein, David Smiley) settled on sending `defType=lucene` as a request param rather than `{!lucene ...}` local params, because
SOLR-11501 restricts parser selection through local params.

## What the branch changes
- `FetchStream.java`: adds `params.add("defType", "lucene")` to the batch request.
- `StreamDecoratorTest.testFetchStreamWithNonLuceneDefaultDefType`: creates a 1-shard collection, rewrites `/select` via the Config API to default to `defType=edismax`,
  indexes 3 docs, runs `fetch(... on="id=a_i" ...)` and asserts all 3 tuples are enriched with `subject`.

## What was guessed (verify these first)
1. **Config API call**: the test uses `V2Request.Builder("/c/<coll>/config")` with `update-requesthandler` on `/select`. If `/select` is not declared in the `conf`
   configset's solrconfig (implicit only), `update-requesthandler` will fail; switch to `create-requesthandler` or use a dedicated configset.
2. **Does the test fail without the fix?** Expected: yes (tuples come back without `subject`, so `getString("subject")` returns null). Unverified. If it passes on the base
   commit, edismax is not actually ignoring the local params in this configset and the premise of the ticket needs re-checking on current main.
3. Imports/formatting: `SolrRequest`, `V2Request` imports were added by hand; run spotless.
4. Config changes propagate asynchronously; the V2 config call normally waits for all replicas, but if the test is flaky add a wait.

## How to verify (Gradle required; not run here)
```
.\gradlew :solr:solrj-streaming:spotlessApply
.\gradlew :solr:solrj-streaming:test --tests "org.apache.solr.client.solrj.io.stream.StreamDecoratorTest.testFetchStream*"
```
Then fail-before: revert only the `FetchStream.java` hunk and confirm the new test fails for the reason above.

## Not done
No changelog fragment (add `changelog/unreleased/SOLR-12505-*.yml` with author `Nick Shanin` before any PR). No JIRA comment or PR was created.

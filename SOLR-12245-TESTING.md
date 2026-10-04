# SOLR-12245 - hypothetical-reproduction handoff

**Read this first: nothing on this branch was compiled or run.** The research/implement pipeline has no Gradle access. The fix and test are best-guess; treat them as hypotheses. This ticket was first skipped as cosmetic and revisited because the user wanted a guessed change and test.

- JIRA: https://issues.apache.org/jira/browse/SOLR-12245 - "DistributedUpdateProcessor doesn't set MDC in some errors" (Varun Thacker, 6.6.2). A logged `DistributedUpdatesAsyncException: Async exception during distributed update: Read timed out` does not say which shard/collection/replica the update was going to.
- Branch: `solr-12245-submit` off `apache/solr` main `14c7aac0d15`
- Commits: fix + test, changelog fragment, this file (drop the doc before a PR)

## The bug, as understood
`DistributedUpdatesAsyncException.buildMsg` uses only `error.e.getMessage()`. The `SolrError` carries the `Req` whose `Node` knows the target URL, collection and shard id, but none of it reaches the message. The ticket asks for the MDC to be set; the error text is the more portable fix (MDC is also applied by the request logging for the sending core, which already exists on current main as far as I could see, but not verified).

## What the branch changes
- `DistributedUpdatesAsyncException`: new `describe(SolrError)` appends ` (while sending to <url>, collection=<c>, shard=<s>)` when the error's request and node are known; unchanged otherwise, so `testStatusCodeOnDistribError_NotSolrException` (no `req`) keeps its exact message.
- New `DistributedUpdateProcessorTest.testDistribErrorMessageNamesTheTargetReplica` with an anonymous `SolrCmdDistributor.Node`.

## What was guessed (verify these first)
1. `SolrCmdDistributor.Node` has exactly the eight abstract methods implemented in the test (`getUrl`, `checkRetry(SolrError)`, `getCoreName`, `getBaseUrl`, `getNodeProps`, `getCollection`, `getShardId`, `getMaxRetries`) and `Req`'s four-argument constructor accepts nulls for `cmd` and `uReq`.
2. The SolrError's `req` is "the request that happened to be executed when the error was triggered" (see the Javadoc on `SolrError.req`), so the replica named can occasionally be a neighbour of the one that actually failed in a merged batch.
3. Other places that match on this message text (clients, tests asserting the whole string) were only searched under `solr/core/src/test`.
4. Changelog type `changed` (message wording), not `fixed`.

## How to verify (Gradle required; not run here)
```
.\gradlew :solr:core:spotlessApply
.\gradlew :solr:core:test --tests "org.apache.solr.update.processor.DistributedUpdateProcessorTest"
```
Fail-before: revert only `DistributedUpdateProcessor.java`.

## Not done
No JIRA comment, no PR. Changelog author is `Nick Shanin` per the ICLA note.

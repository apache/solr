# SOLR-16910 - hypothetical-reproduction handoff

**Read this first: the fix and test on this branch were written without being compiled or run.** The audit pipeline has no Gradle access. Treat both as hypotheses.

- JIRA: https://issues.apache.org/jira/browse/SOLR-16910 - "Adjusting LogUpdateProcessorFactory's log level has side effects on SolrCore logging and changes slowUpdateThresholdMillis formatting" (Chris Hostetter, 2023). Reopened from a skip by the skip audit (audit-1).
- Branch: `solr-16910-submit` off `apache/solr` main `e2cdb2d7e8ae`

## The bug, as understood
`LogUpdateProcessor.finish()` called `getLogStringAndClearRspToLog()` once for the INFO line and again for the slow-update WARN. The first call clears `rsp.toLog`, so when both fire the WARN message loses the request details (`webapp=... path=... params=...`) and is formatted differently from the INFO one. The helper also appended the "... (N adds)" truncation entries on every call. Side effect on SolrCore: `rsp.toLog` is cleared only when INFO is enabled or the update is slow, which changes what SolrCore's request logger prints.

## What the branch changes (minimal)
- `finish()` computes `logInfo` and `logSlow` up front, builds the log string once (`getLogString()`), clears `rsp.toLog` once, then logs INFO and/or the WARN from the same string. The truncation entries are added once.
- Not changed: when neither INFO nor slow-WARN fires, `rsp.toLog` is left for SolrCore to log (as before).
- Test: new `LogUpdateProcessorFactoryTest` (factory with `slowUpdateThresholdMillis=0`, rsp with `webapp`/`path` toLog entries, `LogListener` on INFO and WARN) asserts both messages contain the toLog content and that it is cleared.

## What was NOT changed
The ticket also discusses a larger redesign (a processor that only adds to `rsp.toLog` and never logs itself, as Smiley's company does). Not attempted; it needs a maintainer decision.

## Guesses to verify first
1. `LogListener.info(...)`/`warn(...)` capture only their own level and the INFO level is enabled for this logger under the test log4j config.
2. `SolrQueryResponse.addToLog` / `getToLogAsString` produce `webapp=/solr` style output (`key=value` joined by space).
3. `req.getRequestTimer()` is non-null on a request from `req()`.
4. `initCore("solrconfig.xml", "schema.xml")` is unnecessary overhead; any lighter setup (`SolrTestCaseJ4` with `req()` needs a core) is fine.

## Verify (Gradle required; not run here)
```
.\gradlew :solr:core:spotlessApply
.\gradlew :solr:core:test --tests "org.apache.solr.update.processor.LogUpdateProcessorFactoryTest"
```
Fail-before: revert `LogUpdateProcessorFactory.java` only; the WARN assertions should fail.

## Not done
No JIRA comment, no PR.

# SOLR-14718 - hypothetical-reproduction handoff

**Read this first: the fix and test on this branch were written without being compiled or run.** The audit pipeline has no Gradle access. Treat both as hypotheses.

- JIRA: https://issues.apache.org/jira/browse/SOLR-14718 - "Multiple flaws in tracking which UpdateCommand is associated with a given failure logged by ErrorReportingConcurrentUpdateSolrClient: cmd=add{,id=(null)}" (Hoss, 2020). The old skip note said the reporter is an active committer and the spec needs buy-in. Reopened in audit round audit-1 (Tier 1 batch 9); only the first flaw is addressed here.
- Branch: `solr-14718-submit` off `apache/solr` main `9b3a84b1c46`

## The bug, as understood (Hoss's own analysis, re-checked on main)
`JavabinLoader.parseAndLoadDocs` creates one `AddUpdateCommand` per request and, for each document, sets `solrDoc`, calls `processAdd`, then `addCmd.clear()`. `SolrCmdDistributor.distribAdd` handed that same instance to `new Req(cmd, ...)`. The `Req` outlives the call (the update is sent asynchronously), and when a failure is logged later (`ErrorReportingConcurrentUpdateSolrClient.handleError` → `Req.toString`) the shared command has been cleared or refilled with another document, so the log shows `cmd=add{,id=(null)}` or the wrong document. The same `Req.cmd` is also what `SolrError.req.cmd` consumers see after `finish()`.

## What the branch changes
- `SolrCmdDistributor.distribAdd`: one shallow `cmd.clone()` (`UpdateCommand` is `Cloneable`) per call, shared by the `Req` objects for all target nodes. `clear()` on the original no longer affects it; the `SolrInputDocument` is already retained by the `UpdateRequest`, so no extra retention.
- Test: `SolrCmdDistributorTest.testFailedAddKeepsItsDocumentWhenTheCommandIsReused` (called from `test()` like the other sub-tests): connection-failing mock client, `distribAdd`, then `cmd.clear()`, then asserts the error's `req.cmd` still has the document.

## Guesses to verify first
1. `UpdateCommand.clone()` copies `AddUpdateCommand`'s private id fields (it is `Object.clone()`), so `getPrintableId()` on the copy keeps working for a real `req`. In the test `req` is null, so only `solrDoc` is asserted.
2. The test is timing-dependent: if the failure is processed before `cmd.clear()` runs, it passes without the fix too. A deterministic variant would block the mock client until after `clear()`.
3. `MockStreamingSolrClients` with `Exp.CONNECT_EXCEPTION` produces exactly one error with `maxRetries=0` (as `testMaxRetries` does with 6 retries).

## Not fixed (flaws 2+ in the ticket)
The ticket also asks that the error log say the *cause* of the failure and that the message not depend on `UpdateCommand.toString()` defaults; those are logging-format design questions and were not attempted.

## Verify (Gradle required; not run here)
```
.\gradlew :solr:core:spotlessApply
.\gradlew :solr:core:test --tests "org.apache.solr.update.SolrCmdDistributorTest"
```
Fail-before: replace `reqCmd` with `cmd` in `distribAdd`.

## Not done
No JIRA comment, no PR.

# SOLR-13696 - RoutedAliasUpdateProcessorTest: commitWithin check is not a commit barrier, test-only fix (not run)

Nothing here was compiled or executed. Verify the guesses below first.

## JIRA context
`DimensionalRoutedAliasUpdateProcessorTest.testTimeCat` failed with "expected:<16> but was:<15>"
right after `addDocsAndCommit` returned. The abstract base `RoutedAliasUpdateProcessorTest` (and so
`CategoryRoutedAliasUpdateProcessorTest` and `DimensionalRoutedAliasUpdateProcessorTest`) was marked
`@AwaitsFix`. Hossman's log analysis showed the searchers on the replicas opening at different
times. Gus Heck later wrote that the commitWithin part of the test was overzealous (commitWithin is
orthogonal to routed aliases) and that removing it simplifies the helper.

## Finding
Half of the `addDocsAndCommit` runs send the docs with `commitWithin=500` and then poll an alias
query until all the docs are visible. Each replica opens its searcher on its own timer, and the
alias query lands on one replica per shard. The poll can succeed on replicas that have already
committed, return, and the very next assertion lands on a replica whose searcher is not open yet
(15 of 16 docs).

The explicit-commit branch had a second gap: it committed one randomly chosen collection of the
alias, but the docs may have been routed to any collection of the alias.

## Fix (test only)
`addDocsAndCommit` no longer uses `commitWithin`. After the adds it commits every collection of the
alias explicitly (a commit is distributed to all replicas and opens a new searcher by default). The
now-unused `queryNumDocs` helper and `Collectors` import are removed, and `@AwaitsFix` is removed
from the base class so its subclasses run again. `TimeRoutedAliasUpdateProcessorTest` keeps its own
`@AwaitsFix` for SOLR-13059, which is a separate "no core retrieved" race.

## What was guessed / verify first
- That `@AwaitsFix` on the abstract base is inherited by the subclasses (the original commit message
  says so); `CategoryRoutedAliasUpdateProcessorTest` and `DimensionalRoutedAliasUpdateProcessorTest`
  have no annotation of their own.
- That `CollectionAdminRequest.ListAliases` always lists the alias in `getAliasesAsLists()` when
  `aliasOnly` is true (it was already used for `aliasOnly=false`).
- That the two subclasses are stable now. Beast them (the ticket reported a steady stream of
  failures); other timing issues such as SOLR-13059 may surface once they run again.
- Spotless formatting and unused imports.
- Fail-before is not meaningful: the failure was an intermittent race, so a revert would only
  show it under beasting.

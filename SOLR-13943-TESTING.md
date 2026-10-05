# SOLR-13943 - test-only fix (not run)

Nothing here was compiled or executed. The change was written by reading `upstream/main`;
treat every claim below as a guess to verify first.

## JIRA context
`TimeRoutedAliasUpdateProcessorTest.testDateMathInStart` "multi-threaded race condition due to ZK
assumptions" (Open, assigned to Gus Heck 2019, no follow-up). Hoss's analysis in the ticket: the
test waits on its own watcher on `/aliases.json`, but then reads `router.start` through the
`ClusterStateProvider`, whose `ZkStateReader`/`AliasesManager` may not have refreshed yet, so it
sometimes still sees `2019-09-14T03:00:00Z/DAY`. He proposed either using the watcher's data or
only proceeding once the reader knows the new aliases. The method has been `@AwaitsFix` since.

## Change
- After the document is added, the test polls the provider (re-resolving the alias for the HTTP
  provider) for up to 30 seconds until `router.start` parses as an `Instant`, instead of waiting
  on a second latch and reading once.
- The second `monitorAlias` latch is gone (it is what raced); the first one, which waits for the
  alias creation, is unchanged.
- The method level `@AwaitsFix(SOLR-13943)` is removed.

## What was guessed / verify first
- The whole class is still `@AwaitsFix(SOLR-13059)` ("TBD"), so this test will not run in CI until
  that is lifted. Verify with `-Dtests.awaitsfix=true`.
- That the start rewrite always happens: if no rewrite ever occurs (a real bug rather than a race)
  the loop times out after 30 s with the old failure message.
- `getAliasProperties` may have no `router.start` entry (null) before the alias is visible; that
  case is retried like a start that still has date math.
- Fail-before is not meaningful for a race fix; use repeated runs of this method with the old
  and new test body and a fixed seed such as the one in the ticket (`8879E35521A4B9EA`).
- Spotless formatting.

# SOLR-12864 - hypothetical reproduction (not run)

Nothing here was compiled or run. The test was guessed from reading `upstream/main`.

## JIRA context
Alexandre Rafalovitch: `/update/json/docs?mapUniqueKeyOnly=true&df=text&echo=true` echoed the `id` but an empty `text`
list for every record, while the same request without `echo` indexed the values. With `srcField` the echo was complete.

## What the code shows on main
`JsonRecordReader` reuses one `LinkedHashMap` for all records and removes the record's keys in a `finally` after
`handler.handle` returns. `JsonLoader.getDocMap` for `mapUniqueKeyOnly` builds a fresh `deepValues` list from
`result.values()` before the handler returns, so the echoed `df` list is a copy and should hold the values. The old
view-over-the-reused-map shape that would produce `[]` is gone, but `JsonLoaderTest.testEchoDocs` only covers
`[{'id':..}]` records and `testSrcAndUniqueDocs` only covers the indexing path; echo plus `mapUniqueKeyOnly` is unpinned.

## Change
Test-only: `JsonLoaderTest.testEchoDocsWithMapUniqueKeyOnly` echoes two records (one with an array value), with and
without `srcField`, and asserts the `df` list holds the id and the other values and that the original keys are gone.

## Guessed / verify first
- The array value `c` is assumed to arrive as one nested list element (`["1","b",["d","e"]]`); if the reader flattens it
  the expected list needs `"d","e"` inline.
- Expected to pass on main (a pin, not a fix): `NOT_PROVEN` from the fail-before stage is the likely verdict.

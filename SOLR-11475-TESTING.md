# SOLR-11475 - hypothetical-reproduction handoff

**Read this first: nothing on this branch was compiled or run.** The research/implement pipeline has no Gradle access. The fix and test are best-guess; treat them as hypotheses.

- JIRA: https://issues.apache.org/jira/browse/SOLR-11475 - "Endless loop and OOM in PeerSync" (Andrey Kudryavtsev). Pushkar Raste attached a patch (loop counter) and said the scenario (a version `X` on one replica and `-X` on another) is hypothetical;
  the reporter says it arose from SOLR-11459 (in-place updates). Check the ticket for the patch before using this branch.
- Branch: `solr-11475-submit` off `apache/solr` main `14c7aac0d15`
- Commits: fix + test, changelog fragment, this file (drop the doc before a PR)

## The bug, as understood
`PeerSync.MissedUpdatesFinderBase.handleVersionsWithRanges` walks two sorted lists. When `ourUpdates[i] != otherVersions[j]` but `|ourUpdates[i]| == |otherVersions[j]|` (e.g. `42` vs `-42`), neither the "equal" branch nor the
"our abs is smaller" branch applies, so the `else` runs with an inner `while (|other| < |our|)` that does not advance. Neither index moves and the outer `while` spins, appending ranges until OOM. Still true on main (`PeerSync.java` ~L819-836).

## What the branch changes
- `PeerSync.handleVersionsWithRanges`: new branch for equal absolute values with different signs that advances both indexes (treats it as "already have a version at that position"; no range request).
- `PeerSyncTest.testHandleVersionsWithRangesSameVersionDifferentSign`: `other=[-42]`, `ours=[42]`, both `completeList` values; runs the call in a daemon thread with a 30s join so a regression fails instead of hanging; expects no request.

## What was guessed (verify these first)
1. **Semantics**: stepping over `X` vs `-X` silently hides a real inconsistency (an add on one replica, a delete on the other). The ticket discussion alternatively proposes throwing / failing the sync; a maintainer may prefer
   returning `UNABLE_TO_SYNC` (forcing recovery by replication) over ignoring it. That is probably the better behavior and a small change if wanted.
2. **Compile**: the new test method sits in `PeerSyncTest` as a `private static` helper called from `handleVersionsWithRangesTests()`; it uses only imports already present (`List`, `fail`, assertions via the base class). `MissedUpdatesRequest` is already imported there.
3. **Precondition**: `ourUpdates` and `otherVersions` must be sorted with highest abs first, as in the other tests; single-element lists trivially are.

## How to verify (Gradle required; not run here)
```
.\gradlew :solr:core:spotlessApply
.\gradlew :solr:core:test --tests "org.apache.solr.update.PeerSyncTest"
```
Fail-before: revert only `PeerSync.java`; the new assertion fails after the 30s join (the helper thread stays alive).

## Not done
No JIRA comment, no PR. Changelog author is `Nick Shanin` per the ICLA note.

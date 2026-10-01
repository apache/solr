# SOLR-18391 — Testing handoff

**Status: UNCOMPILED AND UNTESTED.** This patch was written without running
Gradle (no compile, no tidy, no tests). A reviewer must compile and test
before this goes anywhere near a PR.

## What the patch does

`PlacementPluginAssignStrategy.assign()` fetched
`placementContext.getCluster().getCollection(collectionName)` and passed the
result straight into `PlacementRequestImpl.toPlacementRequest(...)`, which
dereferences it immediately (`solrCollection.getName()`). When the collection
is missing from cluster state — a race between
`CreateCollectionCmd.waitForState` and the subsequent `getClusterState()` read
during collection CREATE — this NPEd, and the collection was left behind as a
**zombie with 0 replicas and no cleanup**.

The patch null-checks the `getCollection()` result and throws
`Assign.AssignmentException` with a clear message instead. That routes through
the existing failure path (the `AssignmentException` path triggers collection
cleanup), so the NPE is gone and no zombie collection remains.

## Recommended reviewer commands

```bash
# copy gradle.properties into the worktree first (worktrees don't inherit it)
cp ~/workspace/solr/gradle.properties .

# format + error-prone compile
~/workspace/tools/solr-gradle.sh :solr:core:spotlessApply
~/workspace/tools/solr-gradle.sh :solr:core:compileJava -Pvalidation.errorprone=true

# targeted tests (run serially — never two Gradle builds at once on this VM)
~/workspace/tools/solr-gradle.sh :solr:core:test \
  --tests "org.apache.solr.cloud.TestPlacementPlugin*" \
  --tests "org.apache.solr.cloud.api.collections.TestCreateCollection*" \
  -Pvalidation.errorprone=true
```

Suggested new test (not written): unit test on `assign()` with a placement
context whose cluster returns null for the collection → expect
`Assign.AssignmentException`, not NPE. Needs a mock `SolrCloudManager`/cluster.

## Patch limits, risks, open questions

- **Not compiled or tested** — the diff is small and syntactically simple, but
  treat it as unverified until the reviewer runs the commands above.
- The null return from `getCollection()` is inferred from the ticket's
  stack-trace analysis and the race described in `CreateCollectionCmd`; there
  is no direct repro of the race in this patch.
- `AssignmentException` vs retry: an alternative fix would retry
  `getCollection()` briefly before failing. The ticket explicitly notes the
  `AssignmentException` path performs cleanup, which is why the patch chose
  fail-fast over retry. A reviewer who prefers retry-with-timeout should say
  so — it's a judgment call, not a correctness issue.
- No changelog entry added (per repo convention, changelog entries are
  scaffolded once a Jira/PR is assigned; this branch has neither yet).

## What the reviewer should improve

- Verify the `AssignmentException` path in `CreateCollectionCmd` actually
  deletes the partially created collection in current main (the ticket claims
  it does; confirm against the code).
- Decide fail-fast vs retry and adjust if needed.
- Add the unit test sketched above; convert to `SolrTestCase` style per
  current reviewer preference (no J4).
- Remove this file before opening the upstream PR.

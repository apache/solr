# Solr dependency smoke test (Maven + Gradle)

This mini project verifies that the published `solr-solrj` and `solr-test-framework` POMs are
consumable by a standalone build: it resolves those artifacts from a Maven-layout directory and
runs a small test that starts Solr (embedded), creates a collection, and does an
index/commit/query round-trip.

It's normally run via `dev-tools/scripts/checkTestExternalClient.py` (or as part of
`smokeTestRelease.py`), which copies this project to a throwaway directory with no Solr source
tree above it — so it sees exactly what a third-party consumer sees
(`ExternalPaths.SOURCE_HOME == null`) — and supplies the inputs below.

## Inputs

- `solr.version` (required): version of `solr-solrj` / `solr-test-framework` to resolve
- `local.solr.repo` (optional): path to a Maven-layout repository (default: `~/.m2/repository`)
- `smoke.configset.dir` (required): a configSet directory; the runner supplies a minimal one

## Running it directly

```bash
# publish local artifacts to build/maven-local
./gradlew mavenToLocalFolder

python3 dev-tools/scripts/checkTestExternalClient.py \
  --repo-dir build/maven-local 11.0.0-SNAPSHOT
```

Or invoke a single build tool directly (you must supply all inputs, including a configSet):

```bash
(cd test-external-client && mvn \
  -Dsolr.version=11.0.0-SNAPSHOT \
  -Dlocal.solr.repo="$PWD/../build/maven-local" \
  -Dsmoke.configset.dir="$PWD/../solr/test-framework/src/test-files/solr/configsets/minimal" \
  test)
```

Run in place like this and `ExternalPaths` resolves against the Solr source tree rather than
`null`, so use `checkTestExternalClient.py` for a faithful external-consumer run.

# Solr dependency smoke test (Maven + Gradle)

This mini project validates that a standalone build can resolve Solr artifacts from a Maven-style local directory tree and run a basic test using them.

## Inputs

- `solr.version` (required): version of `solr-solrj` and `solr-test-framework`
- `local.solr.repo` (optional): filesystem path to a Maven-layout repository (default: `~/.m2/repository`)

## Run with Maven

```bash
# build local artifacts to build/maven-local
./gradlew mavenToLocalFolder

(cd test-external-client && mvn \
  -Dsolr.version=11.0.0-SNAPSHOT \
  -Dlocal.solr.repo="$PWD/../build/maven-local" \
  test)
```

To force Maven to avoid online repos while resolving, add `-o` (offline).

## Run with Gradle

```bash
# build local artifacts to build/maven-local
./gradlew mavenToLocalFolder

(cd test-external-client && ../gradlew \
  -Psolr.version=11.0.0-SNAPSHOT \
  -Plocal.solr.repo="$PWD/../build/maven-local" \
  test)
```

## Optional: use Maven Wrapper

From the project directory:

```bash
cd test-external-client
mvn -N wrapper:wrapper
```


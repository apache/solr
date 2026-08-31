# AGENTS.md for Apache Solr

While README.md and CONTRIBUTING.md are mainly written for humans, this file is a condensed knowledge base for LLM coding agents on the Solr codebase. See https://agents.md for more info and how to make various coding assistants consume this file. Also see `dev-docs/how-to-contribute.adoc` for some guidelines when using genAI to contribute to Solr.

## Licensing and Dependencies

- Follow Apache Software Foundation licensing rules, avoid adding a dependency with a banned license
- Always apply the Apache License to new source files
- All versions must be delcared in `gradle/libs.versions.toml`, never build.gradle files
- Try first declaring a dependency without a version (the version might already be in a BOM); and if fails to resolve _then_ specify a version
- Always run `gradlew updateLicenses resolveAndLockAll --write-locks` after adding or changing a dependency. See `dev-docs/gradle-help/dependencies.txt` for more info

## Build and Development Workflow

- When done or preparing to commit changes to java source files, be sure to run `gradlew tidy` to format the code.  Don't bother beforehand.
- Always run `gradlew check -x test` before declaring a feature done

## Code Quality and Best Practices

- Use the project's custom `EnvUtils` to read system properties. It auto converts env.var SOLR_FOO_BAR to system property solr.foo.bar
- Be careful to not add non-essential logging! If you add slf4j log calls, make sure to wrap debug/trace level calls in `logger.isXxxEnabled()` clause
- Validate user input. For file paths, always call `myCoreContainer.assertPathAllowed(myPath)` before using
- Never use fully-qualified-names when an import statement will resolve ambiguity.
- SolrJ & "api" modules should use the latest Java 17 language features as appropriate.  Other modules can use Java 21.

## Running Tests

- See `dev-docs/gradle-help/tests.txt` for hints on running tests
- To run a specific test: `gradlew :solr:core:test --tests "org.apache.solr.search.TestCaffeineCache"`
- To run a specific BATS test: `gradlew iTest --tests test_adminconsole_urls.bats`
- The randomization seed is important.  To repeat a failing tests, pass the same seed given in the failure by adding to Gradle: `-Ptests.seed=HEXADECIMALHERE`.
- Test output goes to `solr/<module>/build/test-results/test/outputs/OUTPUT-<fully.qualified.TestName>.txt` (stdout/stderr log) and `solr/<module>/build/test-results/test/TEST-<fully.qualified.TestName>.xml` (JUnit XML with pass/fail/error details)
- To scan test output for a specific issue across already-run tests: `grep -rl "pattern" solr/*/build/test-results/test/outputs/`

## Writing Tests

- When adding a test to an existing suite/file, keep the same style / design choices
- When adding a *new* Java test suite/file:
    - Subclass SolrTestCase, or if SolrCloud is needed then SolrCloudTestCase
    - If SolrTestCase and need to embed Solr, use either EmbeddedSolrServerTestRule (doesn't use HTTP) or SolrJettyTestRule if HTTP/Jetty is relevant to what is being tested.
    - Avoid SolrTestCaseJ4 for new tests
- For BATS shell integration tests in `solr/packaging/test/`:
    - Always use `run <command>` followed by `assert_output --partial "..."` or `refute_output --partial "..."` instead of capturing output into local variables and using `[[ ]]` comparisons
    - Avoid patterns like `local var=$(cmd | grep ...); [[ "$var" == *"..."* ]]` — use `run cmd` + `assert_output`/`refute_output` instead

## Documentation

- For major or breaking changes, add a prominent note in reference guide major-changes-in-solr-X.adoc
- Always consider whether a reference-guide page needs updating due to the new/changed features. Target audience is end user
- For changes to build system and other developer-focused changes, consider updating or adding docs in dev-docs/ folder
- Keep all documentation including javadoc concise
- New classes should have some javadocs
- Changes should not have code comments communicating the change, which are instead great comments to leave for code review / commentary

## Developer Docs Index

Before diving into code on these topics, read the matching doc in `dev-docs/`. When adding a new dev doc, add a line here.

Internals:

- `dev-docs/overseer/overseer.adoc` — Overseer: cluster state updates, ZkStateWriter, collection API message flow
- `dev-docs/shard-split/shard-split.adoc` — SPLITSHARD: shard/replica states, tlog buffering during split
- `dev-docs/distributed-update-internals.adoc` — SolrCloud update path: routing, `_version_`/optimistic concurrency, tlog durability, replication acks, shard terms (user-facing consistency model: ref-guide page `solrcloud-update-consistency.adoc`)
- `dev-docs/plugins-modules-packages.adoc` — plugin/module/package concepts
- `dev-docs/apis.adoc`, `dev-docs/v2-api-conventions.adoc` — API design and v2 conventions
- `dev-docs/ui/` — new Admin UI architecture, component development, testing

Process & tooling:

- `dev-docs/solr-source-code.adoc`, `git.adoc`, `IDEs.adoc`, `jvms.adoc` — build and dev environment
- `dev-docs/ref-guide/` — ref-guide authoring (AsciiDoc syntax, Antora templates)
- `dev-docs/dependency-upgrades.adoc`, `lucene-upgrade.md`, `working-between-major-versions.adoc` — upgrades and branch management
- `dev-docs/releasing.adoc`, `changelog.adoc`, `asf-jenkins.adoc` — release and CI process

## Changelog

- We use the "logchange" tooling to manage our changelog. See `dev-docs/changelog.adoc` for details and conventions
- To scaffold a new changelog entry, run `gradlew writeChangelog` (JIRA) or `gradlew writeChangeLogPr` (no JIRA), and then edit the new file located in `changelog/unreleased/`.
- Do not add a changelog entry before a JIRA issue or a Github PR is assigned, as one is required.

## Issue Tracking (JIRA)

Solr issues are tracked at https://issues.apache.org/jira (project key `SOLR`). The anonymous REST API works; no auth or scraping needed:

- Search: `curl "https://issues.apache.org/jira/rest/api/2/search?jql=<url-encoded JQL>&fields=summary,status,resolution&maxResults=10"` with JQL like `project=SOLR AND text~"some phrase" ORDER BY updated DESC`
- Single issue (with comments): `curl "https://issues.apache.org/jira/rest/api/2/issue/SOLR-12345?fields=summary,description,comment"`

## Git Branches and Repository History

- This repo's history predates the Lucene/Solr split: before Solr 9.0, Solr was released jointly with Lucene from a combined `lucene-solr` repository. Commits and tags from that era are still present here.
- Release tags come in two families — check both when doing any git-blame/version archaeology (e.g. figuring out which Solr version something was added or deprecated in), or you will silently get a too-recent answer for anything before the split:
    - `releases/lucene-solr/X.Y.Z` — joint releases, pre-9.0 (Solr versions up to 8.x)
    - `releases/solr/X.Y.Z` — standalone Solr releases, 9.0 onward
- Exclude `releases/lucene/*` (pure Lucene releases, not Solr), and `grafts/*` / `history/branches/*` refs (historical/graft markers, not real releases) from any version lookup.
- Example pattern to find the earliest release containing a commit: `git tag --contains <hash> | grep -E '^releases/(solr|lucene-solr)/[0-9]+\.[0-9]+(\.[0-9]+)?$' | sed -E 's#^releases/(solr|lucene-solr)/##' | sort -V | head -1`
- The active development branch is `main`; its in-progress version is the `baseVersion` string in the root `build.gradle`. Maintenance branches for prior lines follow the `branch_9x`, `branch_10x`, etc. naming pattern.

## Security

For security findings, follow the project's threat model:
[THREAT_MODEL.md](THREAT_MODEL.md) — the trust boundaries, the load-bearing
auth+authz / trusted-environment posture, the properties Solr provides vs. those
left to the operator (notably: never expose an unauthenticated Solr to an
untrusted network; SSRF via `shards`/streaming is bounded by operator network
controls; risky features are off by default), and the recurring non-findings.
Reporting is via [SECURITY.md](SECURITY.md). Route any scanner/AI-generated
finding through `THREAT_MODEL.md` section 13 before reporting.

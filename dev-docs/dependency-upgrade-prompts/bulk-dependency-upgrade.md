# Prompt: Merge solrbot dependency upgrade PRs into main branch

## Context

Apache Solr uses [Renovate bot](https://github.com/solrbot) (`solrbot`) to open dependency upgrade PRs automatically.
This task merges all eligible solrbot PRs for the `main` branch into a combined feature branch, fixes any resulting
build or test failures, and prepares the branch for review.

The feature branch is named `deps-main-YYYY-MM-DD` using today's date (e.g. `deps-main-2026-04-21`).
Check whether such a branch already exists (from a previous run of this task). If so, inspect its state
and either continue from where it left off or recreate it from a fresh `main`.

---

## Step 1 — Identify eligible PRs

Use the GitHub CLI to list all open, non-draft solrbot PRs targeting `main`:

```
gh pr list --author solrbot --state open --limit 200 \
  --json number,title,headRefName,baseRefName,isDraft,statusCheckRollup
```

Filter to PRs where **all** of the following are true:
- `baseRefName == "main"`
- `isDraft == false`
- All status checks are `SUCCESS`, `NEUTRAL`, or `SKIPPED` (no `FAILURE`, `PENDING`, or `CANCELLED`)

### Major vs minor/patch upgrades

Renovate opens **two separate PR variants** for some dependencies:
- A **minor/patch PR** (e.g. "Update netty to v4.1.120") — incremental, low risk.
- A **major PR** whose title contains `(major)` (e.g. "Update jersey to v4 (major)") — breaking API changes possible.

Both variants are eligible. Sort all eligible PRs by PR number descending. For each dependency that has
**both** a major and a minor/patch PR in the list:
1. Attempt the **major** PR first.
2. If it merges cleanly (or with only lockfile/TOML conflicts resolvable by the rules in Step 3),
   include it and skip the minor/patch PR for that dependency.
3. If the major PR causes **code conflicts** that cannot be resolved by simple version-line edits,
   **abort that merge**, record it as a failed attempt, and fall back to the minor/patch PR instead.
4. If there is no minor/patch PR either, skip that dependency entirely and record it as skipped.

When two PRs update the **same dependency** to different non-major versions, keep only the one with the
higher target version; skip the lower one entirely.

---

## Step 2 — Prepare the feature branch

```
git checkout main && git pull upstream main
git checkout -b deps-main-$(date +%Y-%m-%d)   # e.g. deps-main-2026-04-21
git remote get-url solrbot &>/dev/null || git remote add solrbot git@github.com:solrbot/apache-_-solr.git
git fetch solrbot
```

---

## Step 3 — Merge each PR

For each eligible PR (in descending PR-number order):

```
git merge --no-edit solrbot/<headRefName>
```

### 3a — Lockfile conflicts (`*.lockfile`)

Take **ours** for every `*.lockfile` conflict — they will be fully regenerated in Step 4:

```
git diff --name-only --diff-filter=U | while read f; do
  if [[ "$f" == *.lockfile ]]; then
    git checkout --ours "$f" && git add "$f"
  fi
done
```

### 3b — `gradle/libs.versions.toml` conflicts

Resolve by **keeping the newer version** for every conflicting line. Typical pattern: a previous merge
already bumped `foo = "1.2.0"` but the current PR's base still has `foo = "1.1.0"`. Always keep the
highest version; never downgrade a dependency that a prior merge already upgraded.

After editing away all conflict markers, `git add gradle/libs.versions.toml`.

**Important**: never use `git checkout --ours` on the TOML file — that would silently drop the version bump.

Commit with: `git commit --no-edit -m "Merge PR #<N> (<branch>) - resolved TOML/lockfile conflicts"`

### 3c — Skip / abort conditions

- If the TOML conflict would **downgrade** a dependency already at a higher version (e.g. merging
  `kotlin-logging v7` when `v8` is already merged), **abort the merge and skip** that PR.
- If a conflict occurs in any file other than `*.lockfile` or `gradle/libs.versions.toml`, investigate
  before proceeding.

---

## Step 4 — Regenerate all lockfiles

```
./gradlew resolveAndLockAll --write-locks
```

**Important caveat for `solr/ui`**: the local environment typically lacks the Node.js/wasm toolchain
needed to resolve Kotlin Multiplatform `wasmJs*` configurations. After `resolveAndLockAll`, check
`solr/ui/gradle.lockfile` for any dependency that appears at **two different versions** — one for
`desktop*` configurations (the new version) and one for `wasmJs*` configurations (an old stale version).
If found, manually merge those split entries so all configurations use the single new version.
Use `git show solrbot/<headRefName>:solr/ui/gradle.lockfile` on the originating PR branch to see what
the fully-resolved lockfile should look like for the affected dependency.

Commit all changed lockfiles:
```
git add -A && git commit -m "Regenerate all Gradle lockfiles after merging dependency updates"
```

---

## Step 5 — Check version compatibility between upgraded dependencies

Some dependencies must stay in lock-step. After merging, verify:

**Jetty / dropwizard-metrics compatibility**
`dropwizard-metrics` 4.2.34+ requires Jetty 12.1.x. Solr's embedded server targets Jetty 12.0.x.
If `dropwizard-metrics` was upgraded to ≥ 4.2.34, check whether it pulls `jetty-ee10-servlet:12.1.x`
onto the classpath alongside `jetty-http:12.0.x` (or vice versa) — this causes a runtime
`NoSuchFieldError: HttpCompliance RFC9110` in tests. Resolution: downgrade `dropwizard-metrics` to the
highest 4.2.x version whose parent POM declares `<jetty12.version>12.0.x</jetty12.version>`, and upgrade
`eclipse-jetty` to the latest `12.0.x` patch release.

Check the Jetty version a specific dropwizard version requires:
```
curl -s https://repo1.maven.org/maven2/io/dropwizard/metrics/metrics-parent/<VERSION>/metrics-parent-<VERSION>.pom \
  | grep jetty12.version
```

**jersey-container-jetty-http**
`jersey-container-jetty-http:4.x` is compatible with Jetty 12.0.x — no issue there.

When manually adjusting a version that was originally from a solrbot PR, **also update the
`changelog/unreleased/` entry** that the PR created (see Step 6).

---

## Step 6 — Changelog entries

Solrbot PRs each create a file in `changelog/unreleased/PR#<N>-<slug>.yml`. Rules:

- When a solrbot PR merges cleanly, its changelog file is included automatically — leave it alone.
- When you **manually change a version** (e.g. downgrade dropwizard, upgrade jetty to a specific
  patch release) that did not come directly from a solrbot PR:
  1. If a solrbot changelog entry exists for that dependency, update its `title:` line to reflect the
     actual version and **rename** the file to something not tied to a PR number
     (e.g. `update-dropwizard-metrics-4.2.33.yml`).
  2. If there is no existing entry, create a new `changelog/unreleased/<slug>.yml`:
     ```yaml
     title: Update <dependency> to v<version>
     type: dependency_update
     authors:
     - name: <your-github-username>
     links:
     - name: PR#4305
       url: https://github.com/apache/solr/pull/4305
     ```

---

## Step 7 — Update license checksums

```
./gradlew updateLicenses
git add -A
git status --short | grep -E "sha1|license"
```

Review the added/removed sha1 files — every upgraded dependency that ships a jar should have an old
sha1 removed and a new one added. Commit if there are changes.

---

## Step 8 — Build validation

```
./gradlew check -x test
```

Must produce **BUILD SUCCESSFUL**. Fix any failures before continuing.

Common failure types:
- `validateJarChecksums` — re-run `./gradlew updateLicenses` then commit the missing sha1 files.
- Compile errors — investigate which upgraded dependency changed an API used in Solr source.

---

## Step 9 — Full test run

```
./gradlew test
```

Investigate every test failure. For each:

1. Check whether it was pre-existing on `main` (run the same test against `main` to confirm).
2. If introduced by an upgrade, identify which PR caused it by bisecting or inspecting the
   dependency change.

### Security policy failures

If a test fails with `java.security.AccessControlException: access denied ("java.security.SecurityPermission" "getProperty.org.bouncycastle.*")`:

Do **not** add all properties found in the BC jar. Instead, run the failing test repeatedly in a
loop, adding only the one denied property each iteration, until the test passes:

```bash
while true; do
  ./gradlew :<module>:test --tests "<TestClass>" -Ptests.useSecurityManager=true 2>&1
  DENIED=$(grep "access denied.*getProperty" <output-file> | grep -oE '"getProperty\.[^"]*"' | sort -u)
  [ -z "$DENIED" ] && break
  # add each denied permission to the policy file, then loop
done
```

Add the discovered permissions to **both**:
- `gradle/testing/randomization/policies/solr-tests.policy` (under the existing BC comment block)
- `solr/server/etc/security.policy` (near the existing `getProperty.org.bouncycastle.*` entries)

---

## Step 10 — Deduplicate changelog entries

Because no Solr release has occurred between the solrbot PRs being merged, users do not care about
intermediate version bumps for the same dependency. If a dependency was upgraded multiple times
(e.g. jetty 12.0.27 → 12.0.30 → 12.0.34, each as a separate PR), only the final version is relevant.

For every group of `changelog/unreleased/*.yml` entries that refer to the **same dependency**:
1. Identify which entry has the highest target version — keep that one.
2. Delete all other entries for that dependency.
3. Ensure the surviving entry's `title:` reflects the final version.
4. If the surviving entry's filename is tied to an intermediate PR number, rename it to a
   version-based name (e.g. `update-eclipse-jetty-12.0.34.yml`).

To find duplicates, list all entries and look for repeated dependency names:
```
ls changelog/unreleased/*.yml | xargs grep -l "title:" | xargs grep "title:" | sort
```

Or inspect them all:
```
for f in changelog/unreleased/*.yml; do echo "=== $f ==="; cat $f; done
```

Commit the result:
```
git add -A
git commit -m "Deduplicate changelog entries: keep only final version per dependency"
```

---

## Step 11 — Final commit summary

After all fixes, the branch should have a clean `git log` showing:
- One merge commit per solrbot PR (with conflict-resolution notes where applicable)
- A lockfile regeneration commit
- Any manual version-adjustment commits with updated changelog entries
- A final license-checksum commit (if needed)
- Any test-fix commits (policy files, etc.)
- A changelog deduplication commit

Do **not** push to the remote — the author will handle that.

---

## Step 12 — Output a pull request description

Print the following block of GitHub-Flavored Markdown to the console, ready to paste as the PR description.
Fill in the table with every PR that was **successfully merged**, and add the optional failure paragraph
if any merges were attempted but aborted.

```markdown
## Dependency upgrades — main branch (YYYY-MM-DD)

This branch combines [solrbot](https://github.com/solrbot) dependency upgrade PRs that had all CI
checks passing on `main` as of YYYY-MM-DD.

Lockfiles were regenerated, license checksums updated, version-compatibility issues resolved, and
the full test suite verified locally.

### Successfully merged PRs

| PR | Dependency | Version |
|----|-----------|---------|
| [#NNN](https://github.com/apache/solr/pull/NNN) | `<dependency>` | `<version>` |
| … | … | … |

### Failed / skipped merge attempts

The following PRs could not be merged cleanly and were skipped:

| PR | Dependency | Reason |
|----|-----------|--------|
| [#NNN](https://github.com/apache/solr/pull/NNN) | `<dependency>` | Code conflict / incompatible major upgrade / … |
```

Omit the "Failed / skipped" section entirely if there were no failures.

---

## Reference: key files

| Purpose | File |
|---------|------|
| Dependency versions | `gradle/libs.versions.toml` |
| Per-module lockfiles | `solr/*/gradle.lockfile`, `solr/modules/*/gradle.lockfile` |
| UI lockfile (wasm quirk) | `solr/ui/gradle.lockfile` |
| Jar checksums | `solr/licenses/*.jar.sha1` |
| Test security policy | `gradle/testing/randomization/policies/solr-tests.policy` |
| Server security policy | `solr/server/etc/security.policy` |
| Changelog entries | `changelog/unreleased/*.yml` |

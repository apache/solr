# SOLR-13706 — Testing Handoff

> **UNCOMPILED / UNTESTED.** This patch was written against `apache/solr` main
> at `c3e18f1e455` without running a build or any tests. The reviewer (or CI)
> must compile and validate before merge.

## What changed

1. `solr/core/src/java/org/apache/solr/core/PluginInfo.java` (`writeMap`):
   children are now grouped by **`child.type`** instead of `child.name`.
   The old name-based grouping had two defects:
   - a child with no `name` attribute (e.g. the `<highlighting>` element)
     produced a `null` map key, which NPEs in noggit JSON serialization
     (`MapWriter` impl calls `k.toString()`);
   - unrelated plugin types sharing a name were merged and lost their type
     identity (e.g. `<formatter name="html">` and `<encoder name="html">`
     collapsed into one list under `"html"`; same for `fragmentsBuilder` /
     `boundaryScanner` both named `"default"`).
   The existing single-vs-list duplicate logic is preserved, now *within*
   each type. Each child's `name` (when present) is still serialized inside
   the child's own attributes map.
2. `solr/core/src/java/org/apache/solr/core/SolrConfig.java` (`writeMap`):
   removed the `// TODO remove after fixing SOLR-13706` workaround that
   silently dropped the `highlight` searchComponent from Config API output.
3. Added `changelog/unreleased/SOLR-13706.yml`.

This matches the contract the consumers already use: `HighlightComponent.inform`
→ `info.getChildren("highlighting")`, `DefaultSolrHighlighter.init` →
`getChildren("fragmenter"/"formatter"/"encoder"/"fragListBuilder"/
"fragmentsBuilder"/"boundaryScanner")`.

## Suggested reviewer validation

```bash
./gradlew :solr:core:compileJava -Pvalidation.errorprone=true

# Config API / PluginInfo-related suites
./gradlew :solr:core:test --tests "org.apache.solr.handler.TestSolrConfigHandler"
./gradlew :solr:core:test --tests "org.apache.solr.highlight.*"
```

Suggested new coverage (not included): a `PluginInfo.writeMap` unit test with a
highlight-style hierarchy asserting `highlighting` appears under its type key
(no null key) and `formatter`/`encoder` (both named `html`) stay under separate
type keys; plus a `TestSolrConfigHandler` assertion that
`GET /config/searchComponent/highlight` returns the highlighting children by
type.

## Limits / risks

- Only plugins with XML `<children>` change serialized shape (highlight, and
  e.g. XML-defined spellcheck `<spellchecker>` entries). That output was
  previously broken (null keys / type-merged lists), so no working consumer
  should depend on the old shape.
- The Config API POST path (`PluginInfo(String, Map)`) treats list-of-maps
  under a key as subcomponents and folds them into `initArgs` — unchanged by
  this patch; a full POST round-trip of highlight children was already lossy
  before and remains so.
- No new tests were added in this phase per the contribution workflow.

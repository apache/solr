<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# AI join sidecar index

`AIJoinIndex` owns an auxiliary Lucene index persisting, for every (from-segment, to-segment)
pair, a SORTED_NUMERIC column mapping from-side doc ids to to-side doc ids, plus edges columns
with the pair's `{min, max}` doc bounds and a to-side count. Pair columns are named by both
sides' persistent side keys (segment id + docvalues generation of the join field), so they
survive reopens of either side and are built lazily — the first `AIJoinQuery` weight that needs
a pair writes it, unless `AIJoinIndex#ensureJoinSegments` already built it eagerly at
`createWeight` time. See `AIJoinIndex` for the user-facing API and `AIJoinIndexConfig` for
tunables (blocking vs. non-blocking refresh, one-field-per-segment, the reaper's sweep
interval).

A batch of pair columns is written through `AIJoinWriter`, which has two interchangeable
implementations selected by `AIJoinIndex.INSTANCE`: `AIJoinDocWriter`, built on the plain
`Document` / `IndexWriter#addDocuments` block-indexing API, and `AIJoinColumnWriter`, built on
`org.apache.lucene.document.column`. Both guarantee a batch lands doc-for-doc in one sidecar
segment, so doc 0's edges and every from-doc id line up the same way.

## Garbage collection of dead pairs

The sidecar is append-only. A side key dies when its segment is merged away, dropped, or its
join field receives an in-place docvalues update (dvGen bump); pair columns referencing a dead
side key can never be read again. `AIJoinMergePolicy` reaps them:

- **Death signal via sampling, not listeners.** `AIJoinIndex#onCreateWeight` (called from
  `AIJoinQuery#createWeight`) reports the set of pair field names a query actually needed to
  `AIJoinMergePolicy#onCreateWeight`, keyed by (from-directory, to-directory). A pair field name
  present in an earlier snapshot for the same searcher pair but missing from a later one is
  queued as a pending removal. Sampling is throttled (`AIJoinIndexConfig#setSweepSamplingInterval`,
  default one minute) since it is only a heuristic hint, not a correctness requirement, and both
  the snapshot map and the pending-removal set are size-bounded (best-effort LRU-ish eviction).
- **Reaping.** `findMerges` drops every sidecar segment whose pair field names are all pending
  removals, via a `OneMerge` that reports the segment as fully deleted (`wrapForMerge` returns a
  `MatchNoBits` live-docs view) so `IndexWriter` discards it instead of rewriting it. This runs
  piggybacked on ordinary merges — no background thread. `TestAIJoinMergePolicy` and
  `droppedSegmentCount()` / `pendingPairRemovalsCount()` cover this end to end.
- **Known gaps.** A pair field name is only queued for removal once a searcher pair that used to
  need it is sampled again *without* needing it — a side key that dies without ever being
  resampled this way stays live. The in-memory `pairBuilds` dedup map (string keys plus completed
  futures) is also never trimmed, bounded only by the number of distinct pairs ever built in the
  process.

## TODO
 - comparative benchmarking for updates: parent, children field, PARENT_ID_FK (`AIJoinBenchmark`
   currently only compares against `JoinUtil` on a static index)
 - many-to-many: `AIJoinUtil` doc mapping currently degrades M:N joins to M:1 (single to-doc kept
   per from-doc)
 - reverse join on the same columns: children by parents filter
 - flush one pair field per sidecar segment, trading a longer sweep for finer-grained reaping
   (`AIJoinIndexConfig#setSingleFieldPerSegment`, currently unused by the writers)
 - AIJoinUtil.getSideKey throws an NPE if the from field was never indexed in any document — it does getFieldInfos().fieldInfo(field).getDocValuesGen() without a null check. JoinUtil-based {!join} degrades gracefully in this situation; AIJoin currently doesn't.


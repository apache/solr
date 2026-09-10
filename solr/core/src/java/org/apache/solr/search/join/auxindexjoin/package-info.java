/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 *
 * <h2>Auxiliary Index Join</h2>
 *
 * Persists doc-nums to doc-nums relation in auxiliary index.
 *
 * <h3>Simplified Collaboration Diagram</h3>
 *
 * <pre>
 * {@link org.apache.solr.search.join.AuxIndexJoinQParserPlugin}
 *       │
 *       ▼
 * {@link org.apache.solr.search.join.auxindexjoin.AuxIndexManager}  ──────▶  {@link org.apache.solr.search.join.auxindexjoin.AuxIndexJoinConfig}
 *       │    │
 *       │    └──────────▶ {@link org.apache.solr.search.join.auxindexjoin.AuxIndexJoinMergePolicy}
 *       ▼
 * {@link org.apache.solr.search.join.auxindexjoin.AuxIndexJoinQuery} ──────▶ {@link org.apache.solr.search.join.auxindexjoin.FromLeafJoinContext}
 *       │                           │
 *       ▼                           ▼
 * {@link org.apache.solr.search.join.auxindexjoin.JoinIndexWeight}             {@link org.apache.solr.search.join.auxindexjoin.ForeignKeyColumn}
 *       │
 *       ▼
 * {@link org.apache.solr.search.join.auxindexjoin.JoinIndexScorerSupplier}   ──────▶   {@link org.apache.solr.search.join.auxindexjoin.JoinColumnIndexer}
 *   │ │                                         │ │
 *   │ ▼                                         │ ▼
 *   │{@link org.apache.solr.search.join.auxindexjoin.JoinIndexScorerSupplier.LeafJoin}             │{@link org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.JoinColumnModel}
 *   ▼                                           ▼
 * {@link org.apache.solr.search.join.auxindexjoin.JoinIndexScorerSupplier.LazyConfirmationIterator} {@link org.apache.solr.search.join.auxindexjoin.JoinColumnDocWriter}
 *
 * </pre>
 *
 * <p>{@link org.apache.solr.search.join.auxindexjoin.AuxIndexManager} owns an auxiliary Lucene
 * index persisting, for every (from-segment, to-segment) pair, a SORTED_NUMERIC column mapping
 * from-side doc ids to to-side doc ids, plus edges columns with the pair's {@code {min, max}} doc
 * bounds and a to-side count. Pair columns are named by both sides' persistent side keys (segment
 * id + docvalues generation of the join field), so they survive reopens of either side and are
 * built lazily — the first {@code AuxIndexJoinQuery} weight that needs a pair writes it. See {@link
 * org.apache.solr.search.join.auxindexjoin.AuxIndexManager} for the user-facing API and {@link
 * org.apache.solr.search.join.auxindexjoin.AuxIndexJoinConfig} for tunables (blocking vs.
 * non-blocking refresh, one-field-per-segment, the reaper's sweep interval).
 *
 * <p>A batch of pair columns is written through {@code JoinColumnDocWriter}. It guarantes a batch
 * lands doc-for-doc in one sidecar segment, so doc 0's edges and every from-doc id line up the same
 * way.
 *
 * <h2>Garbage collection of dead pairs</h2>
 *
 * The sidecar is append-only. A side key dies when its segment is merged away, dropped, or its join
 * field receives an in-place docvalues update (dvGen bump); pair columns referencing a dead side
 * key can never be read again. {@code AuxIndexJoinMergePolicy} reaps them:
 *
 * <ul>
 *   <li><b>Death signal via sampling, not listeners.</b> {@code AuxIndexManager#onCreateWeight}
 *       (called from {@code AuxIndexJoinQuery#createWeight}) reports the set of pair field names a
 *       query actually needed to {@code AuxIndexJoinMergePolicy#onCreateWeight}, keyed by
 *       (from-directory, to-directory). A pair field name present in an earlier snapshot for the
 *       same searcher pair but missing from a later one is queued as a pending removal. Sampling is
 *       throttled ({@code AuxIndexJoinConfig#setSweepSamplingInterval}, default one minute) since
 *       it is only a heuristic hint, not a correctness requirement, and both the snapshot map and
 *       the pending-removal set are size-bounded (best-effort LRU-ish eviction).
 *   <li><b>Only a newer view may condemn.</b> The snapshot is keyed by directory, which is stable
 *       across reopens, so consecutive samples come from different queries that may hold different
 *       searcher generations. A sample taken from an older generation than the stored snapshot is
 *       discarded rather than acted on: it cannot see the segments opened since, and would report
 *       every one of them as dead. Readers with no orderable version are trusted as before.
 *   <li><b>Reaping.</b> {@code findMerges} reclaims dead pairs two ways, both piggybacked on
 *       ordinary merges — there is no background thread. A segment whose pair field names are
 *       <em>all</em> pending removals is dropped whole, via a {@code OneMerge} that reports it
 *       fully deleted ({@code wrapForMerge} returns a {@code MatchNoBits} live-docs view) so {@code
 *       IndexWriter} discards it instead of rewriting it. A segment only <em>partly</em> dead — the
 *       usual case once a segment carries hundreds of pairs — is rewritten by a {@code
 *       DocAlignedMerge} with the dead columns hidden from its {@code FieldInfos}, either folded
 *       into a compaction it was going to take part in anyway or, failing that, as a merge of one.
 *       Either way the names come off the queue only once the merge commits. {@code
 *       TestAuxIndexJoinMergePolicy} and {@code droppedSegmentCount()} / {@code
 *       purgedSegmentCount()} / {@code reapedPairCount()} cover this end to end.
 *   <li><b>Recoverable, not authoritative.</b> Reaping is a heuristic, so a query may still hold a
 *       reference to a column it drops. Such a pair is rebuilt by {@code
 *       JoinIndexScorerSupplier#refreshJoinTasksReferences} — including reading the from-side
 *       foreign-key column the weight skipped, precisely because the pair existed back then — and
 *       the cell is rebound to the rebuilt model mid-flight, which is safe because a cell's
 *       iteration state lives in its from-side iterator and never in the column. The {@code
 *       rebindsAfterReap} counter on the {@code evt=done} line reports how often this happens; it
 *       should be zero in a steady run.
 *   <li><b>Known gaps.</b> A pair field name is only queued for removal once a searcher pair that
 *       used to need it is sampled again <i>without</i> needing it — a side key that dies without
 *       ever being resampled this way stays live.
 * </ul>
 *
 * <h2>Compaction of the sidecar</h2>
 *
 * The sidecar gains a segment per written batch and, unlike an ordinary index, cannot merge them
 * the ordinary way: a sidecar doc id <em>is</em> the from-side doc id its column is addressed by,
 * so concatenating two segments would shift every mapping in the second one. Left alone the segment
 * count therefore grew without bound under a steady stream of pair builds, until the JVM ran out of
 * mmap-able address space. {@code AuxIndexJoinMergePolicy} folds groups of them into one with a
 * {@code DocAlignedMerge} instead, which unions their columns doc-for-doc -- doc {@code i} of the
 * result carries doc {@code i} of every input, and the result is as long as its longest input, not
 * as long as all of them together. Pair columns are opaque to it: their names already carry both
 * sides' segment ids, so no two inputs' columns can collide.
 *
 * <h2>TODO</h2>
 *
 * <ul>
 *   <li>comparative benchmarking for updates: parent, children field, PARENT_ID_FK ({@code
 *       AIJoinBenchmark} currently only compares against {@code JoinUtil} on a static index)
 *   <li>many-to-many: {@code JoinIndexUtils} doc mapping currently degrades M:N joins to M:1
 *       (single to-doc kept per from-doc)
 *   <li>reverse join on the same columns: children by parents filter
 *   <li>we estimate to&amp;from set by a range. Alternatives: union of ranges, roaring (bitset).
 *       Note: to and from estimates might be built different. For "to" side we need to provide
 *       advance()-eble iterator over union. And for "from" side it should be just intersectable
 *       with docSetIter. Format should balance storage size and decoding efforts. It should be
 *       stored as a Document fields.
 *   <li>let {@code DocAlignedMerge} drop the columns of pairs already queued for reaping while it
 *       rewrites a segment anyway: nearly free there, and the only way to reclaim a dead pair that
 *       shares a segment with live ones. Needs {@code
 *       JoinIndexScorerSupplier#refreshJoinTasksReferences} to rebuild a pair that vanished under a
 *       live query first, where it currently throws.
 *   <li>stripe columns for join index: break {@code JoinIndexUtils.TO_DOC_VAL_BY_FROM_DOCNUM} to a
 *       pair one is {@code to_doc_nums<maxdocs(to-side)/2} and {@code
 *       to_doc_nums>=maxdocs(to-side)/2}
 * </ul>
 *
 * @lucene.experimental
 *     <p>This is experimental API and subject to change.
 */
package org.apache.solr.search.join.auxindexjoin;

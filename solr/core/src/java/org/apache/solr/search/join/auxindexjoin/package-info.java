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
 * <h4>Simplified Collaboration Diagram</h4>
 *
 * <pre>
 * {@link AuxIndexJoinQParserPlugin}
 *       │
 *       ▼
 * {@link AuxIndexManager}  ----->  {@link AuxIndexJoinConfig}
 *       │    │
 *       │    +----------> {@link AuxIndexJoinMergePolicy}
 *       ▼
 * {@link AuxIndexJoinQuery} ------> {@link FromLeafJoinContext}
 *       │                           │
 *       ▼                           ▼
 * {@link JoinIndexWeight}             {@link ForeignKeyColumn}
 *       │
 *       ▼
 * {@link JoinIndexScorerSupplier} ---------> {@link JoinColumnIndexer}
 *   │ │                                       │ │
 *   │ ▼                                       │ ▼
 *   │{@link JoinIndexScorerSupplier.LeafJoin}           │{@link JoinIndexUtils.JoinColumnModel}
 *   ▼                                         ▼
 * {@link JoinIndexScorerSupplier.LazyConfimationIterator} {@link JoinColumnDocWriter}
 *
 * </pre>
 *
 * <p>{@link AuxIndexManager} owns an auxiliary Lucene index persisting, for every (from-segment,
 * to-segment) pair, a SORTED_NUMERIC column mapping from-side doc ids to to-side doc ids, plus
 * edges columns with the pair's {@code {min, max}} doc bounds and a to-side count. Pair columns are
 * named by both sides' persistent side keys (segment id + docvalues generation of the join field),
 * so they survive reopens of either side and are built lazily — the first {@code AuxIndexJoinQuery}
 * weight that needs a pair writes it. See {@link AuxIndexManager} for the user-facing API and
 * {@link AuxIndexJoinConfig} for tunables (blocking vs. non-blocking refresh,
 * one-field-per-segment, the reaper's sweep interval).
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
 *   <li><b>Reaping.</b> {@code findMerges} drops every sidecar segment whose pair field names are
 *       all pending removals, via a {@code OneMerge} that reports the segment as fully deleted
 *       ({@code wrapForMerge} returns a {@code MatchNoBits} live-docs view) so {@code IndexWriter}
 *       discards it instead of rewriting it. This runs piggybacked on ordinary merges — no
 *       background thread. {@code TestAIJoinMergePolicy} and {@code droppedSegmentCount()} / {@code
 *       pendingPairRemovalsCount()} cover this end to end.
 *   <li><b>Known gaps.</b> A pair field name is only queued for removal once a searcher pair that
 *       used to need it is sampled again <i>without</i> needing it — a side key that dies without
 *       ever being resampled this way stays live.
 * </ul>
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
 *   <li>approach refined terminology: outer side - slices, inner-side - bands, and a tile is an
 *       intersection
 * </ul>
 */
package org.apache.solr.search.join.auxindexjoin;

import org.apache.solr.search.join.AuxIndexJoinQParserPlugin;

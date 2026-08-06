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
package org.apache.solr.search.join.aijoin;

import java.io.IOException;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/**
 * Wall-clock comparison of {@link JoinUtil} (the term-based variant, no global ordinals) against
 * {@link AIJoinQuery} on a synthetic M:1 join: ~100K children in a few segments point to ~10K
 * unique parents in a few segments through single valued sorted string docvalues. A small
 * child-side term filter selects ~10K children that join to ~1K parents; a second case adds a
 * parent-side filter on top of the join. Results are not asserted, only timed: each search repeats
 * {@link #PASSES} times and min/max/avg wall times are printed.
 *
 * <p>The whole comparison runs {@link #ROUNDS} times over a growing, mutating index: between rounds
 * the parent id boundary is raised by 10% and a tenth of the (new) parent population, with all
 * their children, is rewritten through {@code updateDocument} at random ids below the new boundary
 * — ids above the old boundary append fresh docs while ids below it clash with existing ones and
 * replace them, leaving deletes behind. The {@link AIJoinIndex} is opened once and reused by every
 * round and pass: after each update the first search builds only the missing pair columns, which is
 * timed separately from the steady-state passes.
 *
 * <p>Plain Java benchmark, run directly with {@code main}, no test framework involved.
 */
public class AIJoinBenchmark {

  private static final String PARENT_ID = "parent_id";
  private static final String PARENT_ID_FK = "parent_id_FK";
  private static final String CHILD_ID = "child_id";
  private static final String TAG = "tag";
  private static final String COLOR = "color";

  private static final String HOT = "hot";
  private static final String COLD = "cold";
  // the parent filter matches one of COLOR_CARDINALITY colors, passing ~1/30 of the joined parents
  private static final int COLOR_CARDINALITY = 30;

  private static final int NUM_PARENTS = 100_000;
  private static final int CHILDREN_PER_PARENT = 10;
  // every HOT_STRIDE'th parent is "hot" and all its children carry the hot tag: the child filter
  // TermQuery(tag:hot) matches NUM_PARENTS / HOT_STRIDE * CHILDREN_PER_PARENT ~ 10K children
  // joining to NUM_PARENTS / HOT_STRIDE ~ 1K parents, scattered over all segments
  private static final int HOT_STRIDE = 10;
  private static final int PARENT_SEGMENTS = 4;
  private static final int CHILD_SEGMENTS = 5;
  private static final int PASSES = 10;
  // benchmark rounds: between rounds the parent boundary grows by GROWTH_DENOMINATOR'th and
  // 1/UPDATE_DENOMINATOR of the grown population is rewritten at random ids below the new boundary
  private static final int ROUNDS = 10;
  private static final int GROWTH_DENOMINATOR = 10;
  private static final int UPDATE_DENOMINATOR = 10;

  private final Random random = new Random();

  private interface SearchTask {
    TopDocs run() throws IOException;
  }

  public static void main(String[] args) throws Exception {
    new AIJoinBenchmark().runBenchmark();
  }

  public void runBenchmark() throws Exception {
    // the auxiliary join index is opened once and reused by every round and pass
    try (Directory parentsDir = new ByteBuffersDirectory();
        Directory childrenDir = new ByteBuffersDirectory();
        Directory joinDir = new ByteBuffersDirectory();
        ExecutorService executor =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        AIJoinIndex joinIndex = new AIJoinIndex(joinDir)) {
      buildIndices(parentsDir, childrenDir);
      int numParents = NUM_PARENTS;
      for (int round = 0; round < ROUNDS; round++) {
        benchmarkRound(round, numParents, parentsDir, childrenDir, joinIndex, executor);
        if (round < ROUNDS - 1) {
          numParents += numParents / GROWTH_DENOMINATOR;
          updateIndices(parentsDir, childrenDir, numParents);
        }
      }
    }
  }

  private void benchmarkRound(
      int round,
      int numParents,
      Directory parentsDir,
      Directory childrenDir,
      AIJoinIndex joinIndex,
      ExecutorService executor)
      throws IOException {
    try (IndexReader parentsReader = DirectoryReader.open(parentsDir);
        IndexReader childrenReader = DirectoryReader.open(childrenDir)) {
      System.out.printf(
          Locale.ROOT,
          "round %d: parent ids < %d, parents: %d/%d docs (live/max) / %d segments,"
              + " children: %d/%d docs (live/max) / %d segments%n",
          round,
          numParents,
          parentsReader.numDocs(),
          parentsReader.maxDoc(),
          parentsReader.leaves().size(),
          childrenReader.numDocs(),
          childrenReader.maxDoc(),
          childrenReader.leaves().size());

      // plain searchers without caching: AIJoinQuery is uncacheable anyway, so a cached
      // JoinUtil query would make the comparison lopsided
      IndexSearcher childrenSearcher = new ParallelIndexSearcher(childrenReader, executor);
      childrenSearcher.setQueryCache(null);
      IndexSearcher parentsSearcher = new ParallelIndexSearcher(parentsReader, executor);
      parentsSearcher.setQueryCache(null);

      Query childFilter = new TermQuery(new Term(TAG, HOT));
      Query parentFilter = new TermQuery(new Term(COLOR, color(0)));

      // the first search after a reopen builds the missing pair columns lazily (all of them in
      // round 0, only the new segments' pairs later), so it's timed apart from the steady-state
      // passes
      long buildStart = System.nanoTime();
      exactSearch(
          parentsSearcher, aiJoinChildrenToParents(joinIndex, childFilter, childrenSearcher));
      System.out.printf(
          Locale.ROOT,
          "AI join first search (lazy pair build): %.2fms%n",
          (System.nanoTime() - buildStart) / 1_000_000d);

      // join query creation is inside the timed task on purpose: JoinUtil runs the from-side
      // selection eagerly in createJoinQuery, AIJoinQuery lazily in createWeight, so only
      // create+search is comparable
      bench(
          "JoinUtil",
          () -> exactSearch(parentsSearcher, joinChildrenToParents(childFilter, childrenSearcher)));
      bench(
          "AIJoin",
          () ->
              exactSearch(
                  parentsSearcher,
                  aiJoinChildrenToParents(joinIndex, childFilter, childrenSearcher)));
      bench(
          "JoinUtil + parent filter",
          () ->
              exactSearch(
                  parentsSearcher,
                  filterParents(
                      joinChildrenToParents(childFilter, childrenSearcher), parentFilter)));
      bench(
          "AIJoin + parent filter",
          () ->
              exactSearch(
                  parentsSearcher,
                  filterParents(
                      aiJoinChildrenToParents(joinIndex, childFilter, childrenSearcher),
                      parentFilter)));
    }
  }

  private static Query joinChildrenToParents(Query childFilter, IndexSearcher childrenSearcher)
      throws IOException {
    // multipleValuesPerDocument=false picks the term-based join, no global ordinals involved
    return JoinUtil.createJoinQuery(
        PARENT_ID_FK, false, PARENT_ID, childFilter, childrenSearcher, ScoreMode.None);
  }

  private static final ExecutorService executor = Executors.newFixedThreadPool(4);

  private static Query aiJoinChildrenToParents(
      AIJoinIndex joinIndex, Query childFilter, IndexSearcher childrenSearcher) {
    return joinIndex.newJoinQuery(PARENT_ID_FK, childFilter, childrenSearcher, PARENT_ID, executor);
  }

  private static String color(int index) {
    return "color" + index;
  }

  private static Query filterParents(Query joinQuery, Query parentFilter) {
    return new BooleanQuery.Builder()
        .add(joinQuery, BooleanClause.Occur.MUST)
        .add(parentFilter, BooleanClause.Occur.FILTER)
        .build();
  }

  /**
   * Runs a top-10 search with an uncapped total hits threshold, so {@code totalHits} is always
   * exact instead of the default two-arg {@code search(query, 10)}'s early-terminated estimate past
   * 1000 hits: with that default, JoinUtil and AIJoin visit docs in different orders and cost
   * estimates and stop at different points, making their reported hit counts incomparable.
   */
  private static TopDocs exactSearch(IndexSearcher searcher, Query query) throws IOException {
    return searcher.search(query, new TopScoreDocCollectorManager(10, null, Integer.MAX_VALUE));
  }

  private static void bench(String name, SearchTask task) throws IOException {
    long minNs = Long.MAX_VALUE;
    long maxNs = 0;
    long totalNs = 0;
    long hits = -1;
    for (int pass = 0; pass < PASSES; pass++) {
      long start = System.nanoTime();
      TopDocs topDocs = task.run();
      long elapsed = System.nanoTime() - start;
      minNs = Math.min(minNs, elapsed);
      maxNs = Math.max(maxNs, elapsed);
      totalNs += elapsed;
      hits = topDocs.totalHits.value();
    }
    System.out.printf(
        Locale.ROOT,
        "%-26s min=%8.2fms max=%8.2fms avg=%8.2fms hits=%d%n",
        name,
        minNs / 1_000_000d,
        maxNs / 1_000_000d,
        (double) totalNs / PASSES / 1_000_000d,
        hits);
  }

  /**
   * Writes the two sides with plain {@link IndexWriter}s: every child points to exactly one parent,
   * {@code parent_id} is unique on the parents side, {@code child_id} is unique on the children
   * side, both join fields are single valued sorted string docvalues, and periodic commits under
   * {@link NoMergePolicy} leave each side in a few segments.
   */
  private void buildIndices(Directory parentsDir, Directory childrenDir) throws IOException {
    try (IndexWriter parentsWriter = newBenchWriter(parentsDir);
        IndexWriter childrenWriter = newBenchWriter(childrenDir)) {
      int parentsPerSegment = NUM_PARENTS / PARENT_SEGMENTS;
      int childrenPerSegment = NUM_PARENTS * CHILDREN_PER_PARENT / CHILD_SEGMENTS;
      int childSeq = 0;
      for (int p = 0; p < NUM_PARENTS; p++) {
        parentsWriter.addDocument(parentDoc(p));
        if ((p + 1) % parentsPerSegment == 0) {
          parentsWriter.commit();
        }
        for (int c = 0; c < CHILDREN_PER_PARENT; c++) {
          childrenWriter.addDocument(childDoc(p, c));
          if (++childSeq % childrenPerSegment == 0) {
            childrenWriter.commit();
          }
        }
      }
      parentsWriter.commit();
      childrenWriter.commit();
    }
  }

  /**
   * Simulates incremental growth after the boundary was raised to {@code numParents}: rewrites
   * {@code numParents / }{@link #UPDATE_DENOMINATOR} parents, each with all its children, at random
   * ids below the new boundary. Ids above the previous boundary append fresh docs; ids below it
   * clash with existing {@code parent_id}/{@code child_id} terms and {@code updateDocument}
   * replaces the old docs, leaving deletes behind in the earlier segments. The single closing
   * commit adds one new segment per side under {@link NoMergePolicy}.
   */
  private void updateIndices(Directory parentsDir, Directory childrenDir, int numParents)
      throws IOException {
    try (IndexWriter parentsWriter = newBenchWriter(parentsDir);
        IndexWriter childrenWriter = newBenchWriter(childrenDir)) {
      for (int i = 0; i < numParents / UPDATE_DENOMINATOR; i++) {
        int p = random.nextInt(numParents);
        parentsWriter.updateDocument(new Term(PARENT_ID, parentId(p)), parentDoc(p));
        for (int c = 0; c < CHILDREN_PER_PARENT; c++) {
          childrenWriter.updateDocument(new Term(CHILD_ID, childId(p, c)), childDoc(p, c));
        }
      }
      parentsWriter.commit();
      childrenWriter.commit();
    }
  }

  private IndexWriter newBenchWriter(Directory dir) throws IOException {
    return new IndexWriter(
        dir,
        new IndexWriterConfig()
            .setMergePolicy(NoMergePolicy.INSTANCE)
            // pin the flush triggers so the intended segment layout isn't shattered by tiny
            // default buffers
            .setMaxBufferedDocs(IndexWriter.MAX_DOCS)
            .setRAMBufferSizeMB(256));
  }

  private static String parentId(int p) {
    return "parent" + p;
  }

  private static String childId(int p, int c) {
    return "child" + p + "." + c;
  }

  private static Document parentDoc(int p) {
    String parentId = parentId(p);
    Document doc = new Document();
    doc.add(new StringField(PARENT_ID, parentId, Field.Store.NO));
    doc.add(new SortedDocValuesField(PARENT_ID, new BytesRef(parentId)));
    // cycle colors by hot rank, not by p: a plain p % COLOR_CARDINALITY would correlate with
    // the p % HOT_STRIDE hot selection and the filter wouldn't thin the joined parents
    doc.add(new StringField(COLOR, color((p / HOT_STRIDE) % COLOR_CARDINALITY), Field.Store.NO));
    return doc;
  }

  private static Document childDoc(int p, int c) {
    Document doc = new Document();
    doc.add(new StringField(CHILD_ID, childId(p, c), Field.Store.NO));
    doc.add(new SortedDocValuesField(PARENT_ID_FK, new BytesRef(parentId(p))));
    doc.add(new StringField(TAG, p % HOT_STRIDE == 0 ? HOT : COLD, Field.Store.NO));
    return doc;
  }
}

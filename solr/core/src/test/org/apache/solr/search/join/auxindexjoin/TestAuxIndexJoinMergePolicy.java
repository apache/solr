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
package org.apache.solr.search.join.auxindexjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCase;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.Edges;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.JoinColumnModel;

/**
 * Real (non-mocked) integration smoke test for {@link AuxIndexJoinMergePolicy}: builds a segmented
 * children/parents pair, forces {@link AuxIndexJoinQuery} to populate the sidecar join index, then
 * mass deletes and force-merges both sides so the sidecar's previously-built pair columns go stale,
 * and checks the policy actually notices and reaps them -- not just that it runs without throwing.
 */
public class TestAuxIndexJoinMergePolicy extends SolrTestCase {

  private static final String ID = "id";
  private static final String PARENT_ID = "parent_id";
  private static final String PARENT_ID_FK = "parent_id_FK";

  private Directory parentsDir;
  private Directory childrenDir;
  private Directory joinDir;
  private RandomIndexWriter parentsWriter;
  private RandomIndexWriter childrenWriter;
  private AuxIndexManager joinIndex;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    parentsDir = newDirectory();
    childrenDir = newDirectory();
    // NoMergePolicy keeps every intermediate commit as its own segment, so the join index has
    // several distinct (from-segment, to-segment) pairs to build, not just one
    parentsWriter =
        new RandomIndexWriter(
            random(),
            parentsDir,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setMergePolicy(NoMergePolicy.INSTANCE));
    childrenWriter =
        new RandomIndexWriter(
            random(),
            childrenDir,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setMergePolicy(NoMergePolicy.INSTANCE));
    joinDir = newDirectory();
    joinIndex = new AuxIndexManager(joinDir);
    // this test drives many onCreateWeight calls back to back, well inside the default one-minute
    // sampling interval, and asserts on the reaper noticing every one of them
    joinIndex.mergePolicy.setSweepInterval(0, TimeUnit.NANOSECONDS);
  }

  @Override
  public void tearDown() throws Exception {
    IOUtils.close(joinIndex, joinDir, parentsWriter, childrenWriter, parentsDir, childrenDir);
    super.tearDown();
  }

  private static Document parentDoc(String parentId) {
    Document doc = new Document();
    doc.add(new StringField(PARENT_ID, parentId, Field.Store.YES));
    doc.add(new SortedSetDocValuesField(PARENT_ID, new BytesRef(parentId)));
    return doc;
  }

  private static Document childDoc(String childId, String parentId) {
    Document doc = new Document();
    doc.add(new StringField(ID, childId, Field.Store.YES));
    doc.add(new StringField(PARENT_ID_FK, parentId, Field.Store.YES));
    doc.add(new SortedSetDocValuesField(PARENT_ID_FK, new BytesRef(parentId)));
    return doc;
  }

  /**
   * Adds {@code numParents} parents (3 children each), committing every 3 parents so both sides end
   * up segmented rather than a single flush. Returns the added parent ids.
   */
  private List<String> addParentsAndChildren(String parentIdPrefix, int numParents)
      throws IOException {
    List<String> parentIds = new ArrayList<>();
    for (int p = 0; p < numParents; p++) {
      String parentId = parentIdPrefix + p;
      parentIds.add(parentId);
      parentsWriter.addDocument(parentDoc(parentId));
      for (int c = 0; c < 3; c++) {
        childrenWriter.addDocument(childDoc(parentId + "_child" + c, parentId));
      }
      if (p % 3 == 2) {
        parentsWriter.commit();
        childrenWriter.commit();
      }
    }
    parentsWriter.commit();
    childrenWriter.commit();
    return parentIds;
  }

  /**
   * Deletes every parent in {@code parentIds} (and its children), then force-merges both sides down
   * to a single segment each -- changing both sides' segment identities so every sidecar pair
   * column referencing them goes stale.
   */
  private void deleteParentsAndForceMerge(List<String> parentIds) throws IOException {
    for (String parentId : parentIds) {
      parentsWriter.deleteDocuments(new Term(PARENT_ID, parentId));
      for (int c = 0; c < 3; c++) {
        childrenWriter.deleteDocuments(new Term(ID, parentId + "_child" + c));
      }
    }
    // NoMergePolicy (set at construction, to guarantee segmentation above) blocks forceMerge too,
    // so swap in a real policy just for these force merges
    parentsWriter.w.getConfig().setMergePolicy(new TieredMergePolicy());
    childrenWriter.w.getConfig().setMergePolicy(new TieredMergePolicy());
    parentsWriter.forceMerge(1);
    childrenWriter.forceMerge(1);
    parentsWriter.commit();
    childrenWriter.commit();
  }

  /**
   * Runs the join query for every child against every parent and returns the matched parent ids,
   * forcing {@link AuxIndexJoinQuery} to fully execute (not just build its {@link
   * org.apache.lucene.search.Weight}) so missing pair columns actually get built into the sidecar.
   */
  private Set<String> searchAllParents(
      IndexSearcher parentsSearcher, IndexSearcher childrenSearcher) throws IOException {
    Query aiJoinQuery =
        joinIndex.newJoinQuery(PARENT_ID_FK, new MatchAllDocsQuery(), childrenSearcher, PARENT_ID);
    TopDocs topDocs =
        parentsSearcher.search(aiJoinQuery, parentsSearcher.getIndexReader().maxDoc());
    Set<String> parentIds = new TreeSet<>();
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      parentIds.add(parentsSearcher.storedFields().document(scoreDoc.doc).get(PARENT_ID));
    }
    return parentIds;
  }

  public void testDeadPairsAreReapedAfterForceMerge() throws Exception {
    List<String> firstBatch = addParentsAndChildren("gen1-", atLeast(15));

    try (IndexReader childrenReader = childrenWriter.getReader();
        IndexReader parentsReader = parentsWriter.getReader()) {
      assertTrue(
          "children index should be segmented before merging", childrenReader.leaves().size() > 1);
      assertTrue(
          "parents index should be segmented before merging", parentsReader.leaves().size() > 1);

      // populates the sidecar with every (child-segment, parent-segment) pair currently needed
      assertEquals(
          new TreeSet<>(firstBatch),
          searchAllParents(newSearcher(parentsReader), newSearcher(childrenReader)));
    }

    assertEquals(
        "nothing should look dead yet, nothing has disappeared",
        0,
        joinIndex.mergePolicy.pendingPairRemovalsCount());
    assertEquals(0, joinIndex.mergePolicy.droppedSegmentCount());

    // mass removal: drop every other parent (and its children), then force-merge both sides --
    // every pair column built above referenced a from/to segment that no longer exists
    List<String> deadFromFirstBatch = new ArrayList<>();
    List<String> survivingParents = new ArrayList<>();
    for (int i = 0; i < firstBatch.size(); i++) {
      (i % 2 == 0 ? deadFromFirstBatch : survivingParents).add(firstBatch.get(i));
    }
    deleteParentsAndForceMerge(deadFromFirstBatch);

    try (IndexReader childrenReader = childrenWriter.getReader();
        IndexReader parentsReader = parentsWriter.getReader()) {
      assertEquals(1, childrenReader.leaves().size());
      assertEquals(1, parentsReader.leaves().size());

      // re-running the same join rebuilds pairs against the new segments and, in the process,
      // notices the old pairs vanished
      assertEquals(
          new TreeSet<>(survivingParents),
          searchAllParents(newSearcher(parentsReader), newSearcher(childrenReader)));
    }

    joinIndex.waitForMerges();

    assertTrue(
        "the old pair columns should have been recognized as dead",
        joinIndex.mergePolicy.pendingPairRemovalsCount() > 0);
    assertTrue(
        "AuxIndexJoinMergePolicy should have reaped at least the one dead sidecar segment",
        joinIndex.mergePolicy.droppedSegmentCount() >= 1);

    // do it again: grow, mass-delete, force-merge -- the reaper should keep working, not just
    // fire once. Force-merging an index that already sits at a single, fully-live segment is a
    // no-op (nothing to reclaim), so the surviving segment above keeps its identity and its
    // sidecar pair never goes stale on its own; adding one more small batch afterwards guarantees
    // a genuinely new pair needs building, which is what actually triggers the sidecar's next
    // commit (and so the next reap opportunity) -- the reaper is piggybacked on writes, not on a
    // background timer.
    int droppedSoFar = joinIndex.mergePolicy.droppedSegmentCount();
    List<String> secondBatch = addParentsAndChildren("gen2-", atLeast(15));
    try (IndexReader childrenReader = childrenWriter.getReader();
        IndexReader parentsReader = parentsWriter.getReader()) {
      Set<String> expected = new TreeSet<>(survivingParents);
      expected.addAll(secondBatch);
      assertEquals(
          expected, searchAllParents(newSearcher(parentsReader), newSearcher(childrenReader)));
    }

    deleteParentsAndForceMerge(secondBatch);
    List<String> thirdBatch = addParentsAndChildren("gen3-", 1);

    try (IndexReader childrenReader = childrenWriter.getReader();
        IndexReader parentsReader = parentsWriter.getReader()) {
      Set<String> expected = new TreeSet<>(survivingParents);
      expected.addAll(thirdBatch);
      assertEquals(
          expected, searchAllParents(newSearcher(parentsReader), newSearcher(childrenReader)));
    }

    joinIndex.waitForMerges();

    assertTrue(
        "a second round of mass deletes + force merge should reap more dead segments",
        joinIndex.mergePolicy.droppedSegmentCount() > droppedSoFar);
  }

  /**
   * Writes {@code batches} single-pair batches straight into the sidecar -- one segment each, of
   * deliberately differing lengths -- and returns the from-doc -> to-doc column each pair was
   * written with, keyed by pair field name.
   */
  private Map<String, int[]> writeBatchesOfVaryingLength(int firstBatch, int batches)
      throws IOException {
    Map<String, int[]> columnsByPair = new LinkedHashMap<>();
    for (int batch = firstBatch; batch < firstBatch + batches; batch++) {
      // the shape of a real pair field name doesn't matter here, only that it is unique per pair
      String pairFieldName = "fromSeg" + batch + ":dv0_toSeg:dv0";
      int fromSegmentMaxDoc = 2 + batch; // pairs come from from-segments of different sizes
      int[] toDocByFromDoc = new int[fromSegmentMaxDoc];
      Arrays.fill(toDocByFromDoc, -1);
      // every other from-doc matches, starting at doc 0 for even batches and doc 1 for odd ones,
      // so the union has to keep both "doc 0 has a value" and "doc 0 has none" columns straight
      for (int fromDoc = batch % 2; fromDoc < fromSegmentMaxDoc; fromDoc += 2) {
        toDocByFromDoc[fromDoc] = 1000 * batch + fromDoc;
      }
      columnsByPair.put(pairFieldName, toDocByFromDoc);
      joinIndex.writeBatch(Map.of(pairFieldName, model(toDocByFromDoc)));
    }
    return columnsByPair;
  }

  /** The {@link JoinColumnModel} a from-doc -> to-doc array implies, edges included. */
  private static JoinColumnModel model(int[] toDocByFromDoc) {
    int minFromDoc = -1;
    int maxFromDoc = -1;
    int minToDoc = -1;
    int maxToDoc = -1;
    int toCount = 0;
    for (int fromDoc = 0; fromDoc < toDocByFromDoc.length; fromDoc++) {
      int toDoc = toDocByFromDoc[fromDoc];
      if (toDoc < 0) {
        continue;
      }
      if (toCount == 0) {
        minFromDoc = fromDoc;
        minToDoc = toDoc;
      }
      maxFromDoc = fromDoc;
      minToDoc = Math.min(minToDoc, toDoc);
      maxToDoc = Math.max(maxToDoc, toDoc);
      toCount++;
    }
    return new JoinColumnModel(
        toDocByFromDoc,
        new Edges(new int[] {minFromDoc, maxFromDoc}, new int[] {minToDoc, maxToDoc}, toCount));
  }

  /**
   * Asserts the pair's column reads back from the sidecar exactly as written: same to-doc at the
   * same from-doc, nothing where nothing was written (padding included), and its edges still at doc
   * 0 -- i.e. that compaction moved no doc id.
   */
  private static void assertColumnIntact(IndexReader sidecar, String pairFieldName, int[] expected)
      throws IOException {
    String toCountField = JoinIndexUtils.TO_COUNT_PREFIX + pairFieldName;
    LeafReaderContext carrier = null;
    for (LeafReaderContext leaf : sidecar.leaves()) {
      if (leaf.reader().getFieldInfos().fieldInfo(toCountField) != null) {
        assertNull("pair " + pairFieldName + " ended up in two sidecar segments", carrier);
        carrier = leaf;
      }
    }
    assertNotNull("pair " + pairFieldName + " was lost", carrier);

    JoinColumnModel written = model(expected);
    assertArrayEquals(
        "from-doc edges of " + pairFieldName,
        written.edges().fromDocEdges(),
        JoinIndexUtils.loadEdges(carrier, JoinIndexUtils.FROM_EDGES_PREFIX + pairFieldName));
    assertArrayEquals(
        "to-doc edges of " + pairFieldName,
        written.edges().toDocEdges(),
        JoinIndexUtils.loadEdges(carrier, JoinIndexUtils.TO_EDGES_PREFIX + pairFieldName));
    assertArrayEquals(
        "to-doc count of " + pairFieldName,
        new int[] {written.edges().toCount()},
        JoinIndexUtils.loadEdges(carrier, toCountField));

    SortedNumericDocValues column =
        carrier
            .reader()
            .getSortedNumericDocValues(JoinIndexUtils.TO_DOC_VAL_BY_FROM_DOCNUM + pairFieldName);
    assertNotNull("join column of " + pairFieldName, column);
    for (int fromDoc = 0; fromDoc < expected.length; fromDoc++) {
      String at = pairFieldName + " at from-doc " + fromDoc;
      assertEquals(at, expected[fromDoc] >= 0, column.advanceExact(fromDoc));
      if (expected[fromDoc] >= 0) {
        assertEquals(at, 1, column.docValueCount());
        assertEquals(at, expected[fromDoc], column.nextValue());
      }
    }
    // the tail this pair's from-segment never had: padded by the merge, and still empty
    for (int paddedDoc = expected.length; paddedDoc < carrier.reader().maxDoc(); paddedDoc++) {
      assertFalse(
          pairFieldName + " has a value at padded doc " + paddedDoc,
          column.advanceExact(paddedDoc));
    }
  }

  /**
   * The sidecar is written one batch per segment and can never be merged the ordinary way, so
   * compaction has to fold segments together by doc id. Checks it happens, and that every column
   * still answers at exactly the from-doc it was written at.
   */
  public void testDocAlignedMergeUnionsColumnsWithoutMovingDocIds() throws Exception {
    // fold every 3 segments, so a handful of batches is enough to see compaction converge
    joinIndex.mergePolicy.setCompaction(3, AuxIndexJoinMergePolicy.DEFAULT_MAX_PAIRS_PER_SEGMENT);

    Map<String, int[]> columnsByPair = writeBatchesOfVaryingLength(0, 9);
    joinIndex.waitForMerges();
    // one more batch, so the commit it makes publishes the merged segments to the reader below
    columnsByPair.putAll(writeBatchesOfVaryingLength(9, 1));

    assertTrue(
        "expected the sidecar's segments to be compacted, none were",
        joinIndex.mergePolicy.alignedMergeCount() > 0);

    try (DirectoryReader sidecar = DirectoryReader.open(joinDir)) {
      assertTrue(
          "compaction should have left fewer segments than the "
              + columnsByPair.size()
              + " written batches, got "
              + sidecar.leaves().size(),
          sidecar.leaves().size() < columnsByPair.size());
      for (Map.Entry<String, int[]> pair : columnsByPair.entrySet()) {
        assertColumnIntact(sidecar, pair.getKey(), pair.getValue());
      }
    }
  }

  /** The same, end to end: a join keeps answering identically once its sidecar is compacted. */
  public void testJoinAnswersTheSameAfterCompaction() throws Exception {
    joinIndex.mergePolicy.setCompaction(2, AuxIndexJoinMergePolicy.DEFAULT_MAX_PAIRS_PER_SEGMENT);

    List<String> parentIds = addParentsAndChildren("gen1-", atLeast(15));
    try (IndexReader childrenReader = childrenWriter.getReader();
        IndexReader parentsReader = parentsWriter.getReader()) {
      // builds the sidecar: one segment per written batch, several pairs' worth
      assertEquals(
          new TreeSet<>(parentIds),
          searchAllParents(newSearcher(parentsReader), newSearcher(childrenReader)));

      joinIndex.waitForMerges();
      assertTrue(
          "expected the sidecar's segments to be compacted, none were",
          joinIndex.mergePolicy.alignedMergeCount() > 0);

      // same query, same answer -- now served from doc-aligned, compacted sidecar segments
      assertEquals(
          new TreeSet<>(parentIds),
          searchAllParents(newSearcher(parentsReader), newSearcher(childrenReader)));
    }
  }

  /**
   * The failure mode compaction exists for: under a steady stream of pair builds the sidecar used
   * to grow by one segment per batch forever -- hundreds of segments, each a compound file the
   * merge policy reopens on every commit, until the JVM runs out of mmap-able address space ("Map
   * failed" out of MMapDirectory). With compaction the segment count stays bounded instead.
   */
  public void testSegmentCountStaysBoundedUnderSteadyWrites() throws Exception {
    int batches = 60; // with the default fold of 10, one per commit, as a steady load would
    writeBatchesOfVaryingLength(0, batches);
    joinIndex.waitForMerges();
    writeBatchesOfVaryingLength(batches, 1); // a commit publishing whatever just merged
    joinIndex.waitForMerges();

    try (DirectoryReader sidecar = DirectoryReader.open(joinDir)) {
      assertTrue(
          "expected compactions to keep the sidecar's segment count well under the "
              + batches
              + " batches written, got "
              + sidecar.leaves().size()
              + " segments after "
              + joinIndex.mergePolicy.alignedMergeCount()
              + " compactions",
          sidecar.leaves().size() <= batches / 3);
    }
  }
}

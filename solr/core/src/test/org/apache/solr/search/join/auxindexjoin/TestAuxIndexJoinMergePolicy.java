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
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
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
    // the sidecars here are kilobytes, so the default megabyte of reclaimable bytes would decline
    // every purge; the two tests below that are about the threshold itself set their own
    joinIndex.mergePolicy.setMinReclaimableBytesToPurge(0);
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

    // reapedPairCount, not pendingPairRemovalsCount: the queue is what is still *owed*, and now
    // that reaping actually drains it, it settles back to zero. What has to be true is that the
    // dead pairs were recognized and then reclaimed, which is what this counts.
    assertTrue(
        "the old pair columns should have been recognized as dead and reaped",
        joinIndex.mergePolicy.reapedPairCount() > 0);
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
    int reapedSoFar = joinIndex.mergePolicy.reapedPairCount();
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

    // more dead *pairs* reclaimed, not necessarily more dead *segments*: which of the two routes
    // reaping takes depends on whether a segment's pairs all die together, and with column purging
    // available a segment often sheds its dead columns before it can become entirely dead
    assertTrue(
        "a second round of mass deletes + force merge should reclaim more dead pairs; dropped "
            + droppedSoFar
            + "->"
            + joinIndex.mergePolicy.droppedSegmentCount()
            + " purged "
            + joinIndex.mergePolicy.purgedSegmentCount()
            + " reaped pairs "
            + reapedSoFar
            + "->"
            + joinIndex.mergePolicy.reapedPairCount(),
        joinIndex.mergePolicy.reapedPairCount() > reapedSoFar);
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
   * Writes one batch carrying {@code pairCount} pairs -- so one sidecar segment {@code pairCount}
   * columns wide, which is what the purge threshold measures against. Returns its columns by pair
   * name.
   */
  private Map<String, int[]> writeWideBatch(int batch, int pairCount) throws IOException {
    Map<String, int[]> columnsByPair = new LinkedHashMap<>();
    Map<String, JoinColumnModel> written = new LinkedHashMap<>();
    for (int slot = 0; slot < pairCount; slot++) {
      String pairFieldName = "fromSeg" + batch + "-" + slot + ":dv0_toSeg:dv0";
      int[] toDocByFromDoc = new int[2 + batch];
      Arrays.fill(toDocByFromDoc, -1);
      for (int fromDoc = slot % 2; fromDoc < toDocByFromDoc.length; fromDoc += 2) {
        toDocByFromDoc[fromDoc] = 1000 * batch + fromDoc;
      }
      columnsByPair.put(pairFieldName, toDocByFromDoc);
      written.put(pairFieldName, model(toDocByFromDoc));
    }
    joinIndex.writeBatch(written);
    return columnsByPair;
  }

  /** Every sidecar field belonging to {@code pairFieldName}, across all segments. */
  private static List<String> fieldsOf(IndexReader sidecar, String pairFieldName) {
    List<String> found = new ArrayList<>();
    for (LeafReaderContext leaf : sidecar.leaves()) {
      for (org.apache.lucene.index.FieldInfo fieldInfo : leaf.reader().getFieldInfos()) {
        if (fieldInfo.name.endsWith(pairFieldName)) {
          found.add(leaf.reader().toString() + ":" + fieldInfo.name);
        }
      }
    }
    return found;
  }

  /**
   * A queued pair's columns have to actually leave the index, and its name has to leave the queue.
   * Neither used to happen: reaping could only drop a segment all of whose pairs were dead at once,
   * which a segment carrying many pairs never is, so {@code pendingPairRemovals} only ever grew --
   * every entry pinning a column that could never be read again, until the heap ran out.
   */
  public void testQueuedPairsAreReapedAndDrained() throws Exception {
    joinIndex.mergePolicy.setCompaction(3, AuxIndexJoinMergePolicy.DEFAULT_MAX_PAIRS_PER_SEGMENT);

    Map<String, int[]> columnsByPair = writeBatchesOfVaryingLength(0, 6);
    List<String> allPairs = new ArrayList<>(columnsByPair.keySet());
    List<String> doomed = allPairs.subList(0, 2);
    for (String pairFieldName : doomed) {
      joinIndex.mergePolicy.queueForRemoval(pairFieldName);
    }
    assertEquals(doomed.size(), joinIndex.mergePolicy.pendingPairRemovalsCount());

    // a write is what gives the policy its next look at the index -- reaping rides on merges, it
    // has no timer of its own
    columnsByPair.putAll(writeBatchesOfVaryingLength(6, 1));
    joinIndex.waitForMerges();
    columnsByPair.putAll(writeBatchesOfVaryingLength(7, 1)); // publishes what just merged

    assertEquals(
        "every queued pair should have been reaped, so nothing should still be pending",
        0,
        joinIndex.mergePolicy.pendingPairRemovalsCount());

    try (DirectoryReader sidecar = DirectoryReader.open(joinDir)) {
      for (String pairFieldName : doomed) {
        assertEquals(
            "reaped pair " + pairFieldName + " still has columns in the sidecar",
            List.of(),
            fieldsOf(sidecar, pairFieldName));
      }
      // and the merge that dropped them left every other column exactly as written
      for (Map.Entry<String, int[]> pair : columnsByPair.entrySet()) {
        if (!doomed.contains(pair.getKey())) {
          assertColumnIntact(sidecar, pair.getKey(), pair.getValue());
        }
      }
    }
  }

  /**
   * Compaction cannot be relied on to carry reaping: it needs a quorum of eligible segments, and
   * {@code isCompactable} passes over exactly the segments most worth purging -- the ones already
   * wider than {@code maxPairsPerSegment}. Pinned here by making every segment ineligible: only the
   * single-segment purge path can reclaim anything.
   */
  public void testPairsAreReapedEvenWhenCompactionSkipsEverySegment() throws Exception {
    // maxPairsPerSegment=0 makes isCompactable false for every segment carrying a pair, and the
    // fold size is beyond anything this test writes, so no DocAlignedMerge can be proposed for
    // compaction's own sake
    joinIndex.mergePolicy.setCompaction(1000, 0);

    // two pairs per segment, and only one of them dies: a segment that is merely *partly* dead is
    // the case the whole-segment drop cannot handle, and the one a real sidecar is always in
    Map<String, int[]> columnsByPair = new LinkedHashMap<>();
    String doomed = null;
    for (int batch = 0; batch < 4; batch++) {
      Map<String, int[]> written = writeWideBatch(batch, 2);
      columnsByPair.putAll(written);
      if (batch == 0) {
        doomed = written.keySet().iterator().next(); // one pair of a two-pair segment
      }
    }
    // 1 of 2 is well over the purge threshold
    joinIndex.mergePolicy.queueForRemoval(doomed);

    // a write to give the policy its next look, then one more to publish what merged
    columnsByPair.putAll(writeBatchesOfVaryingLength(100, 1));
    joinIndex.waitForMerges();
    columnsByPair.putAll(writeBatchesOfVaryingLength(101, 1));

    assertEquals(
        "nothing was compactable, so only the purge path could have reclaimed anything",
        0,
        joinIndex.mergePolicy.alignedMergeCount());
    assertEquals(
        "the segment still holds a live pair, so it must not be dropped whole",
        0,
        joinIndex.mergePolicy.droppedSegmentCount());
    assertTrue(
        "expected a single-segment purge to rewrite the segment holding the dead column",
        joinIndex.mergePolicy.purgedSegmentCount() >= 1);
    assertEquals(0, joinIndex.mergePolicy.pendingPairRemovalsCount());

    try (DirectoryReader sidecar = DirectoryReader.open(joinDir)) {
      assertEquals(List.of(), fieldsOf(sidecar, doomed));
      for (Map.Entry<String, int[]> pair : columnsByPair.entrySet()) {
        if (!pair.getKey().equals(doomed)) {
          assertColumnIntact(sidecar, pair.getKey(), pair.getValue());
        }
      }
    }
  }

  /**
   * Runs an already-created {@link Weight} across every leaf, returning the parent ids it matches.
   */
  private static Set<String> searchWith(Weight weight, IndexSearcher parents) throws IOException {
    Set<String> matched = new TreeSet<>();
    for (LeafReaderContext leaf : parents.getIndexReader().leaves()) {
      ScorerSupplier supplier = weight.scorerSupplier(leaf);
      if (supplier == null) {
        continue;
      }
      DocIdSetIterator docs = supplier.get(Long.MAX_VALUE).iterator();
      for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
        matched.add(parents.storedFields().document(leaf.docBase + doc).get(PARENT_ID));
      }
    }
    return matched;
  }

  /**
   * The race that made column-level reaping unsafe, and the reason it was left undone: a query
   * snapshots the pair columns it found at {@code createWeight} time, and the reaper can drop one
   * before the scorer supplier resolves it. That used to throw {@code "unable to refresh segment
   * refs"} and fail the query outright; now the pair is simply rebuilt, since the reap signal is a
   * heuristic and must be recoverable rather than fatal.
   */
  public void testQueryOutlivesTheReapingOfItsOwnColumns() throws Exception {
    List<String> parentIds = addParentsAndChildren("gen1-", atLeast(9));

    try (IndexReader childrenReader = childrenWriter.getReader();
        IndexReader parentsReader = parentsWriter.getReader()) {
      IndexSearcher parents = new IndexSearcher(parentsReader);
      IndexSearcher children = new IndexSearcher(childrenReader);

      // first pass builds the sidecar, so there are real columns to reap
      assertEquals(new TreeSet<>(parentIds), searchAllParents(parents, children));

      Query joinQuery =
          parents.rewrite(
              joinIndex.newJoinQuery(PARENT_ID_FK, new MatchAllDocsQuery(), children, PARENT_ID));
      // the weight snapshots which pair columns exist right now...
      Weight weight = joinQuery.createWeight(parents, ScoreMode.COMPLETE_NO_SCORES, 1f);

      // ...and every one of them is reaped before the scorers resolve it
      List<String> reaped = new ArrayList<>();
      try (DirectoryReader sidecar = DirectoryReader.open(joinDir)) {
        for (LeafReaderContext leaf : sidecar.leaves()) {
          reaped.addAll(JoinIndexUtils.pairFieldNames(leaf.reader().getFieldInfos()));
        }
      }
      assertFalse("the first pass should have built some pair columns", reaped.isEmpty());
      for (String pairFieldName : reaped) {
        joinIndex.mergePolicy.queueForRemoval(pairFieldName);
      }
      joinIndex.writeBatch(Map.of("trigger:dv0_trigger:dv0", model(new int[] {0})));
      joinIndex.waitForMerges();
      joinIndex.writeBatch(Map.of("trigger2:dv0_trigger2:dv0", model(new int[] {0})));

      assertEquals(
          "the query's columns should be gone by now",
          0,
          joinIndex.mergePolicy.pendingPairRemovalsCount());

      // the query, holding references to columns that no longer exist, still answers correctly
      assertEquals(new TreeSet<>(parentIds), searchWith(weight, parents));
    }
  }

  /**
   * A purge rewrites the whole segment to drop what is dead in it, so it has to reclaim enough
   * bytes to be worth the trip; until it does, the name simply stays queued. The threshold is in
   * bytes rather than in share of columns because share is what let the fattest segments -- the
   * ones holding the bytes -- dilute themselves out of ever being cleaned.
   */
  public void testAThinlyDeadSegmentIsNotWorthRewriting() throws Exception {
    joinIndex.mergePolicy.setCompaction(1000, 0); // nothing compactable, so only purges can act

    Map<String, int[]> columnsByPair = new LinkedHashMap<>(writeWideBatch(0, 20));
    // between what one column of twenty is worth and what four are: 1/20 must not pay, 4/20 must
    joinIndex.mergePolicy.setMinReclaimableBytesToPurge(widestSegmentBytes() / 8);
    String doomed = columnsByPair.keySet().iterator().next();
    joinIndex.mergePolicy.queueForRemoval(doomed);

    columnsByPair.putAll(writeBatchesOfVaryingLength(100, 1));
    joinIndex.waitForMerges();
    columnsByPair.putAll(writeBatchesOfVaryingLength(101, 1));

    assertEquals(
        "one dead column in twenty does not reclaim enough to pay for rewriting the other nineteen",
        0,
        joinIndex.mergePolicy.purgedSegmentCount());
    assertEquals(
        "and the name stays queued, so the next death in that segment can pay for the trip",
        1,
        joinIndex.mergePolicy.pendingPairRemovalsCount());

    try (DirectoryReader sidecar = DirectoryReader.open(joinDir)) {
      assertFalse(
          "the column is still there, just not yet worth removing",
          fieldsOf(sidecar, doomed).isEmpty());
    }

    // now enough of that segment dies to make the rewrite pay
    List<String> alsoDoomed = new ArrayList<>(columnsByPair.keySet()).subList(1, 4);
    for (String pairFieldName : alsoDoomed) {
      joinIndex.mergePolicy.queueForRemoval(pairFieldName);
    }
    columnsByPair.putAll(writeBatchesOfVaryingLength(102, 1));
    joinIndex.waitForMerges();
    columnsByPair.putAll(writeBatchesOfVaryingLength(103, 1));

    assertTrue(
        "4 of 20 dead is worth more than the threshold, so the segment should have been purged",
        joinIndex.mergePolicy.purgedSegmentCount() >= 1);
    assertEquals(0, joinIndex.mergePolicy.pendingPairRemovalsCount());
    try (DirectoryReader sidecar = DirectoryReader.open(joinDir)) {
      assertEquals(List.of(), fieldsOf(sidecar, doomed));
      for (String pairFieldName : alsoDoomed) {
        assertEquals(List.of(), fieldsOf(sidecar, pairFieldName));
      }
    }
  }

  /**
   * The threshold trades heap for I/O, and that trade has to stop once the queue is itself the
   * danger -- otherwise declining to rewrite is just the old unbounded leak with extra steps.
   */
  public void testTheThresholdIsWaivedOnceTheQueueIsDangerous() throws Exception {
    joinIndex.mergePolicy.setCompaction(1000, 0);

    Map<String, int[]> columnsByPair = new LinkedHashMap<>(writeWideBatch(0, 20));
    // nothing this segment holds could ever be worth a rewrite on its own
    joinIndex.mergePolicy.setMinReclaimableBytesToPurge(Long.MAX_VALUE);
    String doomed = columnsByPair.keySet().iterator().next();

    // a backlog of names for columns long gone, as a stalled reaper would accumulate
    for (int i = 0; i < 2048; i++) {
      joinIndex.mergePolicy.queueForRemoval("stale" + i + ":dv0_gone:dv0");
    }
    joinIndex.mergePolicy.queueForRemoval(doomed);

    columnsByPair.putAll(writeBatchesOfVaryingLength(100, 1));
    joinIndex.waitForMerges();
    columnsByPair.putAll(writeBatchesOfVaryingLength(101, 1));

    assertTrue(
        "over the high-water mark the threshold must be waived, thin or not",
        joinIndex.mergePolicy.purgedSegmentCount() >= 1);
    try (DirectoryReader sidecar = DirectoryReader.open(joinDir)) {
      assertEquals(List.of(), fieldsOf(sidecar, doomed));
      for (Map.Entry<String, int[]> pair : columnsByPair.entrySet()) {
        if (!pair.getKey().equals(doomed)) {
          assertColumnIntact(sidecar, pair.getKey(), pair.getValue());
        }
      }
    }
  }

  /**
   * The reap signal is keyed by directory, so consecutive samples come from different queries that
   * may hold different searcher generations. An older generation must not be allowed to speak: it
   * cannot see the segments opened since, and would report every one of them as dead -- queueing
   * live columns whose queries then have to rebuild them mid-flight.
   */
  public void testAnOlderSampleCannotCondemnPairsItCannotSee() throws Exception {
    addParentsAndChildren("gen1-", 3);
    try (DirectoryReader parentsGen1 = parentsWriter.getReader();
        DirectoryReader childrenGen1 = childrenWriter.getReader()) {
      addParentsAndChildren("gen2-", 3);
      try (DirectoryReader parentsGen2 = parentsWriter.getReader();
          DirectoryReader childrenGen2 = childrenWriter.getReader()) {
        assertTrue(
            "the second generation must be newer for this test to mean anything",
            parentsGen2.getVersion() > parentsGen1.getVersion());

        // the newer generation needs both pairs
        joinIndex.onCreateWeight(
            Set.of("pairA", "pairB"),
            PARENT_ID_FK,
            new IndexSearcher(childrenGen2),
            PARENT_ID,
            new IndexSearcher(parentsGen2));
        assertEquals(0, joinIndex.mergePolicy.pendingPairRemovalsCount());

        // an older query samples next and does not need pairB -- because it cannot see it yet
        joinIndex.onCreateWeight(
            Set.of("pairA"),
            PARENT_ID_FK,
            new IndexSearcher(childrenGen1),
            PARENT_ID,
            new IndexSearcher(parentsGen1));
        assertEquals(
            "a sample from an older generation must not condemn anything",
            0,
            joinIndex.mergePolicy.pendingPairRemovalsCount());

        // whereas the newest generation dropping it is a real death
        addParentsAndChildren("gen3-", 3);
        try (DirectoryReader parentsGen3 = parentsWriter.getReader();
            DirectoryReader childrenGen3 = childrenWriter.getReader()) {
          joinIndex.onCreateWeight(
              Set.of("pairA"),
              PARENT_ID_FK,
              new IndexSearcher(childrenGen3),
              PARENT_ID,
              new IndexSearcher(parentsGen3));
          assertEquals(
              "the newest generation no longer needing a pair is what death looks like",
              Set.of("pairB").size(),
              joinIndex.mergePolicy.pendingPairRemovalsCount());
        }
      }
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

  /** The size on disk of the sidecar's biggest segment, which the wide batch above just wrote. */
  private long widestSegmentBytes() throws IOException {
    long widest = 0;
    for (SegmentCommitInfo info : SegmentInfos.readLatestCommit(joinDir)) {
      widest = Math.max(widest, info.sizeInBytes());
    }
    return widest;
  }

  /** Every pair the sidecar currently stores a column for, across all its segments. */
  private Set<String> storedPairNames() throws IOException {
    Set<String> names = new TreeSet<>();
    try (DirectoryReader sidecar = DirectoryReader.open(joinDir)) {
      for (LeafReaderContext leaf : sidecar.leaves()) {
        names.addAll(JoinIndexUtils.pairFieldNames(leaf.reader().getFieldInfos()));
      }
    }
    return names;
  }

  /**
   * What a restart inherits has to be reclaimable, and the sampled death signal structurally cannot
   * reclaim it: that signal only fires when a query needs a pair and a later query doesn't, both
   * within one process, so a column whose from-segment was merged away while Solr was down is
   * needed by nobody, missed by nobody, and lives forever. Load tests ended with most of the
   * sidecar's bytes in exactly such columns, inherited from the previous run's directory.
   *
   * <p>Here the readers are the evidence instead: whatever side keys they do not offer name
   * segments that are gone, whether they went during this process or before it started.
   */
  public void testColumnsStrandedByARestartAreReaped() throws Exception {
    List<String> parentIds = addParentsAndChildren("gen1-", atLeast(6));
    try (IndexReader parentsReader = parentsWriter.getReader();
        IndexReader childrenReader = childrenWriter.getReader()) {
      assertEquals(
          new TreeSet<>(parentIds),
          searchAllParents(newSearcher(parentsReader), newSearcher(childrenReader)));
    }
    Set<String> inherited = storedPairNames();
    assertFalse("the join should have built columns to inherit", inherited.isEmpty());

    // the restart: a manager opened on the same directory, with no sampling history whatsoever
    joinIndex.close();
    joinIndex = new AuxIndexManager(joinDir);
    joinIndex.mergePolicy.setSweepInterval(0, TimeUnit.NANOSECONDS);

    // and while it was down, every from-segment those columns name was merged away
    childrenWriter.w.getConfig().setMergePolicy(new TieredMergePolicy());
    childrenWriter.forceMerge(1);
    childrenWriter.commit();

    try (IndexReader parentsReader = parentsWriter.getReader();
        IndexReader childrenReader = childrenWriter.getReader()) {
      assertEquals(1, childrenReader.leaves().size());
      // the first query rebuilds against the merged from-segment and commits, which is what puts
      // the inherited names in front of the policy; the second is the sample that judges them
      for (int sample = 0; sample < 2; sample++) {
        assertEquals(
            "the join must keep answering while its inherited columns are condemned",
            new TreeSet<>(parentIds),
            searchAllParents(newSearcher(parentsReader), newSearcher(childrenReader)));
      }
    }

    assertTrue(
        "a restart's own readers show every inherited column's from-segment gone, so the sweep "
            + "should have condemned all "
            + inherited.size()
            + " of them -- the snapshot diff never can, no query here needing them twice -- but it "
            + "condemned "
            + joinIndex.mergePolicy.strandedColumnCount(),
        joinIndex.mergePolicy.strandedColumnCount() >= inherited.size());

    // and queued means reclaimed: the segments holding them are now entirely dead
    writeBatchesOfVaryingLength(500, 1);
    joinIndex.waitForMerges();
    writeBatchesOfVaryingLength(501, 1); // a commit publishing whatever just dropped

    assertTrue(
        "the inherited columns should have been dropped, reaped=" + reapedOrDropped(),
        reapedOrDropped() > 0);
    Set<String> left = storedPairNames();
    left.retainAll(inherited);
    assertEquals("no inherited column should still be on disk", Set.of(), left);
  }

  private int reapedOrDropped() {
    return joinIndex.mergePolicy.reapedPairCount() + joinIndex.mergePolicy.droppedSegmentCount();
  }
}

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

import static org.apache.lucene.tests.util.LuceneTestCase.newIndexWriterConfig;
import static org.apache.lucene.tests.util.LuceneTestCase.random;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCase;
import org.apache.solr.util.LogLevel;

/**
 * Real (non-mocked) integration smoke test for {@link AIJoinMergePolicy}: builds a segmented
 * children/parents pair, forces {@link AIJoinQuery} to populate the sidecar join index, then mass
 * deletes and force-merges both sides so the sidecar's previously-built pair columns go stale, and
 * checks the policy actually notices and reaps them -- not just that it runs without throwing.
 */
@LogLevel("org.apache.solr.search.join.aijoin=WARN")
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "no.url")
public class TestAIJoinMergePolicy extends SolrTestCase {

  private static final String ID = "id";
  private static final String PARENT_ID = "parent_id";
  private static final String PARENT_ID_FK = "parent_id_FK";

  private Directory parentsDir;
  private Directory childrenDir;
  private Directory joinDir;
  private RandomIndexWriter parentsWriter;
  private RandomIndexWriter childrenWriter;
  private AIJoinIndex joinIndex;

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
    joinIndex = new AIJoinIndex(joinDir);
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
   * forcing {@link AIJoinQuery} to fully execute (not just build its {@link
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
        "AIJoinMergePolicy should have reaped at least the one dead sidecar segment",
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
}

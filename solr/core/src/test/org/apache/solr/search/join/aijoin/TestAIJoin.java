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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Joins a children index to a parents index with {@link JoinUtil}. Children reference parents via a
 * single valued {@link SortedSetDocValuesField} {@code parent_id_FK} pointing to the parents'
 * single valued {@code parent_id} (M:1).
 */
public class TestAIJoin extends LuceneTestCase {

  private static final String ID = "id";
  private static final String PARENT_ID = "parent_id";
  private static final String PARENT_ID_FK = "parent_id_FK";
  private static final String COLOR = "color";

  private static final String[] COLORS = {"red", "green", "blue"};
  private static final int CHILDREN_PER_PARENT = 5;

  /** Shared per-test auxiliary join index: pair columns are built lazily by the first search. */
  private AIJoinIndex joinIndex;

  private Directory joinDir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    joinDir = newDirectory();
    // none of these affect join correctness, only internal segment layout, refresh latency, and
    // dead-pair reap timing -- randomized to exercise all combinations
    AIJoinIndexConfig config =
        new AIJoinIndexConfig()
            .setSingleFieldPerSegment(random().nextBoolean())
            .setBlockingRefresh(random().nextBoolean())
            .setSweepSamplingInterval(TestUtil.nextInt(random(), -1, 2), TimeUnit.MINUTES);
    joinIndex = new AIJoinIndex(joinDir, config);
  }

  @Override
  public void tearDown() throws Exception {
    IOUtils.close(joinIndex, joinDir);
    super.tearDown();
  }

  /** Two indices: parents and their children, segmented by intermediate commits. */
  private class ParentChildIndices implements Closeable {
    final Directory parentsDir = newDirectory();
    final Directory childrenDir = newDirectory();
    final RandomIndexWriter parentsWriter;
    final RandomIndexWriter childrenWriter;
    // childId -> parentId, insertion order
    final Map<String, String> parentIdByChildId = new TreeMap<>();
    final Map<String, String> colorByParentId = new TreeMap<>();

    ParentChildIndices() throws IOException {
      // NoMergePolicy keeps the segments created by the intermediate commits below
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
      int numParents = atLeast(10);
      int childSeq = 0;
      for (int p = 0; p < numParents; p++) {
        String parentId = "parent" + p;
        String color = RandomPicks.randomFrom(random(), COLORS);
        colorByParentId.put(parentId, color);
        parentsWriter.addDocument(parentDoc(parentId, color));
        for (int c = 0; c < CHILDREN_PER_PARENT; c++) {
          String childId = "child" + childSeq++;
          parentIdByChildId.put(childId, parentId);
          childrenWriter.addDocument(childDoc(childId, parentId));
        }
        if (p % 3 == 2) {
          parentsWriter.commit();
          childrenWriter.commit();
        }
      }
    }

    List<String> childrenOf(String parentId) {
      List<String> children = new ArrayList<>();
      parentIdByChildId.forEach(
          (childId, pid) -> {
            if (pid.equals(parentId)) {
              children.add(childId);
            }
          });
      return children;
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(parentsWriter, childrenWriter, parentsDir, childrenDir);
    }
  }

  private static Document parentDoc(String parentId, String color) {
    Document doc = new Document();
    doc.add(new StringField(PARENT_ID, parentId, Field.Store.YES));
    doc.add(new SortedSetDocValuesField(PARENT_ID, new BytesRef(parentId)));
    doc.add(new StringField(COLOR, color, Field.Store.YES));
    return doc;
  }

  private static Document childDoc(String childId, String parentId) {
    Document doc = new Document();
    doc.add(new StringField(ID, childId, Field.Store.YES));
    doc.add(new StringField(PARENT_ID_FK, parentId, Field.Store.YES));
    doc.add(new SortedSetDocValuesField(PARENT_ID_FK, new BytesRef(parentId)));
    return doc;
  }

  private static Query joinChildrenToParents(Query fromQuery, IndexSearcher childrenSearcher)
      throws IOException {
    // any score mode matches the same parents
    ScoreMode scoreMode = RandomPicks.randomFrom(random(), ScoreMode.values());
    return JoinUtil.createJoinQuery(
        PARENT_ID_FK, true, PARENT_ID, fromQuery, childrenSearcher, scoreMode);
  }

  private static Set<String> searchParentIds(IndexSearcher parentsSearcher, Query query)
      throws IOException {
    TopDocs topDocs = parentsSearcher.search(query, parentsSearcher.getIndexReader().maxDoc());
    Set<String> parentIds = new TreeSet<>();
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      parentIds.add(parentsSearcher.storedFields().document(scoreDoc.doc).get(PARENT_ID));
    }
    return parentIds;
  }

  /**
   * Searches the same join through {@link JoinUtil} and through the auxiliary join index and
   * asserts both agree; {@code joinDecorator} wraps each join query the same way before searching.
   */
  private Set<String> searchParentIdsBothJoins(
      IndexSearcher parentsSearcher,
      Query fromQuery,
      IndexSearcher childrenSearcher,
      UnaryOperator<Query> joinDecorator)
      throws IOException {
    Query joinUtilQuery = joinChildrenToParents(fromQuery, childrenSearcher);
    Set<String> joinUtilParents =
        searchParentIds(parentsSearcher, joinDecorator.apply(joinUtilQuery));
    Query aiJoinQuery =
        joinIndex.newJoinQuery(PARENT_ID_FK, fromQuery, childrenSearcher, PARENT_ID);
    assertEquals(
        "AIJoinQuery disagrees with JoinUtil",
        joinUtilParents,
        searchParentIds(parentsSearcher, joinDecorator.apply(aiJoinQuery)));
    return joinUtilParents;
  }

  private Set<String> searchParentIdsBothJoins(
      IndexSearcher parentsSearcher, Query fromQuery, IndexSearcher childrenSearcher)
      throws IOException {
    return searchParentIdsBothJoins(
        parentsSearcher, fromQuery, childrenSearcher, UnaryOperator.identity());
  }

  private static Query anyOfChildren(Set<String> childIds) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (String childId : childIds) {
      builder.add(new TermQuery(new Term(ID, childId)), BooleanClause.Occur.SHOULD);
    }
    return builder.build();
  }

  private static Set<String> randomChildrenSubset(ParentChildIndices indices) {
    Set<String> selected = new TreeSet<>();
    for (String childId : indices.parentIdByChildId.keySet()) {
      if (random().nextBoolean()) {
        selected.add(childId);
      }
    }
    if (selected.isEmpty()) {
      selected.add(RandomPicks.randomFrom(random(), indices.parentIdByChildId.keySet()));
    }
    return selected;
  }

  private static Set<String> expectedParents(ParentChildIndices indices, Set<String> childIds) {
    Set<String> expected = new TreeSet<>();
    for (String childId : childIds) {
      expected.add(indices.parentIdByChildId.get(childId));
    }
    return expected;
  }

  public void testAIJoinRandomChildrenSubset() throws Exception {
    try (ParentChildIndices indices = new ParentChildIndices()) {
      try (IndexReader childrenReader = indices.childrenWriter.getReader();
          IndexReader parentsReader = indices.parentsWriter.getReader()) {
        assertTrue("children index should be segmented", childrenReader.leaves().size() > 1);
        assertTrue("parents index should be segmented", parentsReader.leaves().size() > 1);

        Set<String> selectedChildren = randomChildrenSubset(indices);
        // the first search builds the missing pair columns into the join index on demand
        assertEquals(
            expectedParents(indices, selectedChildren),
            searchParentIdsBothJoins(
                newSearcher(parentsReader),
                anyOfChildren(selectedChildren),
                newSearcher(childrenReader)));
      }

      // grow both sides and reopen: the next search finds the old pair columns persisted and
      // lazily builds only the pairs involving the new segments
      String newParentId = "parentNew";
      String newParentColor = RandomPicks.randomFrom(random(), COLORS);
      indices.colorByParentId.put(newParentId, newParentColor);
      indices.parentsWriter.addDocument(parentDoc(newParentId, newParentColor));
      String newChildId = "childNew";
      String oldChildId = RandomPicks.randomFrom(random(), indices.parentIdByChildId.keySet());
      indices.parentIdByChildId.put(newChildId, newParentId);
      indices.childrenWriter.addDocument(childDoc(newChildId, newParentId));
      indices.parentsWriter.commit();
      indices.childrenWriter.commit();
      try (IndexReader childrenReader = indices.childrenWriter.getReader();
          IndexReader parentsReader = indices.parentsWriter.getReader()) {
        assertEquals(
            Set.of(newParentId),
            searchParentIdsBothJoins(
                newSearcher(parentsReader),
                new TermQuery(new Term(ID, newChildId)),
                newSearcher(childrenReader)));

        // one query spanning old and new children joins to their old and new parents
        assertEquals(
            Set.of(indices.parentIdByChildId.get(oldChildId), newParentId),
            searchParentIdsBothJoins(
                newSearcher(parentsReader),
                anyOfChildren(new TreeSet<>(List.of(oldChildId, newChildId))),
                newSearcher(childrenReader)));
      }
    }
  }

  public void testUpdateChildParentIdFK() throws Exception {
    try (ParentChildIndices indices = new ParentChildIndices()) {
      // repeated re-pointing exercises the sidecar join index's update path many times over,
      // not just a single one-off update
      for (int iter = 0; iter < atLeast(5); iter++) {
        String childId = RandomPicks.randomFrom(random(), indices.parentIdByChildId.keySet());
        String oldParentId = indices.parentIdByChildId.get(childId);
        String newParentId;
        do {
          newParentId = RandomPicks.randomFrom(random(), indices.colorByParentId.keySet());
        } while (newParentId.equals(oldParentId));

        // SortedSetDocValues can't be updated in place, so replace the whole child document
        indices.childrenWriter.updateDocument(
            new Term(ID, childId), childDoc(childId, newParentId));
        indices.parentIdByChildId.put(childId, newParentId);

        try (IndexReader childrenReader = indices.childrenWriter.getReader();
            IndexReader parentsReader = indices.parentsWriter.getReader()) {
          IndexSearcher childrenSearcher = newSearcher(childrenReader);
          IndexSearcher parentsSearcher = newSearcher(parentsReader);

          assertEquals(
              Set.of(newParentId),
              searchParentIdsBothJoins(
                  parentsSearcher, new TermQuery(new Term(ID, childId)), childrenSearcher));

          // the old parent is reachable only while it still has other children pointing at it
          Set<String> expectedOldParent =
              indices.childrenOf(oldParentId).isEmpty() ? Set.of() : Set.of(oldParentId);
          assertEquals(
              expectedOldParent,
              searchParentIdsBothJoins(
                  parentsSearcher,
                  new TermQuery(new Term(PARENT_ID_FK, oldParentId)),
                  childrenSearcher));
        }
      }
    }
  }

  public void testUpdateParentId() throws Exception {
    try (ParentChildIndices indices = new ParentChildIndices()) {
      // repeated renaming exercises the sidecar join index's update path many times over, not
      // just a single one-off rename
      for (int iter = 0; iter < atLeast(5); iter++) {
        String parentId = RandomPicks.randomFrom(random(), indices.colorByParentId.keySet());
        String renamedParentId = parentId + "-renamed" + iter;
        String color = indices.colorByParentId.remove(parentId);
        indices.parentsWriter.updateDocument(
            new Term(PARENT_ID, parentId), parentDoc(renamedParentId, color));
        indices.colorByParentId.put(renamedParentId, color);

        try (IndexReader childrenReader = indices.childrenWriter.getReader();
            IndexReader parentsReader = indices.parentsWriter.getReader()) {
          // children still point at the old id, so they join to nothing
          assertEquals(
              Set.of(),
              searchParentIdsBothJoins(
                  newSearcher(parentsReader),
                  new TermQuery(new Term(PARENT_ID_FK, parentId)),
                  newSearcher(childrenReader)));
        }

        // re-point one child at the renamed parent and the join works again, if any of this
        // parent's children haven't already been re-pointed elsewhere by an earlier iteration
        List<String> children = indices.childrenOf(parentId);
        if (children.isEmpty()) {
          continue;
        }
        String childId = RandomPicks.randomFrom(random(), children);
        indices.childrenWriter.updateDocument(
            new Term(ID, childId), childDoc(childId, renamedParentId));
        indices.parentIdByChildId.put(childId, renamedParentId);

        try (IndexReader childrenReader = indices.childrenWriter.getReader();
            IndexReader parentsReader = indices.parentsWriter.getReader()) {
          assertEquals(
              Set.of(renamedParentId),
              searchParentIdsBothJoins(
                  newSearcher(parentsReader),
                  new TermQuery(new Term(ID, childId)),
                  newSearcher(childrenReader)));
        }
      }
    }
  }

  public void testAIJoinWithParentTermFilter() throws Exception {
    try (ParentChildIndices indices = new ParentChildIndices()) {
      try (IndexReader childrenReader = indices.childrenWriter.getReader();
          IndexReader parentsReader = indices.parentsWriter.getReader();
          Directory joinDir = newDirectory();
          AIJoinIndex joinIndex = new AIJoinIndex(joinDir)) {
        Set<String> selectedChildren = randomChildrenSubset(indices);
        String color = RandomPicks.randomFrom(random(), COLORS);

        Query aiJoinQuery =
            joinIndex.newJoinQuery(
                PARENT_ID_FK,
                anyOfChildren(selectedChildren),
                newSearcher(childrenReader),
                PARENT_ID);

        Query filteredJoin =
            new BooleanQuery.Builder()
                .add(aiJoinQuery, BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term(COLOR, color)), BooleanClause.Occur.FILTER)
                .build();

        Set<String> expected = new TreeSet<>();
        for (String parentId : expectedParents(indices, selectedChildren)) {
          if (color.equals(indices.colorByParentId.get(parentId))) {
            expected.add(parentId);
          }
        }
        assertEquals(expected, searchParentIds(newSearcher(parentsReader), filteredJoin));
      }
    }
  }

  public void testJoinWithParentTermFilter() throws Exception {
    try (ParentChildIndices indices = new ParentChildIndices()) {
      try (IndexReader childrenReader = indices.childrenWriter.getReader();
          IndexReader parentsReader = indices.parentsWriter.getReader()) {
        Set<String> selectedChildren = randomChildrenSubset(indices);
        String color = RandomPicks.randomFrom(random(), COLORS);

        Set<String> expected = new TreeSet<>();
        for (String parentId : expectedParents(indices, selectedChildren)) {
          if (color.equals(indices.colorByParentId.get(parentId))) {
            expected.add(parentId);
          }
        }
        assertEquals(
            expected,
            searchParentIdsBothJoins(
                newSearcher(parentsReader),
                anyOfChildren(selectedChildren),
                newSearcher(childrenReader),
                join ->
                    new BooleanQuery.Builder()
                        .add(join, BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term(COLOR, color)), BooleanClause.Occur.FILTER)
                        .build()));
      }
    }
  }
}

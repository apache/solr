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
package org.apache.solr.search.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.solr.SolrTestCase;
import org.apache.solr.schema.IndexSchema;
import org.junit.Test;

public class WithDescendantsQueryTest extends SolrTestCase {

  private static final String NEST_PATH = IndexSchema.NEST_PATH_FIELD_NAME;

  private static final BitSetProducer ROOTS =
      new QueryBitSetProducer(
          new BooleanQuery.Builder()
              .add(MatchAllDocsQuery.INSTANCE, Occur.MUST)
              .add(new FieldExistsQuery(NEST_PATH), Occur.MUST_NOT)
              .build());

  /** Like {@link #ROOTS} but always sparse, which the query handles differently. */
  private static final BitSetProducer SPARSE_ROOTS =
      new BitSetProducer() {
        @Override
        public BitSet getBitSet(LeafReaderContext context) throws IOException {
          final BitSet roots = ROOTS.getBitSet(context);
          if (roots == null) {
            return null;
          }
          final SparseFixedBitSet sparse = new SparseFixedBitSet(roots.length());
          sparse.or(new BitSetIterator(roots, 0));
          return sparse;
        }
      };

  /** A document in the reference model. */
  private static final class Node {
    final String id;
    final String rootId;
    final int depth;
    final String path; // full path with ordinals, as Solr would put in docValues; null for roots
    final List<Node> children = new ArrayList<>();

    Node(String id, String rootId, int depth, String path) {
      this.id = id;
      this.rootId = rootId;
      this.depth = depth;
      this.path = path;
    }

    void addSubtreeIds(Set<String> ids) {
      ids.add(id);
      for (Node child : children) {
        child.addSubtreeIds(ids);
      }
    }
  }

  private final Map<String, Node> nodes = new HashMap<>();
  private final Set<String> deletedRoots = new HashSet<>();

  @Test
  public void testExample() throws IOException {
    // The example in the javadocs: root with children /a (with /a/b) twice, and /c
    Node root = new Node("root", "root", 0, null);
    Node a1 = addChild(root, "a", 0);
    addChild(a1, "b", 0);
    Node a2 = addChild(root, "a", 1);
    addChild(a2, "b", 0);
    addChild(root, "c", 0);
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, newIwc())) {
        writer.addDocuments(toDocs(root));
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        assertEquals(Set.of("root/a#1/b#0", "root/a#1"), search(searcher, idsQuery("root/a#1")));
        assertEquals(6, search(searcher, idsQuery("root")).size());
        assertEquals(Set.of("root/c#0"), search(searcher, idsQuery("root/c#0")));
        assertEquals(
            Set.of("root/a#0", "root/a#0/b#0", "root/c#0"),
            search(searcher, idsQuery("root/a#0", "root/c#0")));
      }
    }
  }

  @Test
  public void testRandom() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, newIwc())) {
        final int numTrees = atLeast(50);
        final boolean doDeletes = random().nextBoolean();
        for (int t = 0; t < numTrees; t++) {
          Node root = randomTree("r" + t);
          writer.addDocuments(toDocs(root));
          if (random().nextInt(10) == 0) {
            writer.commit(); // more segments
          }
          if (doDeletes && random().nextInt(15) == 0) {
            String rootId = "r" + random().nextInt(t + 1);
            writer.deleteDocuments(new Term("root", rootId));
            deletedRoots.add(rootId);
          }
        }
        if (rarely()) {
          writer.forceMerge(1);
        }
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        final List<Node> all = new ArrayList<>(nodes.values());
        final int numQueries = atLeast(30);
        for (int i = 0; i < numQueries; i++) {
          final Predicate<Node> filter =
              switch (random().nextInt(4)) {
                case 0 -> n -> n.depth == 0; // roots only (the fast path)
                case 1 -> n -> n.depth > 0; // children only
                default -> n -> true;
              };
          final double probability = random().nextDouble() * random().nextDouble();
          final List<String> ids = new ArrayList<>();
          for (Node node : all) {
            if (filter.test(node) && random().nextDouble() < probability) {
              ids.add(node.id);
            }
          }
          final Query query = idsQuery(ids.toArray(new String[0]));
          final BitSetProducer roots = random().nextBoolean() ? ROOTS : SPARSE_ROOTS;
          assertEquals("ids: " + ids, expected(ids), search(searcher, query, roots));
          // QueryUtils.checkFirstSkipTo misjudges deleted docs: it uses the next leaf's liveDocs
          if (i < 3 && !reader.hasDeletions()) {
            QueryUtils.check(random(), new WithDescendantsQuery(query, ROOTS), searcher);
          }
        }
      }
    }
  }

  @Test
  public void testNoNestedDocs() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, newIwc())) {
        for (int i = 0; i < 5; i++) {
          writer.addDocuments(toDocs(new Node("r" + i, "r" + i, 0, null)));
        }
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        assertEquals(Set.of("r1", "r3"), search(searcher, idsQuery("r1", "r3")));
        assertEquals(Set.of(), search(searcher, idsQuery("bogus")));
      }
    }
  }

  @Test
  public void testRewrite() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, newIwc())) {
        writer.addDocuments(toDocs(new Node("r", "r", 0, null)));
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        assertTrue(
            searcher.rewrite(new WithDescendantsQuery(new MatchNoDocsQuery(), ROOTS))
                instanceof MatchNoDocsQuery);
        assertTrue(
            searcher.rewrite(new WithDescendantsQuery(MatchAllDocsQuery.INSTANCE, ROOTS))
                instanceof MatchAllDocsQuery);
        Query termQuery = new TermQuery(new Term("id", "r"));
        Query query = new WithDescendantsQuery(termQuery, ROOTS);
        assertSame(query, searcher.rewrite(query));
      }
    }
  }

  @Test
  public void testEquals() {
    Query a = new TermQuery(new Term("id", "a"));
    Query b = new TermQuery(new Term("id", "b"));
    BitSetProducer otherRoots = new QueryBitSetProducer(new TermQuery(new Term("x", "y")));
    QueryUtils.checkEqual(new WithDescendantsQuery(a, ROOTS), new WithDescendantsQuery(a, ROOTS));
    QueryUtils.checkUnequal(new WithDescendantsQuery(a, ROOTS), new WithDescendantsQuery(b, ROOTS));
    QueryUtils.checkUnequal(
        new WithDescendantsQuery(a, ROOTS), new WithDescendantsQuery(a, otherRoots));
  }

  private static IndexWriterConfig newIwc() {
    // a merge policy that doesn't reorder docs, which would break blocks
    return newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random()));
  }

  private Node randomTree(String rootId) {
    Node root = new Node(rootId, rootId, 0, null);
    nodes.put(rootId, root);
    addRandomChildren(root, 1 + random().nextInt(4));
    return root;
  }

  private void addRandomChildren(Node parent, int maxDepth) {
    if (parent.depth >= maxDepth) {
      return;
    }
    final int numChildren = random().nextInt(4);
    for (int i = 0; i < numChildren; i++) {
      Node child = addChild(parent, random().nextBoolean() ? "a" : "b", i);
      addRandomChildren(child, maxDepth);
    }
  }

  private Node addChild(Node parent, String label, int ordinal) {
    final String path = (parent.path == null ? "" : parent.path) + "/" + label + "#" + ordinal;
    Node child = new Node(parent.rootId + path, parent.rootId, parent.depth + 1, path);
    parent.children.add(child);
    nodes.put(child.id, child);
    return child;
  }

  /** Flattens the tree post-order, as Solr does: descendants before their ancestor. */
  private static List<Document> toDocs(Node node) {
    List<Document> docs = new ArrayList<>();
    for (Node child : node.children) {
      docs.addAll(toDocs(child));
    }
    Document doc = new Document();
    doc.add(new StringField("id", node.id, Field.Store.YES));
    doc.add(new StringField("root", node.rootId, Field.Store.NO));
    if (node.path != null) {
      // like NestPathField: the indexed term lacks the ordinals; docValues has the full path
      doc.add(new StringField(NEST_PATH, node.path.replaceAll("#\\d*", ""), Field.Store.NO));
      doc.add(new SortedDocValuesField(NEST_PATH, new BytesRef(node.path)));
    }
    docs.add(doc);
    return docs;
  }

  private static Query idsQuery(String... ids) {
    List<BytesRef> terms = new ArrayList<>();
    for (String id : ids) {
      terms.add(new BytesRef(id));
    }
    return new TermInSetQuery("id", terms);
  }

  private Set<String> expected(List<String> ids) {
    Set<String> result = new HashSet<>();
    for (String id : ids) {
      Node node = nodes.get(id);
      if (!deletedRoots.contains(node.rootId)) {
        node.addSubtreeIds(result);
      }
    }
    return result;
  }

  private static Set<String> search(IndexSearcher searcher, Query query) throws IOException {
    return search(searcher, query, ROOTS);
  }

  private static Set<String> search(IndexSearcher searcher, Query query, BitSetProducer roots)
      throws IOException {
    final Query wrapped = new WithDescendantsQuery(query, roots);
    final int maxDoc = Math.max(1, searcher.getIndexReader().maxDoc());
    final StoredFields storedFields = searcher.storedFields();
    final Set<String> ids = new HashSet<>();
    for (ScoreDoc scoreDoc : searcher.search(wrapped, maxDoc).scoreDocs) {
      assertTrue("duplicate", ids.add(storedFields.document(scoreDoc.doc).get("id")));
    }
    assertEquals(ids.size(), searcher.count(wrapped));
    return ids;
  }
}

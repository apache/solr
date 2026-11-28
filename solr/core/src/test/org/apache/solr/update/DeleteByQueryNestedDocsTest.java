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
package org.apache.solr.update;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.RandomNoReverseMergePolicyFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

/** Delete-by-query also deletes the nested descendants of the docs it matches. */
public class DeleteByQueryNestedDocsTest extends SolrTestCase {

  private static final String ROOT_ONLY = "rootOnly";

  @ClassRule
  public static final EmbeddedSolrServerTestRule solrTestRule = new EmbeddedSolrServerTestRule();

  @ClassRule
  public static final TestRule noReverseMerge = RandomNoReverseMergePolicyFactory.createRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrTestRule.startSolr(SolrTestCaseJ4.TEST_HOME());
    SolrTestCaseJ4.newRandomConfig();
    solrTestRule
        .newCollection()
        .withConfigSet(SolrTestCaseJ4.TEST_COLL1_CONF())
        .withSchemaFile("schema15.xml") // has _nest_path_
        .create();
    solrTestRule
        .newCollection(ROOT_ONLY)
        .withConfigSet(SolrTestCaseJ4.TEST_COLL1_CONF())
        .withSchemaFile("schema-root.xml") // has _root_ but not _nest_path_
        .withProperty("solr.index.updatelog.enabled", "false") // schema lacks _version_
        .create();
  }

  /** A document in the reference model. */
  private static final class Node {
    final String id;
    final Node parent;
    final String path; // without ordinals; null for roots
    final String color; // may be null
    final List<Node> children = new ArrayList<>();

    Node(String id, Node parent, String path, String color) {
      this.id = id;
      this.parent = parent;
      this.path = path;
      this.color = color;
    }

    boolean hasDescendant(Predicate<Node> predicate) {
      for (Node child : children) {
        if (predicate.test(child) || child.hasDescendant(predicate)) {
          return true;
        }
      }
      return false;
    }

    void addSubtree(Set<String> ids) {
      ids.add(id);
      for (Node child : children) {
        child.addSubtree(ids);
      }
    }
  }

  private static final String[] COLORS = {"red", "green", "blue", null};

  /** All live docs by id, including children. */
  private final Map<String, Node> model = new LinkedHashMap<>();

  /**
   * Roots of trees that had a subtree deleted. The deleted docs remain in the block until merged
   * away, and {@code {!parent}} still matches parents of deleted children, so such trees are
   * re-indexed before those queries.
   */
  private final Set<String> partiallyDeletedRoots = new HashSet<>();

  private int treeCounter;

  @Before
  public void before() throws Exception {
    solrTestRule.clearIndex();
    solrTestRule.getSolrClient(ROOT_ONLY).deleteByQuery("*:*");
    solrTestRule.getSolrClient(ROOT_ONLY).commit();
  }

  @Test
  public void testDeleteAtEachLevel() throws Exception {
    SolrClient client = solrTestRule.getSolrClient();
    client.add(fixedTree("A"));
    client.add(fixedTree("B"));
    client.commit();
    assertEquals(14, numFound("*:*"));

    // leaf
    client.deleteByQuery("id:A.0.0");
    client.commit();
    assertEquals(13, numFound("*:*"));
    assertEquals(1, numFound("id:A.0"));

    // mid-level: its descendants go too, but not its parent or siblings
    client.deleteByQuery("id:A.0");
    client.commit();
    assertEquals(11, numFound("*:*"));
    assertEquals(4, numFound("_root_:A"));

    // root
    client.deleteByQuery("id:A");
    client.commit();
    assertEquals(0, numFound("_root_:A"));
    assertEquals(7, numFound("_root_:B"));
  }

  @Test
  public void testUncommittedDocs() throws Exception {
    SolrClient client = solrTestRule.getSolrClient();
    client.add(fixedTree("A"));
    client.deleteByQuery("id:A.1");
    client.add(fixedTree("B"));
    client.commit();
    assertEquals(4 + 7, numFound("*:*"));
    assertEquals(0, numFound("id:A.1*"));
  }

  @Test
  public void testOptOut() throws Exception {
    SolrClient client = solrTestRule.getSolrClient();
    client.add(fixedTree("A"));
    client.add(fixedTree("B"));
    client.commit();

    client.deleteByQuery("{!lucene withDescendants=true}id:B");
    client.commit();
    assertEquals(0, numFound("_root_:B"));

    // the user takes responsibility; here we leave orphans behind
    client.deleteByQuery("{!lucene withDescendants=false}id:A");
    client.commit();
    assertEquals(0, numFound("id:A"));
    assertEquals(6, numFound("_root_:A"));
  }

  @Test
  public void testRootOnlySchema() throws Exception {
    // without _nest_path_, delete-by-query isn't expanded
    SolrClient client = solrTestRule.getSolrClient(ROOT_ONLY);
    client.add(fixedTree("A"));
    client.commit();
    client.deleteByQuery("id:A");
    client.commit();
    assertEquals(0, numFound(client, "id:A"));
    assertEquals(6, numFound(client, "*:*"));
  }

  @Test
  public void testRandom() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient();
    final int rounds = atLeast(20);
    for (int round = 0; round < rounds; round++) {
      // add or replace some trees
      final int numTrees = 1 + random().nextInt(5);
      for (int t = 0; t < numTrees; t++) {
        final String rootId;
        if (!model.isEmpty() && random().nextInt(5) == 0) {
          rootId = randomLiveRoot(); // replace
          deleteFromModel(Set.of(model.get(rootId)));
          partiallyDeletedRoots.remove(rootId);
        } else {
          rootId = "t" + treeCounter++;
        }
        client.add(randomTree(rootId));
      }
      switch (random().nextInt(3)) {
        case 0 -> client.commit();
        case 1 -> client.commit(true, true, true); // soft
        default -> {} // DBQ sees uncommitted docs too
      }

      deleteByRandomQuery(client);
      client.commit();
      assertModel();
    }
  }

  private void deleteByRandomQuery(SolrClient client) throws Exception {
    final String color = COLORS[random().nextInt(COLORS.length - 1)];
    final String query;
    final Predicate<Node> matches;
    switch (random().nextInt(7)) {
      case 0 -> {
        query = "color_s:" + color;
        matches = n -> color.equals(n.color);
      }
      case 1 -> {
        query = "{!term f=color_s}" + color;
        matches = n -> color.equals(n.color);
      }
      case 2 -> { // pure negative
        query = "-color_s:" + color;
        matches = n -> !color.equals(n.color);
      }
      case 3 -> { // roots with a matching descendant
        reindexPartiallyDeletedTrees(client);
        query = "{!parent parentPath=/}color_s:" + color;
        matches = n -> n.parent == null && n.hasDescendant(c -> color.equals(c.color));
      }
      case 4 -> { // mid-level docs with a matching descendant
        reindexPartiallyDeletedTrees(client);
        query = "{!parent parentPath=/kids}color_s:" + color;
        matches = n -> "/kids".equals(n.path) && n.hasDescendant(c -> color.equals(c.color));
      }
      case 5 -> {
        if (rarely()) {
          query = "*:*";
          matches = n -> true;
          break;
        }
        // random ids, at any level
        final Set<String> ids = new HashSet<>();
        final List<String> allIds = new ArrayList<>(model.keySet());
        final int numIds = allIds.isEmpty() ? 0 : 1 + random().nextInt(3);
        for (int i = 0; i < numIds; i++) {
          ids.add(allIds.get(random().nextInt(allIds.size())));
        }
        query = ids.isEmpty() ? "id:bogus" : "id:(" + String.join(" ", ids) + ")";
        matches = n -> ids.contains(n.id);
      }
      default -> { // docs at a nest path
        query = "_nest_path_:\\/kids\\/subs";
        matches = n -> "/kids/subs".equals(n.path);
      }
    }

    final Set<Node> matched = new HashSet<>();
    for (Node node : model.values()) {
      if (matches.test(node)) {
        matched.add(node);
      }
    }
    client.deleteByQuery(query);
    deleteFromModel(matched);
  }

  private void deleteFromModel(Set<Node> matched) {
    final Set<String> toDelete = new HashSet<>();
    for (Node node : matched) {
      node.addSubtree(toDelete);
      if (node.parent != null) {
        node.parent.children.remove(node);
        partiallyDeletedRoots.add(node.id.substring(0, node.id.indexOf('.')));
      }
    }
    model.keySet().removeAll(toDelete);
    partiallyDeletedRoots.removeAll(toDelete);
  }

  private void reindexPartiallyDeletedTrees(SolrClient client) throws Exception {
    for (String rootId : partiallyDeletedRoots) {
      client.add(toDoc(model.get(rootId)));
    }
    partiallyDeletedRoots.clear();
  }

  private static SolrInputDocument toDoc(Node node) {
    final SolrInputDocument doc = new SolrInputDocument("id", node.id);
    if (node.color != null) {
      doc.setField("color_s", node.color);
    }
    if (!node.children.isEmpty()) {
      final List<SolrInputDocument> children = new ArrayList<>();
      for (Node child : node.children) {
        children.add(toDoc(child));
      }
      doc.setField(node.parent == null ? "kids" : "subs", children);
    }
    return doc;
  }

  private void assertModel() throws Exception {
    final SolrQuery query = new SolrQuery("*:*");
    query.setFields("id");
    query.setRows(model.size() + 100);
    final Set<String> actual = new HashSet<>();
    for (SolrDocument doc : solrTestRule.getSolrClient().query(query).getResults()) {
      actual.add((String) doc.getFieldValue("id"));
    }
    assertEquals(model.keySet(), actual);
  }

  private String randomLiveRoot() {
    final List<String> roots = new ArrayList<>();
    for (Node node : model.values()) {
      if (node.parent == null) {
        roots.add(node.id);
      }
    }
    return roots.get(random().nextInt(roots.size()));
  }

  private SolrInputDocument randomTree(String rootId) {
    return randomDoc(rootId, null, null, 1 + random().nextInt(3));
  }

  private SolrInputDocument randomDoc(String id, Node parent, String path, int maxDepth) {
    final String color = COLORS[random().nextInt(COLORS.length)];
    final Node node = new Node(id, parent, path, color);
    model.put(id, node);
    if (parent != null) {
      parent.children.add(node);
    }
    final SolrInputDocument doc = new SolrInputDocument("id", id);
    if (color != null) {
      doc.setField("color_s", color);
    }
    final int depth = path == null ? 0 : path.split("/").length - 1;
    if (depth < maxDepth) {
      final String label = depth == 0 ? "kids" : "subs";
      final String childPath = (path == null ? "" : path) + "/" + label;
      final int numChildren = random().nextInt(4);
      final List<SolrInputDocument> children = new ArrayList<>();
      for (int i = 0; i < numChildren; i++) {
        children.add(randomDoc(id + "." + i, node, childPath, maxDepth));
      }
      if (!children.isEmpty()) {
        doc.setField(label, children);
      }
    }
    return doc;
  }

  /** A root with 3 levels: 7 docs in total. */
  private static SolrInputDocument fixedTree(String id) {
    final SolrInputDocument root = new SolrInputDocument("id", id);
    final List<SolrInputDocument> kids = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      final SolrInputDocument kid = new SolrInputDocument("id", id + "." + i);
      kid.setField(
          "subs",
          List.of(
              new SolrInputDocument("id", id + "." + i + ".0"),
              new SolrInputDocument("id", id + "." + i + ".1")));
      kids.add(kid);
    }
    root.setField("kids", kids);
    return root;
  }

  private static long numFound(String q) throws Exception {
    return numFound(solrTestRule.getSolrClient(), q);
  }

  private static long numFound(SolrClient client, String q) throws Exception {
    return client.query(new SolrQuery(q)).getResults().getNumFound();
  }
}

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
package org.apache.solr.response.transform;

import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.LiteralValueSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.index.NoMergePolicyFactory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.response.ResultContext;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.FunctionQParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestValueSourceAugmenterPrefetchLimit extends SolrTestCaseJ4 {

  private static final int MAX_PREFETCH_SIZE = 1000;
  private static final int NUM_DOCS = MAX_PREFETCH_SIZE + 10;

  /** Commit this often while indexing so the index has several segments. */
  private static final int DOCS_PER_COMMIT = 300;

  /** No returned doc has a foo value that differs from its id. */
  private static final String FOO_MATCHES_ID =
      "count(//result/doc[str[@name='foo'] != str[@name='id']])=0";

  /** No returned doc has a foo value that differs from its val_i_p. */
  private static final String FOO_MATCHES_VAL =
      "count(//result/doc[int[@name='foo'] != int[@name='val_i_p']])=0";

  @BeforeClass
  public static void beforeTests() throws Exception {
    // Keep every flushed segment so the index spans several segments.
    systemSetPropertySolrTestsMergePolicyFactory(NoMergePolicyFactory.class.getName());
    initCore("solrconfig.xml", "schema.xml");

    // Index enough documents to exceed the prefetch limit. val_i_p descends as the docid
    // ascends, so sorting on it returns docs in reverse docid order across segments.
    for (int i = 0; i < NUM_DOCS; i++) {
      assertU(adoc("id", Integer.toString(i), "val_i_p", Integer.toString(NUM_DOCS - i)));
      if ((i + 1) % DOCS_PER_COMMIT == 0) {
        assertU(commit());
      }
    }
    assertU(commit());
  }

  @AfterClass
  public static void afterTests() {
    System.clearProperty(SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY);
  }

  public void testWithScoreBelowPrefetchLimit() {
    assertQ(
        "fl=foo:field(id),score with rows within prefetch limit",
        req(
            "q", "*:*",
            "fl", "id,foo:field(id),score",
            "rows", String.valueOf(MAX_PREFETCH_SIZE)),
        "//result[@numFound='" + NUM_DOCS + "']",
        "//result/doc/float[@name='score']",
        "count(//result/doc/str[@name='foo'])=" + MAX_PREFETCH_SIZE,
        FOO_MATCHES_ID);
  }

  public void testWithScoreAbovePrefetchLimit() {
    assertQ(
        "fl=foo:field(id),score with rows exceeding prefetch limit",
        req(
            "q", "*:*",
            "fl", "id,foo:field(id),score",
            "rows", String.valueOf(NUM_DOCS)),
        "//result[@numFound='" + NUM_DOCS + "']",
        "//result/doc/float[@name='score']",
        "count(//result/doc/str[@name='foo'])=" + NUM_DOCS,
        FOO_MATCHES_ID);
  }

  public void testWithScoreAbovePrefetchLimitUsingPreFetchDocsParam() throws Exception {
    assertQ(
        "fl=foo:field(id),score with custom preFetchDocs",
        req(
            "q", "*:*",
            "fl", "id,foo:field(id),score",
            "rows", String.valueOf(NUM_DOCS),
            "preFetchDocs", "2000"), // Set to 2000
        "//result[@numFound='" + NUM_DOCS + "']",
        "//result/doc/float[@name='score']",
        "count(//result/doc/str[@name='foo'])=" + NUM_DOCS,
        FOO_MATCHES_ID);
    assertPrefetchedCount("2000", NUM_DOCS); // the whole DocList is below the raised limit
  }

  /**
   * The production case: results that are not in docid order, over several segments, with a
   * forward-only docValues function. The prefetch must sort the docids and switch segments as it
   * crosses each docBase, or the per-segment FunctionValues reject out-of-order docs.
   */
  public void testOutOfDocIdOrderAcrossSegments() throws Exception {
    int numSegments =
        h.getCore().withSearcher(searcher -> searcher.getIndexReader().leaves().size());
    assertTrue("expected a multi-segment index, got " + numSegments, numSegments > 1);

    // both inside the prefetch limit and past it (where the per-doc fallback takes over)
    for (int rows : new int[] {500, NUM_DOCS}) {
      assertQ(
          "sort=val_i_p asc returns docs in reverse docid order, rows=" + rows,
          req(
              "q", "*:*",
              "sort", "val_i_p asc",
              "rows", String.valueOf(rows),
              "fl", "id,val_i_p,foo:field(val_i_p)"),
          "//result[@numFound='" + NUM_DOCS + "']",
          "count(//result/doc)=" + rows,
          "//result/doc[1]/str[@name='id'][.='" + (NUM_DOCS - 1) + "']",
          "//result/doc[1]/int[@name='val_i_p'][.='1']",
          "count(//result/doc/int[@name='foo'])=" + rows,
          FOO_MATCHES_VAL);
    }
  }

  public void testPreFetchDocsZeroComputesEveryValueOnDemand() throws Exception {
    assertQ(
        "preFetchDocs=0 disables prefetching",
        req(
            "q", "*:*",
            "fl", "id,foo:field(id)",
            "rows", "20",
            "preFetchDocs", "0"),
        "count(//result/doc/str[@name='foo'])=20",
        FOO_MATCHES_ID);
    assertPrefetchedCount("0", 0);
  }

  /**
   * Builds an augmenter whose request carries {@code preFetchDocs}, sets a context holding every
   * doc, and checks how many values were cached. The on-demand fallback produces the same output as
   * the cache, so the assertQ tests above cannot tell whether the limit was honored.
   */
  private void assertPrefetchedCount(String preFetchDocs, int expectedCached) throws Exception {
    int[] ids = new int[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      ids[i] = i;
    }
    DocList docs = new DocSlice(0, NUM_DOCS, ids, null, NUM_DOCS, 0f, TotalHits.Relation.EQUAL_TO);
    try (SolrQueryRequest req = req("preFetchDocs", preFetchDocs)) {
      QParser parser = QParser.getParser("field(id)", FunctionQParserPlugin.NAME, req);
      ValueSource vs = ((FunctionQuery) parser.getQuery()).getValueSource();
      ValueSourceAugmenter augmenter = new ValueSourceAugmenter("foo", parser, vs);
      h.getCore()
          .withSearcher(
              searcher -> {
                augmenter.setContext(new BasicResultContext(docs, null, searcher, null, req));
                if (expectedCached == 0) {
                  assertNull(augmenter.cachedValuesById);
                } else {
                  assertNotNull(augmenter.cachedValuesById);
                  assertEquals(expectedCached, augmenter.cachedValuesById.size());
                }
                return null;
              });
    }
  }

  public void testInvalidPreFetchDocsIsBadRequest() {
    for (String bad : new String[] {"abc", "-1"}) {
      assertQEx(
          "preFetchDocs=" + bad,
          req("q", "*:*", "fl", "foo:field(id)", "preFetchDocs", bad),
          SolrException.ErrorCode.BAD_REQUEST);
    }
  }

  public void testNullQParserUsesDefault() throws Exception {
    ValueSourceAugmenter augmenter =
        new ValueSourceAugmenter("foo", null, new LiteralValueSource("x"));
    int[] ids = new int[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      ids[i] = i;
    }
    DocList docs = new DocSlice(0, NUM_DOCS, ids, null, NUM_DOCS, 0f, TotalHits.Relation.EQUAL_TO);
    h.getCore()
        .withSearcher(
            searcher -> {
              augmenter.setContext(new BasicResultContext(docs, null, searcher, null, null));
              assertNotNull(augmenter.cachedValuesById);
              assertEquals(MAX_PREFETCH_SIZE, augmenter.cachedValuesById.size());
              return null;
            });
  }

  public void testSetContextWithoutDocListClearsCache() throws Exception {
    String extraId = Integer.toString(NUM_DOCS);
    SolrQueryRequest req = req("q", "*:*");
    RefCounted<SolrIndexSearcher> oldRef = h.getCore().getSearcher();
    RefCounted<SolrIndexSearcher> newRef = null;
    try {
      // numdocs() differs between the two searchers for the same docid
      QParser parser = QParser.getParser("numdocs()", FunctionQParserPlugin.NAME, req);
      ValueSource vs = ((FunctionQuery) parser.getQuery()).getValueSource();
      ValueSourceAugmenter augmenter = new ValueSourceAugmenter("foo", parser, vs);

      SolrIndexSearcher oldSearcher = oldRef.get();
      DocList docs = new DocSlice(0, 2, new int[] {0, 1}, null, 2, 0f, TotalHits.Relation.EQUAL_TO);
      augmenter.setContext(new BasicResultContext(docs, null, oldSearcher, null, req));
      assertNotNull(augmenter.cachedValuesById);

      assertU(adoc("id", extraId));
      assertU(commit());
      newRef = h.getCore().getSearcher();
      SolrIndexSearcher newSearcher = newRef.get();
      assertNotSame(oldSearcher, newSearcher);
      int expected = newSearcher.getIndexReader().numDocs();
      assertNotEquals(oldSearcher.getIndexReader().numDocs(), expected);

      augmenter.setContext(new NoDocListResultContext(newSearcher, req));
      SolrDocument doc = new SolrDocument();
      augmenter.transform(doc, 0, null);
      assertEquals(expected, doc.getFieldValue("foo"));
    } finally {
      if (newRef != null) {
        newRef.decref();
      }
      oldRef.decref();
      req.close();
      assertU(delI(extraId));
      assertU(commit());
    }
  }

  private static final class NoDocListResultContext extends ResultContext {
    private final SolrIndexSearcher searcher;
    private final SolrQueryRequest req;

    NoDocListResultContext(SolrIndexSearcher searcher, SolrQueryRequest req) {
      this.searcher = searcher;
      this.req = req;
    }

    @Override
    public DocList getDocList() {
      return null;
    }

    @Override
    public ReturnFields getReturnFields() {
      return null;
    }

    @Override
    public SolrIndexSearcher getSearcher() {
      return searcher;
    }

    @Override
    public Query getQuery() {
      return null;
    }

    @Override
    public SolrQueryRequest getRequest() {
      return req;
    }
  }
}

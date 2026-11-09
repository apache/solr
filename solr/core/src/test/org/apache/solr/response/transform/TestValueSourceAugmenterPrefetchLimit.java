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
import org.junit.BeforeClass;

public class TestValueSourceAugmenterPrefetchLimit extends SolrTestCaseJ4 {

  private static final int MAX_PREFETCH_SIZE = 1000;
  private static final int NUM_DOCS = MAX_PREFETCH_SIZE + 10;

  /** No returned doc has a foo value that differs from its id. */
  private static final String FOO_MATCHES_ID =
      "count(//result/doc[str[@name='foo'] != str[@name='id']])=0";

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema.xml");

    // Index enough documents to exceed the prefetch limit
    for (int i = 0; i < NUM_DOCS; i++) {
      assertU(adoc("id", Integer.toString(i)));
    }
    assertU(commit());
  }

  public void testWithScoreBelowPrefetchLimit() {
    assertQ(
        "fl=foo:field(id),score with rows within prefetch limit",
        req(
            "q", "*:*",
            "fl", "id,foo:field(id),score",
            "rows", String.valueOf(MAX_PREFETCH_SIZE)),
        "//result[@numFound='" + (MAX_PREFETCH_SIZE + 10) + "']",
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
            "rows", String.valueOf(MAX_PREFETCH_SIZE + 10)),
        "//result[@numFound='" + (MAX_PREFETCH_SIZE + 10) + "']",
        "//result/doc/float[@name='score']",
        "count(//result/doc/str[@name='foo'])=" + (MAX_PREFETCH_SIZE + 10),
        FOO_MATCHES_ID);
  }

  public void testWithScoreAbovePrefetchLimitUsingPreFetchDocsParam() {
    assertQ(
        "fl=foo:field(id),score with custom preFetchDocs",
        req(
            "q", "*:*",
            "fl", "id,foo:field(id),score",
            "rows", String.valueOf(MAX_PREFETCH_SIZE + 10),
            "preFetchDocs", "2000"), // Set to 2000
        "//result[@numFound='" + (MAX_PREFETCH_SIZE + 10) + "']",
        "//result/doc/float[@name='score']",
        "count(//result/doc/str[@name='foo'])=" + (MAX_PREFETCH_SIZE + 10),
        FOO_MATCHES_ID);
  }

  public void testPreFetchDocsZeroComputesEveryValueOnDemand() {
    assertQ(
        "preFetchDocs=0 disables prefetching",
        req(
            "q", "*:*",
            "fl", "id,foo:field(id)",
            "rows", "20",
            "preFetchDocs", "0"),
        "count(//result/doc/str[@name='foo'])=20",
        FOO_MATCHES_ID);
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

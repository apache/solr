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
package org.apache.solr.search;

import java.io.IOException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.index.NoMergePolicyFactory;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.TestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Tests for {@link MultiThreadedSearcher}. */
public class TestMultiThreadedSearcher extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    systemSetPropertySolrTestsMergePolicyFactory(NoMergePolicyFactory.class.getName());

    NodeConfig nodeConfig =
        new NodeConfig.NodeConfigBuilder("testNode", TEST_PATH())
            .setUseSchemaCache(Boolean.getBoolean("shareSchema"))
            .setUpdateShardHandlerConfig(UpdateShardHandlerConfig.TEST_DEFAULT)
            .setIndexSearcherExecutorThreads(4)
            .build();
    createCoreContainer(
        nodeConfig,
        new TestHarness.TestCoresLocator(
            DEFAULT_TEST_CORENAME,
            createTempDir("data").toAbsolutePath().toString(),
            "solrconfig-minimal.xml",
            "schema.xml"));
    h.coreName = DEFAULT_TEST_CORENAME;

    // Non-matching segments first, matching segment last.
    // This ensures different slices see different result counts during parallel search.
    for (int seg = 0; seg < 7; seg++) {
      for (int i = 0; i < 10; i++) {
        assertU(
            adoc(
                "id", String.valueOf(20000 + seg * 100 + i),
                "field1_s", "nomatchterm",
                "field4_t", "nomatchterm"));
      }
      assertU(commit());
    }

    // Matching segment last
    for (int i = 0; i < 10; i++) {
      assertU(
          adoc(
              "id", String.valueOf(10000 + i),
              "field1_s", "xyzrareterm",
              "field4_t", "xyzrareterm"));
    }
    assertU(commit());
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty(SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY);
  }

  public void testReRankWithMultiThreadedSearch() throws Exception {
    float fixedScore = 5.0f;
    h.getCore()
        .withSearcher(
            searcher -> {
              int numSegments = searcher.getTopReaderContext().leaves().size();
              assertTrue("Expected > 5 segments, got " + numSegments, numSegments > 5);
              assertTrue(
                  "Expected > 1 slice, got " + searcher.getSlices().length,
                  searcher.getSlices().length > 1);

              final QueryCommand cmd = new QueryCommand();
              cmd.setFlags(SolrIndexSearcher.GET_SCORES);
              cmd.setLen(10);
              cmd.setMultiThreaded(true);
              cmd.setSort(
                  new Sort(SortField.FIELD_SCORE, new SortField("id", SortField.Type.STRING)));
              cmd.setQuery(
                  new SimpleReRankQuery(
                      new TermQuery(new Term("field1_s", "xyzrareterm")), fixedScore));

              final QueryResult qr = searcher.search(cmd);

              assertTrue(qr.getDocList().matches() >= 1);
              final DocIterator iter = qr.getDocList().iterator();
              assertTrue(iter.hasNext());
              iter.next();
              assertEquals(fixedScore, iter.score(), 0);
              return null;
            });
  }

  private static final class SimpleReRankQuery extends RankQuery {

    private Query q;
    private final float reRankScore;

    SimpleReRankQuery(Query q, float reRankScore) {
      this.q = q;
      this.reRankScore = reRankScore;
    }

    @Override
    public Weight createWeight(IndexSearcher indexSearcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return q.createWeight(indexSearcher, scoreMode, boost);
    }

    @Override
    public void visit(QueryVisitor visitor) {
      q.visit(visitor);
    }

    @Override
    public boolean equals(Object obj) {
      return this == obj;
    }

    @Override
    public int hashCode() {
      return q.hashCode();
    }

    @Override
    public String toString(String field) {
      return q.toString(field);
    }

    @Override
    public TopDocsCollector<? extends ScoreDoc> getTopDocsCollector(
        int len, QueryCommand cmd, IndexSearcher searcher) throws IOException {
      return new ReRankCollector(
          len,
          len,
          new Rescorer() {
            @Override
            public TopDocs rescore(IndexSearcher searcher, TopDocs firstPassTopDocs, int topN) {
              for (ScoreDoc scoreDoc : firstPassTopDocs.scoreDocs) {
                scoreDoc.score = reRankScore;
              }
              return firstPassTopDocs;
            }

            @Override
            public Explanation explain(
                IndexSearcher searcher, Explanation firstPassExplanation, int docID) {
              return firstPassExplanation;
            }
          },
          cmd,
          searcher,
          null);
    }

    @Override
    public MergeStrategy getMergeStrategy() {
      return null;
    }

    @Override
    public RankQuery wrap(Query q) {
      this.q = q;
      return this;
    }
  }
}

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
package org.apache.solr.handler.component.combine;

import java.util.List;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.CollapsingQParserPlugin.CollapsingPostFilter;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortedIntDocSet;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryAndResponseCombinerTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema11.xml");
  }

  public static List<QueryResult> getQueryResults() {
    QueryResult r1 = new QueryResult();
    r1.setDocList(
        new DocSlice(
            0,
            2,
            new int[] {1, 2},
            new float[] {0.67f, 0, 0.62f},
            3,
            0.67f,
            TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
    r1.setDocSet(new SortedIntDocSet(new int[] {1, 2, 3}, 3));
    QueryResult r2 = new QueryResult();
    r2.setDocList(
        new DocSlice(
            0,
            1,
            new int[] {0},
            new float[] {0.87f},
            2,
            0.87f,
            TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
    r2.setDocSet(new SortedIntDocSet(new int[] {0, 1}, 2));
    return List.of(r1, r2);
  }

  /** Parses a collapse fq string and returns the CollapsingPostFilter. */
  private static CollapsingPostFilter parseCollapseFilter(String collapseFq) throws Exception {
    SolrQueryRequest solrReq = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrRequestInfo.setRequestInfo(new SolrRequestInfo(solrReq, rsp));
    try {
      Query q = QParser.getParser(collapseFq, solrReq).getQuery();
      assertTrue(
          "Expected CollapsingPostFilter but got " + q.getClass(),
          q instanceof CollapsingPostFilter);
      return (CollapsingPostFilter) q;
    } finally {
      SolrRequestInfo.clearRequestInfo();
      solrReq.close();
    }
  }

  /** Tests that simpleCombine without using collapse. */
  @Test
  public void simpleCombine() {
    QueryResult queryResult = QueryAndResponseCombiner.simpleCombine(getQueryResults(), null, null);
    assertEquals(3, queryResult.getDocList().size());
    assertEquals(4, queryResult.getDocSet().size());
  }

  /**
   * Tests that simpleCombine deduplicates by collapse field value when a collapse field is
   * provided. Uses the same query results from getQueryResults() but indexes docs so that Lucene
   * doc IDs 0 and 1 share the same collapse field value — only the higher-scoring doc should
   * survive.
   */
  @Test
  public void simpleCombineWithCollapseField() throws Exception {
    // getQueryResults() returns:
    //   r1: docs [1, 2] scores [0.67, 0.62]
    //   r2: docs [0]    scores [0.87]
    assertU(delQ("*:*"));
    assertU(adoc("id", "0", "group_ti_dv", "1"));
    assertU(adoc("id", "1", "group_ti_dv", "1"));
    assertU(adoc("id", "2", "group_ti_dv", "2"));
    assertU(commit());

    CollapsingPostFilter collapseFilter = parseCollapseFilter("{!collapse field=group_ti_dv}");

    RefCounted<SolrIndexSearcher> searcherRef = h.getCore().getSearcher();
    try {
      SolrIndexSearcher searcher = searcherRef.get();

      QueryResult combined =
          QueryAndResponseCombiner.simpleCombine(
              getQueryResults(), List.of(collapseFilter), searcher);

      // group=1: doc 0 (0.87) beats doc 1 (0.67) → keep doc 0
      // group=2: doc 2 (0.62) → kept
      assertEquals(
          "Expected 2 docs after collapse dedup (one per group value)",
          2,
          combined.getDocList().size());
      assertEquals(0, combined.getDocList().iterator().nextDoc());
    } finally {
      searcherRef.decref();
    }
  }

  /**
   * Tests that docs with missing collapse field values are kept (null policy pass-through) and not
   * considered duplicates.
   */
  @Test
  public void simpleCombineWithCollapseFieldNullPolicy() throws Exception {
    assertU(delQ("*:*"));
    assertU(adoc("id", "0", "group_ti_dv", "1"));
    assertU(adoc("id", "1", "group_ti_dv", "1"));
    assertU(adoc("id", "2")); // no collapse field
    assertU(commit());

    CollapsingPostFilter collapseFilter =
        parseCollapseFilter("{!collapse field=group_ti_dv nullPolicy=collapse sort='score asc'}");

    RefCounted<SolrIndexSearcher> searcherRef = h.getCore().getSearcher();
    try {
      SolrIndexSearcher searcher = searcherRef.get();

      QueryResult combined =
          QueryAndResponseCombiner.simpleCombine(
              getQueryResults(), List.of(collapseFilter), searcher);

      // group=1: doc 0 (0.87) > doc 1 (0.67) → keep doc 1
      // doc 2 has no group value → kept (null policy)
      assertEquals(
          "Expected 2 docs: one group head + one null-field doc", 2, combined.getDocList().size());
      assertEquals(1, combined.getDocList().iterator().nextDoc());
    } finally {
      searcherRef.decref();
    }
  }
}

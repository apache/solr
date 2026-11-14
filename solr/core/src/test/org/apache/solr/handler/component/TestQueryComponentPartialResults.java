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
package org.apache.solr.handler.component;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryCommand;
import org.apache.solr.search.QueryLimit;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.stats.StatsCache;
import org.apache.solr.util.TestInjection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests response publication when a limit is reached after documents have been selected. */
public class TestQueryComponentPartialResults extends SolrTestCase {
  private QueryLimit previousQueryTimeout;

  @Before
  public void saveQueryTimeout() {
    previousQueryTimeout = TestInjection.queryTimeout;
    assumeWorkingMockito();
  }

  @After
  public void restoreQueryTimeout() {
    TestInjection.queryTimeout = previousQueryTimeout;
  }

  @Test
  public void testPostSearchTimeoutOmitsDisallowedPartialResults() throws Exception {
    assertPostSearchTimeout(false);
  }

  @Test
  public void testPostSearchTimeoutPreservesAllowedPartialResults() throws Exception {
    assertPostSearchTimeout(true);
  }

  @Test
  public void testPostSearchTimeoutPreservesPartialResultsByDefault() throws Exception {
    assertPostSearchTimeout(null);
  }

  private void assertPostSearchTimeout(Boolean allowPartialResults) throws Exception {
    AtomicBoolean searchComplete = new AtomicBoolean();
    TestInjection.queryTimeout =
        new QueryLimit() {
          @Override
          public boolean shouldExit() {
            return searchComplete.get();
          }

          @Override
          public Object currentValue() {
            return searchComplete.get();
          }
        };
    DocList selectedDocuments =
        new DocSlice(0, 1, new int[] {42}, null, 1, 0, TotalHits.Relation.EQUAL_TO);
    QueryResult result = new QueryResult();
    result.setDocList(selectedDocuments);
    SolrIndexSearcher searcher = mock(SolrIndexSearcher.class);
    when(searcher.getStatsCache()).thenReturn(mock(StatsCache.class));
    when(searcher.search(any(QueryCommand.class)))
        .thenAnswer(
            invocation -> {
              assertFalse(SolrRequestInfo.getRequestInfo().getLimits().shouldExit());
              searchComplete.set(true);
              return result;
            });

    ModifiableSolrParams params = new ModifiableSolrParams();
    if (allowPartialResults != null) {
      params.set(CommonParams.PARTIAL_RESULTS, allowPartialResults);
    }
    try (SolrQueryRequestBase req =
        new SolrQueryRequestBase(null, params) {
          @Override
          public SolrIndexSearcher getSearcher() {
            return searcher;
          }
        }) {
      SolrQueryResponse rsp = new SolrQueryResponse();
      rsp.addResponseHeader(new SimpleOrderedMap<>());
      rsp.setReturnFields(new SolrReturnFields());
      ResponseBuilder rb = new ResponseBuilder(req, rsp, List.of(new QueryComponent()));
      rb.setQuery(new MatchAllDocsQuery());
      rb.setSortSpec(new SortSpec(null, List.of(), 10, 0));
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      try {
        new SearchHandler().processComponentsLocal(req, rsp, rb, null, rb.components);
        verify(searcher).search(any(QueryCommand.class));
        assertSame(selectedDocuments, rb.getResults().docList);
        assertTrue(SolrRequestInfo.getRequestInfo().getLimits().shouldExit());
        assertNull(rsp.getException());
        assertTrue(
            rsp.getResponseHeader()
                .get("partialResultsDetails")
                .toString()
                .contains("QueryComponent"));
        if (Boolean.FALSE.equals(allowPartialResults)) {
          assertEquals("omitted", rsp.getResponseHeader().get("partialResults"));
          assertTrue(
              "Disallowed partial results must not be published",
              rsp.getResponse() instanceof SolrDocumentList);
          SolrDocumentList documents = (SolrDocumentList) rsp.getResponse();
          assertEquals(0, documents.getNumFound());
          assertTrue(documents.isEmpty());
        } else {
          assertEquals(Boolean.TRUE, rsp.getResponseHeader().get("partialResults"));
          assertTrue(rsp.getResponse() instanceof ResultContext);
          DocList documents = ((ResultContext) rsp.getResponse()).getDocList();
          assertSame(selectedDocuments, documents);
          assertEquals(1, documents.matches());
          assertEquals(1, documents.size());
          assertEquals(42, documents.iterator().nextDoc());
        }
      } finally {
        SolrRequestInfo.clearRequestInfo();
      }
    }
  }
}

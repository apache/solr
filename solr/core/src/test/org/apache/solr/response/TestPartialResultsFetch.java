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
package org.apache.solr.response;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryLimit;
import org.apache.solr.search.QueryLimits;
import org.apache.solr.search.SolrDocumentFetcher;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.util.TestInjection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPartialResultsFetch extends SolrTestCase {
  private QueryLimit previousQueryTimeout;

  @Before
  public void saveQueryTimeout() {
    previousQueryTimeout = TestInjection.queryTimeout;
  }

  @After
  public void restoreQueryTimeout() {
    TestInjection.queryTimeout = previousQueryTimeout;
  }

  @Test
  public void testAlreadyTrippedLimitIsSuppressedOnlyDuringFetch() {
    TestInjection.queryTimeout = alwaysTrippedLimit();
    QueryLimits limits = newQueryLimits(new ModifiableSolrParams());
    assertTrue(limits.shouldExit());

    Object result = new Object();
    assertSame(
        result,
        limits.withPartialResultsFetch(
            () -> {
              assertFalse(limits.shouldExit());
              return result;
            }));
    assertTrue(limits.shouldExit());
  }

  @Test
  public void testFetchRestoresLimitAfterException() {
    TestInjection.queryTimeout = alwaysTrippedLimit();
    QueryLimits limits = newQueryLimits(new ModifiableSolrParams());
    assertTrue(limits.shouldExit());

    expectThrows(
        IllegalStateException.class,
        () ->
            limits.withPartialResultsFetch(
                () -> {
                  assertFalse(limits.shouldExit());
                  throw new IllegalStateException("fetch failed");
                }));
    assertTrue(limits.shouldExit());
  }

  @Test
  public void testNestedFetchScopesRemainSuppressedUntilOutermostReturns() {
    TestInjection.queryTimeout = alwaysTrippedLimit();
    QueryLimits limits = newQueryLimits(new ModifiableSolrParams());
    assertTrue(limits.shouldExit());

    limits.withPartialResultsFetch(
        () -> {
          assertFalse(limits.shouldExit());
          limits.withPartialResultsFetch(
              () -> {
                assertFalse(limits.shouldExit());
                return null;
              });
          assertFalse(limits.shouldExit());
          return null;
        });
    assertTrue(limits.shouldExit());
  }

  @Test
  public void testPartialResultsFalseDoesNotSuppressTrippedLimit() {
    TestInjection.queryTimeout = alwaysTrippedLimit();
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.PARTIAL_RESULTS, false);
    QueryLimits limits = newQueryLimits(params);
    assertTrue(limits.shouldExit());

    limits.withPartialResultsFetch(
        () -> {
          assertTrue(limits.shouldExit());
          return null;
        });
    assertTrue(limits.shouldExit());
  }

  @Test
  public void testLimitIsStillCheckedBeforeItHasTripped() {
    AtomicInteger checks = new AtomicInteger();
    TestInjection.queryTimeout =
        new QueryLimit() {
          @Override
          public boolean shouldExit() {
            checks.incrementAndGet();
            return false;
          }

          @Override
          public Object currentValue() {
            return checks.get();
          }
        };
    QueryLimits limits = newQueryLimits(new ModifiableSolrParams());

    limits.withPartialResultsFetch(
        () -> {
          assertFalse(limits.shouldExit());
          assertEquals(1, checks.get());
          return null;
        });
    assertFalse(limits.shouldExit());
    assertEquals(2, checks.get());
  }

  @Test
  public void testFetchSuppressionIsThreadLocal() throws Exception {
    TestInjection.queryTimeout = alwaysTrippedLimit();
    QueryLimits limits = newQueryLimits(new ModifiableSolrParams());
    assertTrue(limits.shouldExit());
    AtomicReference<Boolean> otherThreadShouldExit = new AtomicReference<>();

    limits.withPartialResultsFetch(
        () -> {
          assertFalse(limits.shouldExit());
          Thread otherThread = new Thread(() -> otherThreadShouldExit.set(limits.shouldExit()));
          otherThread.start();
          try {
            otherThread.join();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
          }
          assertEquals(Boolean.TRUE, otherThreadShouldExit.get());
          return null;
        });
    assertTrue(limits.shouldExit());
  }

  @Test
  public void testDocsStreamerFetchesAlreadySelectedDocumentDespiteTrippedLimit() throws Exception {
    assumeWorkingMockito();
    TestInjection.queryTimeout = alwaysTrippedLimit();
    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrQueryRequestBase req = new SolrQueryRequestBase(null, params);
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrRequestInfo requestInfo = new SolrRequestInfo(req, rsp);
    SolrRequestInfo.setRequestInfo(requestInfo);
    try {
      QueryLimits limits = requestInfo.getLimits();
      assertTrue(limits.shouldExit());

      SolrReturnFields returnFields = new SolrReturnFields();
      SolrDocument expectedDocument = new SolrDocument();
      SolrDocumentFetcher fetcher = mock(SolrDocumentFetcher.class);
      when(fetcher.solrDoc(42, returnFields))
          .thenAnswer(
              invocation -> {
                assertFalse(limits.shouldExit());
                return expectedDocument;
              });
      ResultContext context =
          new ResultContext() {
            @Override
            public DocList getDocList() {
              return oneDocumentList(42);
            }

            @Override
            public SolrReturnFields getReturnFields() {
              return returnFields;
            }

            @Override
            public SolrIndexSearcher getSearcher() {
              return null;
            }

            @Override
            public Query getQuery() {
              return null;
            }

            @Override
            public SolrQueryRequest getRequest() {
              return req;
            }

            @Override
            public SolrDocumentFetcher getDocFetcher() {
              return fetcher;
            }
          };

      DocsStreamer streamer = new DocsStreamer(context);
      assertTrue(streamer.hasNext());
      assertSame(expectedDocument, streamer.next());
      assertTrue(limits.shouldExit());
      verify(fetcher).solrDoc(42, returnFields);
    } finally {
      SolrRequestInfo.clearRequestInfo();
      req.close();
    }
  }

  private static QueryLimits newQueryLimits(ModifiableSolrParams params) {
    SolrQueryRequestBase req = new SolrQueryRequestBase(null, params);
    try {
      return new QueryLimits(req, new SolrQueryResponse());
    } finally {
      req.close();
    }
  }

  private static QueryLimit alwaysTrippedLimit() {
    return new QueryLimit() {
      @Override
      public boolean shouldExit() {
        return true;
      }

      @Override
      public Object currentValue() {
        return "tripped";
      }
    };
  }

  private static DocList oneDocumentList(int documentId) {
    return new DocSlice(0, 1, new int[] {documentId}, null, 1, 0, TotalHits.Relation.EQUAL_TO);
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SyntaxError;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for the metric and exception handling in {@link RequestHandlerBase} */
public class RequestHandlerBaseTest extends SolrTestCaseJ4 {

  private SolrCore solrCore;
  private CoreContainer coreContainer;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void initMocks() {
    solrCore = mock(SolrCore.class);
    coreContainer = mock(CoreContainer.class);
  }

  @Test
  public void testEachNonSolrExceptionIncrementsTheServerErrorCount() {
    final Exception e = new RuntimeException("Generic exception");
    final RequestHandlerBase.HandlerMetrics metrics = createHandlerMetrics();

    RequestHandlerBase.processErrorMetricsOnException(e, metrics);

    verify(metrics.numErrors).mark();
    verify(metrics.numServerErrors).mark();
    verifyNoInteractions(metrics.numClientErrors);
  }

  @Test
  public void test409SolrExceptionsSkipMetricRecording() {
    final Exception e = new SolrException(SolrException.ErrorCode.CONFLICT, "Conflict message");
    final RequestHandlerBase.HandlerMetrics metrics = createHandlerMetrics();

    RequestHandlerBase.processErrorMetricsOnException(e, metrics);

    verifyNoInteractions(metrics.numErrors);
    verifyNoInteractions(metrics.numServerErrors);
    verifyNoInteractions(metrics.numClientErrors);
  }

  @Test
  public void testEach4xxSolrExceptionIncrementsTheClientErrorCount() {
    final Exception e = new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Conflict message");
    final RequestHandlerBase.HandlerMetrics metrics = createHandlerMetrics();

    RequestHandlerBase.processErrorMetricsOnException(e, metrics);

    verify(metrics.numErrors).mark();
    verify(metrics.numClientErrors).mark();
    verifyNoInteractions(metrics.numServerErrors);
  }

  @Test
  public void testReceivedSyntaxErrorsAreWrappedIn400SolrException() {
    final SolrQueryRequest solrQueryRequest =
        new LocalSolrQueryRequest(solrCore, new ModifiableSolrParams()) {
          @Override
          public CoreContainer getCoreContainer() {
            return coreContainer;
          }
        };
    final Exception e = new SyntaxError("Some syntax error");

    final Exception normalized = RequestHandlerBase.normalizeReceivedException(solrQueryRequest, e);

    assertEquals(SolrException.class, normalized.getClass());
    final SolrException normalizedSolrException = (SolrException) normalized;
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, normalizedSolrException.code());
  }

  @Test
  public void testReceivedNonTragicNonSolrExceptionsAreNotModified() {
    final SolrQueryRequest solrQueryRequest =
        new LocalSolrQueryRequest(solrCore, new ModifiableSolrParams()) {
          @Override
          public CoreContainer getCoreContainer() {
            return coreContainer;
          }
        };
    final Exception e = new RuntimeException("Some generic, non-SolrException");

    final Exception normalized = RequestHandlerBase.normalizeReceivedException(solrQueryRequest, e);

    assertEquals(normalized, e);
  }

  @Test
  public void testTragicNonSolrExceptionsAreWrappedInA500SolrException() {
    when(coreContainer.checkTragicException(solrCore)).thenReturn(true);
    final SolrQueryRequest solrQueryRequest =
        new LocalSolrQueryRequest(solrCore, new ModifiableSolrParams()) {
          @Override
          public CoreContainer getCoreContainer() {
            return coreContainer;
          }
        };
    final Exception e = new RuntimeException("Some generic, non-SolrException");

    final Exception normalized = RequestHandlerBase.normalizeReceivedException(solrQueryRequest, e);

    assertEquals(SolrException.class, normalized.getClass());
    final SolrException normalizedSolrException = (SolrException) normalized;
    assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, normalizedSolrException.code());
  }

  @Test
  public void testIsInternalShardRequest() {
    final SolrQueryRequest solrQueryRequest =
        new LocalSolrQueryRequest(solrCore, new ModifiableSolrParams()) {
          @Override
          public CoreContainer getCoreContainer() {
            return coreContainer;
          }
        };

    assertFalse(RequestHandlerBase.isInternalShardRequest(solrQueryRequest));

    solrQueryRequest.setParams(new MapSolrParams(Map.of("isShard", "true")));
    assertTrue(RequestHandlerBase.isInternalShardRequest(solrQueryRequest));

    solrQueryRequest.setParams(new MapSolrParams(Map.of("distrib.from", "http://foo:1234/solr")));
    assertTrue(RequestHandlerBase.isInternalShardRequest(solrQueryRequest));
  }

  // Ideally we wouldn't need to use mocks here, but HandlerMetrics requires a SolrMetricsContext,
  // which
  //  requires a MetricsManager, which requires ...
  private RequestHandlerBase.HandlerMetrics createHandlerMetrics() {
    final SolrMetricsContext metricsContext = mock(SolrMetricsContext.class);
    when(metricsContext.timer(any(), any())).thenReturn(mock(Timer.class));
    when(metricsContext.meter(any(), any())).then(invocation -> mock(Meter.class));
    when(metricsContext.counter(any(), any())).thenReturn(mock(Counter.class));

    return new RequestHandlerBase.HandlerMetrics(metricsContext, "someBaseMetricPath");
  }
}

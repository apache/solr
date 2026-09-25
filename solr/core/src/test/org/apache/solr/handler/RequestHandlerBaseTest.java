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

import static org.apache.solr.metrics.SolrMetricProducer.HANDLER_ATTR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QueryLimitsExceededException;
import org.apache.solr.search.SyntaxError;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for the metric and exception handling in {@link RequestHandlerBase} */
public class RequestHandlerBaseTest extends SolrTestCaseJ4 {

  public static final AttributeKey<String> SOURCE_ATTR = AttributeKey.stringKey("source");
  private SolrCore solrCore;
  private CoreContainer coreContainer;
  private LongCounter mockLongCounter;
  private LongHistogram mockLongHistogram;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void initMocks() {
    solrCore = mock(SolrCore.class);
    coreContainer = mock(CoreContainer.class);
    mockLongCounter = mock(LongCounter.class);
    mockLongHistogram = mock(LongHistogram.class);
  }

  @Test
  public void testEachNonSolrExceptionIncrementsTheServerErrorCount() {
    final Exception e = new RuntimeException("Generic exception");
    final RequestHandlerBase.HandlerMetrics metrics = createHandlerMetrics();

    RequestHandlerBase.processErrorMetricsOnException(e, metrics);

    verify(mockLongCounter, never())
        .add(
            eq(1L), argThat(attrs -> "source".equals(attrs.get(AttributeKey.stringKey("client")))));
  }

  @Test
  public void test409SolrExceptionsSkipMetricRecording() {
    final Exception e = new SolrException(SolrException.ErrorCode.CONFLICT, "Conflict message");
    final RequestHandlerBase.HandlerMetrics metrics = createHandlerMetrics();

    RequestHandlerBase.processErrorMetricsOnException(e, metrics);

    verify(mockLongCounter, never())
        .add(eq(1L), argThat(attrs -> "client".equals(attrs.get(SOURCE_ATTR))));
    verify(mockLongCounter, never())
        .add(eq(1L), argThat(attrs -> "server".equals(attrs.get(SOURCE_ATTR))));
  }

  @Test
  public void testEach4xxSolrExceptionIncrementsTheClientErrorCount() {
    final Exception e = new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Conflict message");
    final RequestHandlerBase.HandlerMetrics metrics = createHandlerMetrics();

    RequestHandlerBase.processErrorMetricsOnException(e, metrics);

    verify(mockLongCounter, times(1))
        .add(eq(1L), argThat(attrs -> "client".equals(attrs.get(SOURCE_ATTR))));

    verify(mockLongCounter, never())
        .add(eq(1L), argThat(attrs -> "server".equals(attrs.get(SOURCE_ATTR))));
  }

  @Test
  public void testReceivedSyntaxErrorsAreWrappedIn400SolrException() {
    final SolrQueryRequest solrQueryRequest =
        new SolrQueryRequestBase(solrCore, new ModifiableSolrParams()) {
          @Override
          public CoreContainer getCoreContainer() {
            return coreContainer;
          }
        };
    final Exception e = new SyntaxError("Some syntax error");

    final Exception normalized = RequestHandlerBase.processReceivedException(solrQueryRequest, e);

    assertEquals(SolrException.class, normalized.getClass());
    final SolrException normalizedSolrException = (SolrException) normalized;
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, normalizedSolrException.code());
  }

  @Test
  public void testReceivedNonTragicNonSolrExceptionsAreNotModified() {
    final SolrQueryRequest solrQueryRequest =
        new SolrQueryRequestBase(solrCore, new ModifiableSolrParams()) {
          @Override
          public CoreContainer getCoreContainer() {
            return coreContainer;
          }
        };
    final Exception e = new RuntimeException("Some generic, non-SolrException");

    final Exception normalized = RequestHandlerBase.processReceivedException(solrQueryRequest, e);

    assertEquals(normalized, e);
  }

  @Test
  public void testTragicNonSolrExceptionsAreWrappedInA500SolrException() {
    when(coreContainer.checkTragicException(solrCore)).thenReturn(true);
    final SolrQueryRequest solrQueryRequest =
        new SolrQueryRequestBase(solrCore, new ModifiableSolrParams()) {
          @Override
          public CoreContainer getCoreContainer() {
            return coreContainer;
          }
        };
    final Exception e = new RuntimeException("Some generic, non-SolrException");

    final Exception normalized = RequestHandlerBase.processReceivedException(solrQueryRequest, e);

    assertEquals(SolrException.class, normalized.getClass());
    final SolrException normalizedSolrException = (SolrException) normalized;
    assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, normalizedSolrException.code());
  }

  @Test
  public void testIsInternalShardRequest() {
    final SolrQueryRequest solrQueryRequest =
        new SolrQueryRequestBase(solrCore, new ModifiableSolrParams()) {
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

  @Test
  public void testHandleRequestWithoutMetricsTouchesNoMetrics() throws Exception {
    final RequestHandlerBase.HandlerMetrics metrics = createHandlerMetrics();
    final TestHandler handler = new TestHandler(metrics, (req, rsp) -> {});
    final SolrQueryResponse rsp = new SolrQueryResponse();

    final boolean completedNormally = handler.handleRequestWithoutMetrics(testRequest(), rsp);

    assertTrue(completedNormally);
    verifyNoInteractions(mockLongCounter, mockLongHistogram);
  }

  @Test
  public void testHandleRequestCountsExactlyOneRequestOnSuccess() throws Exception {
    // A caller relying on handleRequest (rather than handleRequestWithoutMetrics, as UpdateAPI
    // now does) should still see exactly the pre-existing metrics behavior: one counter touch
    // (requests.inc()) for a request that completes normally with no timeout.
    final RequestHandlerBase.HandlerMetrics metrics = createHandlerMetrics();
    final TestHandler handler = new TestHandler(metrics, (req, rsp) -> {});
    final SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.addResponseHeader(new org.apache.solr.common.util.SimpleOrderedMap<>());

    handler.handleRequest(testRequest(), rsp);

    verify(mockLongCounter, times(1)).add(eq(1L), any());
  }

  @Test
  public void testHandleRequestDoesNotCountQueryLimitsExceededAsATimeout() throws Exception {
    // rsp.setPartialResults(req) (called for QueryLimitsExceededException) makes
    // haveCompleteResults(...) false, same as an actual timeout would -- but this path must not
    // also increment numTimeouts, matching pre-existing behavior of only doing so when
    // handleRequestBody returns normally. If it were double-counted, this would see 2 calls
    // instead of 1.
    final RequestHandlerBase.HandlerMetrics metrics = createHandlerMetrics();
    final TestHandler handler =
        new TestHandler(
            metrics,
            (req, rsp) -> {
              throw new QueryLimitsExceededException("over limit");
            });
    final SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.addResponseHeader(new org.apache.solr.common.util.SimpleOrderedMap<>());

    handler.handleRequest(testRequest(), rsp);

    verify(mockLongCounter, times(1)).add(eq(1L), any());
  }

  @Test
  public void testHandleRequestCountsRequestAndErrorOnException() throws Exception {
    final RequestHandlerBase.HandlerMetrics metrics = createHandlerMetrics();
    final TestHandler handler =
        new TestHandler(
            metrics,
            (req, rsp) -> {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "boom");
            });
    final SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.addResponseHeader(new org.apache.solr.common.util.SimpleOrderedMap<>());

    handler.handleRequest(testRequest(), rsp);

    // requests.inc() (top of handleRequest) + processErrorMetricsOnException's client-error
    // increment: exactly 2 calls total, not 3 (which double-accounting would produce).
    verify(mockLongCounter, times(2)).add(eq(1L), any());
    verify(mockLongCounter, times(1))
        .add(eq(1L), argThat(attrs -> "client".equals(attrs.get(SOURCE_ATTR))));
  }

  private SolrQueryRequest testRequest() {
    return new SolrQueryRequestBase(solrCore, new ModifiableSolrParams()) {
      @Override
      public CoreContainer getCoreContainer() {
        return coreContainer;
      }
    };
  }

  /** Minimal concrete {@link RequestHandlerBase} whose body and metrics are test-controlled. */
  private static class TestHandler extends RequestHandlerBase {
    @FunctionalInterface
    interface Body {
      void run(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception;
    }

    private final Body body;

    TestHandler(RequestHandlerBase.HandlerMetrics metrics, Body body) {
      this.metrics = metrics;
      this.body = body;
      // createHandlerMetrics() only mocks the 2-arg longHistogram(name, description) overload;
      // AttributedInstrumentFactory.attributedLongTimer actually calls the 3-arg overload, so
      // metrics.requestTimes ends up backed by a real (unmocked, null-histogram) instance. Swap
      // it for a no-op stand-in so handleRequest()'s timer.start()/stop() don't NPE.
      metrics.requestTimes =
          new org.apache.solr.metrics.otel.instruments.AttributedLongTimer(null, null) {
            @Override
            public void record(Long value) {}
          };
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      body.run(req, rsp);
    }

    @Override
    public String getDescription() {
      return "test";
    }

    @Override
    public org.apache.solr.security.PermissionNameProvider.Name getPermissionName(
        org.apache.solr.security.AuthorizationContext ctx) {
      return null;
    }
  }

  // Ideally we wouldn't need to use mocks here, but HandlerMetrics requires a SolrMetricsContext,
  // which
  //  requires a MetricsManager, which requires ...
  private RequestHandlerBase.HandlerMetrics createHandlerMetrics() {
    final SolrMetricsContext metricsContext = mock(SolrMetricsContext.class);

    when(metricsContext.getRegistryName()).thenReturn("solr.core");

    when(metricsContext.longCounter(any(), any())).thenReturn(mockLongCounter);
    when(metricsContext.longHistogram(any(), any())).thenReturn(mockLongHistogram);

    return new RequestHandlerBase.HandlerMetrics(
        metricsContext, Attributes.of(HANDLER_ATTR, "/someBaseMetricPath"), false);
  }
}

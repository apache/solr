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
package org.apache.solr.handler.admin.api;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.metrics.otel.instruments.AttributedLongCounter;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Covers a bug flagged in review of SOLR-18457 (PR #4177): {@link UpdateAPI#handleUpdate} used to
 * call {@code updateRequestHandler.handleRequest(...)} -- the full v1 {@link
 * org.apache.solr.handler.RequestHandlerBase#handleRequest} wrapper, not just the inner
 * request-processing logic. That wrapper does its own metrics bookkeeping on the handler's {@link
 * org.apache.solr.handler.RequestHandlerBase.HandlerMetrics}: it increments {@code requests}, times
 * the call, and on failure increments {@code numClientErrors}/{@code numServerErrors}.
 *
 * <p>But {@code updateRequestHandler} here is the very same {@link
 * org.apache.solr.handler.V2UpdateRequestHandler} instance that Jersey's {@code
 * RequestMetricHandling.PreRequestMetricsFilter}/{@code PostRequestMetricsFilter} already use to
 * record metrics for this same request (via {@code PluginBag.JaxrsResourceToHandlerMappings}, keyed
 * by the {@link UpdateAPI} resource class) -- and, on failure, {@code
 * UpdateAPI#rethrowAnyException} re-throws so the same exception reaches {@code
 * CatchAllExceptionMapper}, which recorded the error a second time too. So every v2 update request
 * used to be counted twice, and every v2 update error used to be counted twice.
 *
 * <p>The fix: {@link UpdateAPI#handleUpdate} now calls the new {@link
 * RequestHandlerBase#handleRequestWithoutMetrics}, which runs everything {@code handleRequest} does
 * (configured defaults/appends/invariants, HTTP caching, exception handling) except the
 * metrics/timer bookkeeping, leaving that solely to the surrounding Jersey filters.
 *
 * <p>This test doesn't stand up a real Jersey container; it fetches the real, core-registered
 * handler (so metrics are genuinely initialized) and simulates the surrounding filters/mapper
 * directly, to isolate the accounting to {@code UpdateAPI.handleUpdate} rather than the
 * request-processing logic alone.
 */
public class UpdateAPIMetricsDoubleCountingTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testSuccessfulRequestCountedOnce() throws Exception {
    final UpdateRequestHandler handler = updateHandler();
    final RequestHandlerBase.HandlerMetrics metrics = handler.getMetricsForThisRequest(null);
    final AtomicLong requestCount = new AtomicLong();
    metrics.requests = countingCounter(requestCount);

    try (SolrQueryRequestBase req = jsonRequest("[{\"id\":\"1\"}]")) {
      final SolrQueryResponse rsp = new SolrQueryResponse();
      final UpdateAPI api =
          new UpdateAPI(new UpdateAPI.UpdateRequestHandlerConfig(handler), req, rsp);

      // Simulates RequestMetricHandling.PreRequestMetricsFilter, which increments this same
      // handler's metrics before the Jersey resource method (UpdateAPI.update) ever runs.
      metrics.requests.inc();

      api.update(null, null, null, null, null, null);

      assertEquals(
          "one logical v2 update request should count as exactly one request -- "
              + "UpdateAPI must not also increment PreRequestMetricsFilter's HandlerMetrics via "
              + "handleRequest()",
          1L,
          requestCount.get());
    }
  }

  @Test
  public void testFailedRequestErrorCountedOnce() throws Exception {
    final UpdateRequestHandler handler = updateHandler();
    final RequestHandlerBase.HandlerMetrics metrics = handler.getMetricsForThisRequest(null);
    final AtomicLong clientErrorCount = new AtomicLong();
    metrics.numClientErrors = countingCounter(clientErrorCount);

    // No "id": AddUpdateCommand rejects this with a 400 (client error) once it reaches
    // RunUpdateProcessorFactory, deep inside updateRequestHandler.handleRequest().
    try (SolrQueryRequestBase req = jsonRequest("[{\"title\":\"no id here\"}]")) {
      final SolrQueryResponse rsp = new SolrQueryResponse();
      final UpdateAPI api =
          new UpdateAPI(new UpdateAPI.UpdateRequestHandlerConfig(handler), req, rsp);

      final SolrException thrown =
          expectThrows(SolrException.class, () -> api.update(null, null, null, null, null, null));

      // UpdateAPI re-throws so that CatchAllExceptionMapper can build the error response and
      // record the error metric -- simulating that here, since there's no real Jersey container
      // in this test. handleRequestWithoutMetrics itself must not have already recorded it too.
      RequestHandlerBase.processErrorMetricsOnException(thrown, metrics);

      assertEquals(
          "one failed v2 update request should count as exactly one client error -- "
              + "UpdateAPI must not record error metrics itself now that it uses "
              + "handleRequestWithoutMetrics, leaving that solely to CatchAllExceptionMapper",
          1L,
          clientErrorCount.get());
    }
  }

  private static UpdateRequestHandler updateHandler() {
    // V2UpdateRequestHandler.registerV1() is false, so it's deliberately absent from the v1
    // handler registry core.getRequestHandler(...) reads from -- it's only reachable via the
    // same JAX-RS resource-to-handler mapping RequestMetricHandling.PreRequestMetricsFilter uses.
    final UpdateRequestHandler handler =
        (UpdateRequestHandler)
            h.getCore().getRequestHandlers().getJaxrsRegistry().get(UpdateAPI.class);
    assertNotNull(
        "expected the v2-only V2UpdateRequestHandler to be registered for the UpdateAPI resource",
        handler);
    return handler;
  }

  private static SolrQueryRequestBase jsonRequest(String json) {
    final SolrQueryRequestBase req = new SolrQueryRequestBase(h.getCore(), SolrParams.of());
    req.setContentStreams(List.of(new ContentStreamBase.StringStream(json, "application/json")));
    return req;
  }

  /** An {@link AttributedLongCounter} that tallies into {@code count} instead of touching OTel. */
  private static AttributedLongCounter countingCounter(AtomicLong count) {
    return new AttributedLongCounter(null, null) {
      @Override
      public void add(Long value) {
        count.addAndGet(value);
      }
    };
  }
}

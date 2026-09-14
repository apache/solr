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

package org.apache.solr.jersey;

import static org.apache.solr.jersey.RequestContextKeys.HANDLER_METRICS;
import static org.apache.solr.jersey.RequestContextKeys.TIMER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.metrics.SolrMetricsContext;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link RequestMetricHandling} */
public class RequestMetricHandlingTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Test
  public void testPostRequestMetricsFilterToleratesBeingInvokedTwice() throws Exception {
    // Jersey can re-invoke response filters a second time when an exception occurs while
    // building the first response (e.g. via CatchAllExceptionMapper); the second invocation must
    // not try to stop an already-stopped timer.
    final var mockRequestContext = mock(ContainerRequestContext.class);
    final var mockResponseContext = mock(ContainerResponseContext.class);
    final RequestHandlerBase.HandlerMetrics metrics = createHandlerMetrics();
    final var timer = metrics.requestTimes.start();

    when(mockRequestContext.getPropertyNames()).thenReturn(Set.of());
    when(mockRequestContext.getProperty(HANDLER_METRICS)).thenReturn(metrics);
    when(mockRequestContext.getProperty(TIMER)).thenReturn(timer);

    final var filter = new RequestMetricHandling.PostRequestMetricsFilter();
    filter.filter(mockRequestContext, mockResponseContext);

    // Simulate Jersey clearing the property after the first stop, as our fix does.
    when(mockRequestContext.getProperty(TIMER)).thenReturn(null);

    // A second invocation must not throw (would previously throw AssertionError with
    // assertions enabled, from RTimer.stop() being called on an already-stopped timer).
    filter.filter(mockRequestContext, mockResponseContext);
  }

  @Test
  public void testPostRequestMetricsFilterNoOpsWithoutMetrics() throws Exception {
    final var mockRequestContext = mock(ContainerRequestContext.class);
    final var mockResponseContext = mock(ContainerResponseContext.class);
    when(mockRequestContext.getPropertyNames()).thenReturn(Set.of());
    when(mockRequestContext.getProperty(HANDLER_METRICS)).thenReturn(null);

    new RequestMetricHandling.PostRequestMetricsFilter()
        .filter(mockRequestContext, mockResponseContext);
  }

  private RequestHandlerBase.HandlerMetrics createHandlerMetrics() {
    final SolrMetricsContext metricsContext = mock(SolrMetricsContext.class);
    final LongCounter mockLongCounter = mock(LongCounter.class);
    final LongHistogram mockLongHistogram = mock(LongHistogram.class);

    when(metricsContext.getRegistryName()).thenReturn("solr.core");
    when(metricsContext.longCounter(any(), any())).thenReturn(mockLongCounter);
    when(metricsContext.longCounter(any(), any(), any())).thenReturn(mockLongCounter);
    when(metricsContext.longHistogram(any(), any())).thenReturn(mockLongHistogram);
    when(metricsContext.longHistogram(any(), any(), any())).thenReturn(mockLongHistogram);

    return new RequestHandlerBase.HandlerMetrics(
        metricsContext, Attributes.of(AttributeKey.stringKey("source"), "test"), false);
  }
}

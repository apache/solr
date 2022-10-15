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
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_REQUEST;
import static org.apache.solr.jersey.RequestContextKeys.TIMER;

import com.codahale.metrics.Timer;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import org.apache.solr.core.PluginBag;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A request and response filter used to initialize and report per-request metrics.
 *
 * <p>Currently, JAX-RS v2 APIs rely on a {@link
 * org.apache.solr.handler.RequestHandlerBase.HandlerMetrics} instance from an associated request
 * handler.
 */
public class RequestMetricHandling {

  /**
   * Sets up the metrics-context for individual requests
   *
   * <p>Looks up the requestHandler associated with the particular Jersey request and attaches its
   * {@link org.apache.solr.handler.RequestHandlerBase.HandlerMetrics} to the request context to be
   * manipulated by other pre- and post-request filters in this chain.
   */
  public static class PreRequestMetricsFilter implements ContainerRequestFilter {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Context private ResourceInfo resourceInfo;

    private PluginBag.JerseyMetricsLookupRegistry beanRegistry;

    @Inject
    public PreRequestMetricsFilter(PluginBag.JerseyMetricsLookupRegistry beanRegistry) {
      this.beanRegistry = beanRegistry;
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
      final RequestHandlerBase handlerBase = beanRegistry.get(resourceInfo.getResourceClass());
      if (handlerBase == null) {
        log.debug("No handler found for request {}", requestContext);
        return;
      }

      final SolrQueryRequest solrQueryRequest =
          (SolrQueryRequest) requestContext.getProperty(SOLR_QUERY_REQUEST);
      final RequestHandlerBase.HandlerMetrics metrics =
          handlerBase.getMetricsForThisRequest(solrQueryRequest);

      requestContext.setProperty(HANDLER_METRICS, metrics);
      requestContext.setProperty(TIMER, metrics.requestTimes.time());
      metrics.requests.inc();
    }
  }

  /** Adjusts post-request metrics (timing, etc.)for individual Jersey requests. */
  public static class PostRequestMetricsFilter implements ContainerResponseFilter {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void filter(
        ContainerRequestContext requestContext, ContainerResponseContext responseContext)
        throws IOException {
      if (requestContext.getPropertyNames().contains(RequestContextKeys.NOT_FOUND_FLAG)) {
        return;
      }

      final RequestHandlerBase.HandlerMetrics metrics =
          (RequestHandlerBase.HandlerMetrics) requestContext.getProperty(HANDLER_METRICS);
      if (metrics == null) return;

      // Increment the timeout count if responseHeader indicates a timeout
      if (responseContext.hasEntity()
          && SolrJerseyResponse.class.isInstance(responseContext.getEntity())) {
        final SolrJerseyResponse response = (SolrJerseyResponse) responseContext.getEntity();
        if (Boolean.TRUE.equals(response.responseHeader.partialResults)) {
          metrics.numTimeouts.mark();
        }
      } else {
        log.debug("Skipping partialResults check because entity was not SolrJerseyResponse");
      }

      final Timer.Context timer = (Timer.Context) requestContext.getProperty(TIMER);
      metrics.totalTime.inc(timer.stop());
    }
  }
}

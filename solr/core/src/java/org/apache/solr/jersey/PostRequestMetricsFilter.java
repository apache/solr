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

import static org.apache.solr.jersey.RequestContextConstants.HANDLER_METRICS_KEY;
import static org.apache.solr.jersey.RequestContextConstants.TIMER_KEY;

import com.codahale.metrics.Timer;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import org.apache.solr.handler.RequestHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adjusts post-request metrics for individual Jersey requests. */
public class PostRequestMetricsFilter implements ContainerResponseFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void filter(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    final RequestHandlerBase.HandlerMetrics metrics =
        (RequestHandlerBase.HandlerMetrics) requestContext.getProperty(HANDLER_METRICS_KEY);
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

    final Timer.Context timer = (Timer.Context) requestContext.getProperty(TIMER_KEY);
    metrics.totalTime.inc(timer.stop());
  }
}

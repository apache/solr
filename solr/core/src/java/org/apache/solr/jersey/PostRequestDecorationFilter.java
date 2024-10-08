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

import static org.apache.solr.jersey.PostRequestDecorationFilter.PRIORITY;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_REQUEST;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies standard post-processing decorations to a {@link SolrJerseyResponse} that are needed on
 * all responses.
 *
 * @see SolrCore#postDecorateResponse(SolrRequestHandler, SolrQueryRequest, SolrQueryResponse)
 */
@Priority(PRIORITY)
public class PostRequestDecorationFilter implements ContainerResponseFilter {
  public static final int PRIORITY = 10;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void filter(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    if (requestContext.getPropertyNames().contains(RequestContextKeys.NOT_FOUND_FLAG)) {
      return;
    }
    final SolrQueryRequest solrQueryRequest =
        (SolrQueryRequest) requestContext.getProperty(SOLR_QUERY_REQUEST);
    if (!responseContext.hasEntity()
        || !SolrJerseyResponse.class.isInstance(responseContext.getEntity())) {
      log.debug("Skipping QTime assignment because response was not a SolrJerseyResponse");
      return;
    }

    final SolrJerseyResponse response = (SolrJerseyResponse) responseContext.getEntity();
    response.responseHeader.qTime = Math.round(solrQueryRequest.getRequestTimer().getTime());
  }
}

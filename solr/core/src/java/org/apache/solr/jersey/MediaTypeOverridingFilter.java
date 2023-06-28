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

package org.apache.solr.jersey;

import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_REQUEST;

import java.io.IOException;
import java.util.List;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.handler.admin.ZookeeperReadAPI;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;

// TODO Deprecate or remove support for the 'wt' parameter in the v2 APIs in favor of the more
//  HTTP-compliant 'Accept' header
/** Overrides the content-type of the response based on an optional user-provided 'wt' parameter */
public class MediaTypeOverridingFilter implements ContainerResponseFilter {

  private static final List<Class<? extends JerseyResource>> EXEMPTED_RESOURCES =
      List.of(ZookeeperReadAPI.class);

  @Context private ResourceInfo resourceInfo;

  @Override
  public void filter(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {

    // Solr has historically ignored 'wt' for client or server error responses, so maintain that
    // behavior here for compatibility.
    if (responseContext.getStatus() >= 400) {
      return;
    }

    // Some endpoints have their own media-type logic and opt out of the overriding behavior this
    // filter provides.
    if (EXEMPTED_RESOURCES.contains(resourceInfo.getResourceClass())) {
      return;
    }

    final SolrQueryRequest solrQueryRequest =
        (SolrQueryRequest) requestContext.getProperty(SOLR_QUERY_REQUEST);
    final String mediaType = V2ApiUtils.getMediaTypeFromWtParam(solrQueryRequest, null);
    if (mediaType != null) {
      responseContext.getHeaders().putSingle(CONTENT_TYPE, mediaType);
    }
  }
}

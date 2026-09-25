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

import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import java.io.IOException;
import java.util.List;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.handler.admin.ZookeeperRead;

/**
 * Defaults the response content-type to JSON, unless the client requested something else via the
 * (HTTP-compliant) 'Accept' header.
 *
 * <p>v2 APIs do not honor the legacy 'wt' parameter for response-format selection -- 'Accept' is
 * the only supported mechanism.
 */
public class MediaTypeOverridingFilter implements ContainerResponseFilter {

  private static final List<Class<? extends JerseyResource>> EXEMPTED_RESOURCES =
      List.of(ZookeeperRead.class);

  @Context private ResourceInfo resourceInfo;

  @Override
  public void filter(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {

    // Solr has historically ignored client/server error responses here, so maintain that
    // behavior for compatibility.
    if (responseContext.getStatus() >= 400) {
      return;
    }

    // Some endpoints have their own media-type logic and opt out of the overriding behavior this
    // filter provides.
    if (resourceInfo.getResourceClass() == null
        || EXEMPTED_RESOURCES.contains(resourceInfo.getResourceClass())) {
      return;
    }

    if (!requestContext.getHeaders().containsKey(ACCEPT)
        || "*/*".equals(requestContext.getHeaderString(ACCEPT))) { // Default response to json
      responseContext.getHeaders().putSingle(CONTENT_TYPE, APPLICATION_JSON);
    }
    // Else, obey the user-provided 'Accept' header
  }
}

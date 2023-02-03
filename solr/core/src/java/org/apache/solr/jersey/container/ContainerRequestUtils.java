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

package org.apache.solr.jersey.container;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.Enumeration;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.SecurityContext;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.internal.ContainerUtils;
import org.glassfish.jersey.server.spi.ContainerResponseWriter;

/**
 * Utility methods for creating and populating a {@link
 * org.glassfish.jersey.server.ContainerRequest} for use with Jersey {@link
 * org.glassfish.jersey.server.ApplicationHandler}s
 */
public class ContainerRequestUtils {
  private ContainerRequestUtils() {
    /* Private ctor prevents instantiation */
  }

  // We don't rely on any of Jersey's authc/z features so we pass in this empty context for
  // all requests.
  public static final SecurityContext DEFAULT_SECURITY_CONTEXT =
      new SecurityContext() {
        @Override
        public boolean isUserInRole(String role) {
          return false;
        }

        @Override
        public boolean isSecure() {
          return false;
        }

        @Override
        public Principal getUserPrincipal() {
          return null;
        }

        @Override
        public String getAuthenticationScheme() {
          return null;
        }
      };

  /**
   * Creates a {@link ContainerRequest}
   *
   * <p>Implementation guided by code in 'jersey-container-jetty-http's JettyHttpContainer class.
   */
  public static ContainerRequest createContainerRequest(
      HttpServletRequest httpServletRequest,
      HttpServletResponse httpServletResponse,
      Configuration appConfig) {
    final ContainerResponseWriter responseWriter =
        new JettyBridgeResponseWriter(httpServletResponse);
    try {
      final URI baseUri = getBaseUri(httpServletRequest);
      final URI requestUri = getRequestUri(httpServletRequest, baseUri);
      final ContainerRequest requestContext =
          new ContainerRequest(
              baseUri,
              requestUri,
              httpServletRequest.getMethod(),
              DEFAULT_SECURITY_CONTEXT,
              new MapPropertiesDelegate(),
              appConfig);
      requestContext.setEntityStream(httpServletRequest.getInputStream());
      final Enumeration<String> headerNames = httpServletRequest.getHeaderNames();
      while (headerNames.hasMoreElements()) {
        final String headerName = headerNames.nextElement();
        String headerValue = httpServletRequest.getHeader(headerName);
        requestContext.headers(headerName, headerValue == null ? "" : headerValue);
      }
      requestContext.setWriter(responseWriter);
      return requestContext;
    } catch (Exception e) {
      // TODO Should we handle URISyntaxException any differently here?
      throw new RuntimeException(e);
    }
  }

  private static URI getBaseUri(HttpServletRequest httpServletRequest) {
    try {
      return new URI(
          httpServletRequest.getScheme(),
          null,
          httpServletRequest.getServerName(),
          httpServletRequest.getServerPort(),
          "/",
          null,
          null);
    } catch (final URISyntaxException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  private static URI getRequestUri(HttpServletRequest httpServletRequest, URI baseUri)
      throws URISyntaxException {
    final String serverAddress = getServerAddress(baseUri);
    String uri = httpServletRequest.getRequestURI();
    // Jersey is only used for v2 APIs so we have no need of the janky v2 suffixing (and it impedes
    // matching) - remove if present.
    uri = uri.replace("/solr/____v2", "");

    final String queryString = httpServletRequest.getQueryString();
    if (queryString != null) {
      uri = uri + "?" + ContainerUtils.encodeUnsafeCharacters(queryString);
    }

    return new URI(serverAddress + uri);
  }

  private static String getServerAddress(URI baseUri) {
    String serverAddress = baseUri.toString();
    if (serverAddress.charAt(serverAddress.length() - 1) == '/') {
      return serverAddress.substring(0, serverAddress.length() - 1);
    }
    return serverAddress;
  }
}

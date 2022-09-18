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

import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletResponse;
import org.glassfish.jersey.server.ContainerException;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.spi.ContainerResponseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ResponseWriter which copies that output of JAX-RS computation over to {@link
 * HttpServletResponse} object used by the Jetty server.
 *
 * <p>Since we're not running Jersey as a full server or as a servlet that Jetty knows about, we
 * need to connect the response Jersey outputs with the actual objects that Jetty sends back to
 * clients.
 *
 * <p>Inspired and partially copied from the JettyHttpContainer.ResponseWriter class available in
 * the jersey-container-jetty-http artifact.
 */
public class JettyBridgeResponseWriter implements ContainerResponseWriter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final HttpServletResponse httpServletResponse;

  public JettyBridgeResponseWriter(HttpServletResponse httpServletResponse) {
    this.httpServletResponse = httpServletResponse;
  }

  @Override
  public OutputStream writeResponseStatusAndHeaders(
      final long contentLength, final ContainerResponse context) throws ContainerException {
    final javax.ws.rs.core.Response.StatusType statusInfo = context.getStatusInfo();
    httpServletResponse.setStatus(statusInfo.getStatusCode());

    if (contentLength != -1 && contentLength < Integer.MAX_VALUE) {
      httpServletResponse.setContentLength((int) contentLength);
    }
    for (final Map.Entry<String, List<String>> e : context.getStringHeaders().entrySet()) {
      for (final String value : e.getValue()) {
        httpServletResponse.addHeader(e.getKey(), value);
      }
    }

    try {
      return httpServletResponse.getOutputStream();
    } catch (final IOException ioe) {
      throw new ContainerException("Error during writing out the response headers.", ioe);
    }
  }

  @Override
  public boolean suspend(
      final long timeOut,
      final TimeUnit timeUnit,
      final ContainerResponseWriter.TimeoutHandler timeoutHandler) {
    // I don't think we can (or should) do anything here as the surrounding servlet filter will
    // handle this on its own.
    return true;
  }

  @Override
  public void setSuspendTimeout(final long timeOut, final TimeUnit timeUnit)
      throws IllegalStateException {
    // I don't think we can (or should) do anything here as the surrounding servlet filter will
    // handle this on its own.
  }

  @Override
  public void commit() {
    // I don't think we can (or should) do anything here as the surrounding servlet filter will
    // handle this on its own.
  }

  @Override
  public void failure(final Throwable error) {
    // We don't (or shouldn't) do anything here, as the surrounding servlet filter handles this on
    // its own.
  }

  @Override
  public boolean enableResponseBuffering() {
    return false;
  }
}

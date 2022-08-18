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

package org.apache.solr.core;

import org.eclipse.jetty.http.HttpHeader;
import org.glassfish.jersey.server.ContainerException;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.spi.ContainerResponseWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A ResponseWriter which copies that output of JAX-RS computation over to {@link HttpServletResponse} object used by the Jetty server.
 *
 * Since we're not running Jersey as a full server or as a servlet that Jetty knows about, we need to connect the response Jersey
 * outputs with the actual objects that Jetty sends back to clients.
 */
public class JettyBasedResponseWriter implements ContainerResponseWriter {

    private final HttpServletRequest req;
    private final HttpServletResponse rsp;

    public JettyBasedResponseWriter(HttpServletRequest req, HttpServletResponse rsp) {
        this.req = req;
        this.rsp = rsp;
    }

    @Override
    public OutputStream writeResponseStatusAndHeaders(long contentLength, ContainerResponse responseContext) throws ContainerException {
        final javax.ws.rs.core.Response.StatusType statusInfo = responseContext.getStatusInfo();
        rsp.setStatus(statusInfo.getStatusCode());

        if (contentLength != -1 && contentLength < Integer.MAX_VALUE) {
            rsp.setHeader(HttpHeader.CONTENT_LENGTH.asString(), Long.toString(contentLength));
        }

        for (final Map.Entry<String, List<String>> e : responseContext.getStringHeaders().entrySet()) {
            for (final String value : e.getValue()) {
                rsp.addHeader(e.getKey(), value);
            }
        }

        try {
            return rsp.getOutputStream();
        } catch (final IOException ioe) {
            throw new ContainerException("Error during writing out the response headers.", ioe);
        }
    }

    @Override
    public boolean suspend(long timeOut, TimeUnit timeUnit, TimeoutHandler timeoutHandler) {
        // TODO Do I need to do anything here?  The surrounding servlet filter will handle closing the wrapped
        //  HttpServletResponse on its own.
        return true;
    }

    @Override
    public void setSuspendTimeout(long timeOut, TimeUnit timeUnit) throws IllegalStateException {
        // TODO Do I need to do anything here?  The surrounding servlet filter will handle closing the wrapped
        //  HttpServletResponse on its own.
    }

    @Override
    public void commit() {
        // TODO Do I need to do anything here?  The surrounding servlet filter will handle closing the wrapped
        //  HttpServletResponse on its own.
    }

    @Override
    public void failure(Throwable error) {
        // TODO Do I need to do anything here?  The surrounding servlet filter will handle any errors, stream closing, etc.
    }

    @Override
    public boolean enableResponseBuffering() {
        return false;
    }
}

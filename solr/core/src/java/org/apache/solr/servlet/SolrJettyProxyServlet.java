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

package org.apache.solr.servlet;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.solr.util.tracing.TraceUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.ee10.proxy.ProxyServlet;

/**
 * Solr customization of Jetty {@link ProxyServlet}, used when a node needs to forward the request
 * to another.
 */
public class SolrJettyProxyServlet extends ProxyServlet {
  static final String PROXY_TO_ATTRIBUTE = SolrJettyProxyServlet.class.getName() + ".proxyTo";

  /*
  Should we use an HttpClient created from HttpSolrClientProvider?  The ProxyServlet creates one
  with a lot of particular settings, so here we're assuming ProxyServlet knows best but maybe
  it should be more configurable / hybrid.
   */

  @Override
  protected HttpClient getHttpClient() {
    return super.getHttpClient();
  }

  @Override
  protected String rewriteTarget(HttpServletRequest clientRequest) {
    return (String) clientRequest.getAttribute(PROXY_TO_ATTRIBUTE);
  }

  @Override
  protected void copyRequestHeaders(HttpServletRequest clientRequest, Request proxyRequest) {
    super.copyRequestHeaders(clientRequest, proxyRequest);

    // FYI see InstrumentedHttpListenerFactory
    TraceUtils.injectTraceContext(proxyRequest);
    // TODO client spans.  See OTEL agent's approach:
    // https://github.com/open-telemetry/opentelemetry-java-instrumentation/tree/main/instrumentation/jetty-httpclient/jetty-httpclient-12.0
  }
}

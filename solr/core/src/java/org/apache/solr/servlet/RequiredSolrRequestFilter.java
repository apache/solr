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

import static org.apache.solr.servlet.ServletUtils.closeShield;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.logging.MDCSnapshot;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.util.RTimerTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet Filter to set up and tear down a various things that downstream code in solr relies on.
 * It is expected that solr will fail to function as intended without the conditions initialized in
 * this filter. Before adding anything to this filter ask yourself if a user could run without the
 * feature you are adding (either with an alternative implementation or without it at all to reduce
 * cpu/memory/dependencies). If it is not required, it should have its own separate filter. Also,
 * please include a comment indicating why the thing added here is required.
 */
public class RequiredSolrRequestFilter extends CoreContainerAwareHttpFilter {

  // Best to put constant here because solr is not supposed to be functional (or compile)
  // without this filter.
  public static final String CORE_CONTAINER_REQUEST_ATTRIBUTE = "org.apache.solr.CoreContainer";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  @SuppressForbidden(
      reason =
          "Set the thread contextClassLoader for all 3rd party dependencies that we cannot control")
  protected void doFilter(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws IOException, ServletException {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    // this autocloseable is here to invoke MDCSnapshot.close() which restores captured state
    try (var mdcSnapshot = MDCSnapshot.create()) {

      // MDC logging *shouldn't* be required but currently is, see SOLR-18050.
      log.trace("MDC snapshot recorded {}", mdcSnapshot); // avoid both compiler and ide warning.
      MDCLoggingContext.reset();
      MDCLoggingContext.setNode(getCores());

      // This is required to accommodate libraries that (annoyingly) use
      // Thread.currentThread().getContextClassLoader()
      Thread.currentThread().setContextClassLoader(getCores().getResourceLoader().getClassLoader());

      // set a request timer which can be reused by requests if needed
      // Request Timer is required for QueryLimits functionality as well as
      // timing our requests.
      req.setAttribute(SolrRequestParsers.REQUEST_TIMER_SERVLET_ATTRIBUTE, new RTimerTree());

      // put the core container in request attribute
      // This is required for the LoadAdminUiServlet class. Removing it will cause 404
      req.setAttribute(CORE_CONTAINER_REQUEST_ATTRIBUTE, getCores());

      // we want to prevent any attempts to close our request or response prematurely
      chain.doFilter(closeShield(req), closeShield(res));
    } finally {
      // cleanups for above stuff
      MDCLoggingContext.reset();
      Thread.currentThread().setContextClassLoader(contextClassLoader);

      // This is a required safety valve to ensure we don't accidentally bleed information
      // between requests.
      SolrRequestInfo.reset();
      if (!req.isAsyncStarted()) { // jetty's proxy uses this

        // required to avoid SOLR-8453 and SOLR-8683
        ServletUtils.consumeInputFully(req, res);

        // required to remove temporary files created during multipart requests.
        SolrRequestParsers.cleanupMultipartFiles(req);
      }
    }
  }
}

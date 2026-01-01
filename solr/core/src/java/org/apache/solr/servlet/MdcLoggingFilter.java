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

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.logging.MDCSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Servlet Filter to set up and tear down MDC Logging context for each reqeust. */
public class MdcLoggingFilter extends CoreContainerAwareHttpFilter {

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
      log.trace("MDC snapshot recorded {}", mdcSnapshot); // avoid both compiler and ide warning.
      MDCLoggingContext.reset();
      MDCLoggingContext.setNode(getCores());
      // This doesn't belong here, but for the moment it is here to preserve it's relative
      // timing of execution for now. Probably I will break this out in a subsequent change
      Thread.currentThread().setContextClassLoader(getCores().getResourceLoader().getClassLoader());
      chain.doFilter(req, res);
    } finally {
      MDCLoggingContext.reset();
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
  }
}

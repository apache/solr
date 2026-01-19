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
package org.apache.solr.crossdc.manager.consumer;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;

/**
 * An HTTP servlets which outputs a {@code text/plain} dump of all threads in the VM. Only responds
 * to {@code GET} requests.
 *
 * <p>Copy of the code from <code>
 * https://github.com/dropwizard/metrics/blob/release/5.0.x/metrics-jakarta-servlets/src/main/java/io/dropwizard/metrics5/servlets/ThreadDumpServlet.java
 * </code>
 */
public class ThreadDumpServlet extends HttpServlet {

  private static final long serialVersionUID = -2690343532336103046L;
  private static final String CONTENT_TYPE = "text/plain";

  private transient ThreadDump threadDump;

  @Override
  public void init() throws ServletException {
    try {
      // Some PaaS like Google App Engine blacklist java.lang.managament
      this.threadDump = new ThreadDump(ManagementFactory.getThreadMXBean());
    } catch (NoClassDefFoundError ncdfe) {
      this.threadDump = null; // we won't be able to provide thread dump
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    final boolean includeMonitors = getParam(req.getParameter("monitors"), true);
    final boolean includeSynchronizers = getParam(req.getParameter("synchronizers"), true);

    resp.setStatus(HttpServletResponse.SC_OK);
    resp.setContentType(CONTENT_TYPE);
    resp.setHeader("Cache-Control", "must-revalidate,no-cache,no-store");
    if (threadDump == null) {
      resp.getWriter().println("Sorry your runtime environment does not allow to dump threads.");
      return;
    }
    try (OutputStream output = resp.getOutputStream()) {
      threadDump.dump(includeMonitors, includeSynchronizers, output);
    }
  }

  private static Boolean getParam(String initParam, boolean defaultValue) {
    return initParam == null ? defaultValue : Boolean.parseBoolean(initParam);
  }
}

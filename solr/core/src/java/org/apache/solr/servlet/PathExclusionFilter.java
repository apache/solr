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
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PathExclusionFilter extends HttpFilter {

  private List<Pattern> excludePatterns;

  boolean shouldBeExcluded(HttpServletRequest request) {
    String requestPath = ServletUtils.getPathAfterContext(request);
    if (excludePatterns != null) {
      return excludePatterns.stream().map(p -> p.matcher(requestPath)).anyMatch(Matcher::lookingAt);
    }
    return false;
  }

  @Override
  public void init(FilterConfig config) throws ServletException {
    String patternConfig = config.getInitParameter("excludePatterns");
    if (patternConfig != null) {
      String[] excludeArray = patternConfig.split(",");
      this.excludePatterns = Arrays.stream(excludeArray).map(Pattern::compile).toList();
    }
    super.init(config);
  }

  @Override
  protected void doFilter(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws IOException, ServletException {
    if (!shouldBeExcluded(req)) {
      chain.doFilter(req, res);
    } else {
      // N.B. "default" is the name for org.eclipse.jetty.ee10.servlet.DefaultServlet
      // configured in solr/server/etc/webdefault.xml
      req.getServletContext().getNamedDispatcher("default").forward(req, res);
    }
  }
}

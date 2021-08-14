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

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import static org.apache.solr.servlet.ServletUtils.closeShield;
import static org.apache.solr.servlet.ServletUtils.configExcludes;
import static org.apache.solr.servlet.ServletUtils.excludedPath;

@WebServlet
public class AdminServlet extends HttpServlet implements PathExcluder{
  private ArrayList<Pattern> excludes;

  @Override
  public void init() throws ServletException {
    configExcludes(this, getInitParameter("excludePatterns"));
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    req = closeShield(req, false);
    resp = closeShield(resp, false);
    if (excludedPath(excludes,req,resp,null)) {
      return;
    }
    ServletUtils.rateLimitRequest(req,resp,() -> {
      // stuff from HttpSolrCallHere
    },false);
    // todo: enable tracing
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    req = closeShield(req, false);
    resp = closeShield(resp, false);
    if (excludedPath(excludes,req,resp,null)) {
      return;
    }
    ServletUtils.rateLimitRequest(req,resp,() -> {
      // stuff from HttpSolrCallHere
    },false);
    //todo: enable tracing
  }

  @Override
  public void setExcludePatterns(ArrayList<Pattern> excludePatterns) {
    this.excludes = excludePatterns;
  }
}

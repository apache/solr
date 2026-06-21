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
package org.apache.solr.client.solrj.impl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.util.SuppressForbidden;

/**
 * Test servlet that records the last request it received. Formerly an inner class of
 * BasicHttpSolrClientTest (removed with the HTTP/1 Http2SolrClient); extracted so the
 * HTTP/2 client tests can keep using it.
 */
public class DebugServlet extends HttpServlet {
  public static void clear() {
    lastMethod = null;
    headers = null;
    parameters = null;
    errorCode = null;
    queryString = null;
    cookies = null;
  }

  public static Integer errorCode = null;
  public static String lastMethod = null;
  public static HashMap<String,String> headers = null;
  public static Map<String,String[]> parameters = null;
  public static String queryString = null;
  public static javax.servlet.http.Cookie[] cookies = null;

  public static void setErrorCode(Integer code) {
    errorCode = code;
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    lastMethod = "delete";
    recordRequest(req, resp);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    lastMethod = "get";
    recordRequest(req, resp);
  }

  @Override
  protected void doHead(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    lastMethod = "head";
    recordRequest(req, resp);
  }

  private void setHeaders(HttpServletRequest req) {
    Enumeration<String> headerNames = req.getHeaderNames();
    headers = new HashMap<>();
    while (headerNames.hasMoreElements()) {
      final String name = headerNames.nextElement();
      headers.put(name, req.getHeader(name));
    }
  }

  @SuppressForbidden(reason = "fake servlet only")
  private void setParameters(HttpServletRequest req) {
    parameters = req.getParameterMap();
  }

  private void setQueryString(HttpServletRequest req) {
    queryString = req.getQueryString();
  }

  private void setCookies(HttpServletRequest req) {
    cookies = req.getCookies();
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    lastMethod = "post";
    recordRequest(req, resp);
  }

  @Override
  protected void doPut(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    lastMethod = "put";
    recordRequest(req, resp);
  }

  private void recordRequest(HttpServletRequest req, HttpServletResponse resp) {
    setHeaders(req);
    setParameters(req);
    setQueryString(req);
    setCookies(req);
    if (null != errorCode) {
      try {
        resp.sendError(errorCode);
      } catch (IOException e) {
        throw new RuntimeException("sendError IO fail in DebugServlet", e);
      }
    }
  }
}

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.ReadListener;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.UnavailableException;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL
public class HttpSolrCallCloudTest extends SolrCloudTestCase {
  private static final String COLLECTION = "collection1";
  private static final int NUM_SHARD = 3;
  private static final int REPLICA_FACTOR = 2;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "config", NUM_SHARD, REPLICA_FACTOR)
        .process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        COLLECTION, cluster.getZkStateReader(), false, true, 30);
  }

  @Test
  public void testCoreChosen() throws Exception {
    assertCoreChosen(NUM_SHARD, new TestRequest("/collection1/update"));
    assertCoreChosen(NUM_SHARD, new TestRequest("/collection1/update/json"));
    assertCoreChosen(NUM_SHARD * REPLICA_FACTOR, new TestRequest("/collection1/select"));
  }

  // https://issues.apache.org/jira/browse/SOLR-16019
  @Test
  public void testWrongUtf8InQ() throws Exception {
    var baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
    var request =
        URI.create(baseUrl.toString() + "/" + COLLECTION + "/select?q=%C0")
            .toURL(); // Illegal UTF-8 string
    var connection = (HttpURLConnection) request.openConnection();
    assertEquals(400, connection.getResponseCode());
  }

  private void assertCoreChosen(int numCores, TestRequest testRequest) throws UnavailableException {
    JettySolrRunner jettySolrRunner = cluster.getJettySolrRunner(0);
    Set<String> coreNames = new HashSet<>();
    SolrDispatchFilter dispatchFilter = jettySolrRunner.getSolrDispatchFilter();
    for (int i = 0; i < NUM_SHARD * REPLICA_FACTOR * 20; i++) {
      if (coreNames.size() == numCores) return;
      HttpSolrCall httpSolrCall =
          new HttpSolrCall(
              dispatchFilter, dispatchFilter.getCores(), testRequest, new TestResponse(), false);
      try {
        httpSolrCall.init();
      } catch (Exception e) {
      } finally {
        coreNames.add(httpSolrCall.core.getName());
        httpSolrCall.destroy();
      }
    }
    assertEquals(numCores, coreNames.size());
  }

  public static class TestRequest implements HttpServletRequest {
    private final String path;

    public TestRequest(String path) {
      this.path = path;
    }

    @Override
    public String getAuthType() {
      return "";
    }

    @Override
    public Cookie[] getCookies() {
      return new Cookie[0];
    }

    @Override
    public long getDateHeader(String name) {
      return 0;
    }

    @Override
    public String getHeader(String name) {
      return "";
    }

    @Override
    public Enumeration<String> getHeaders(String name) {
      return null;
    }

    @Override
    public Enumeration<String> getHeaderNames() {
      return null;
    }

    @Override
    public int getIntHeader(String name) {
      return 0;
    }

    @Override
    public String getMethod() {
      return "";
    }

    @Override
    public String getPathInfo() {
      return "";
    }

    @Override
    public String getPathTranslated() {
      return "";
    }

    @Override
    public String getContextPath() {
      return "";
    }

    @Override
    public String getQueryString() {
      return "version=2";
    }

    @Override
    public String getRemoteUser() {
      return "";
    }

    @Override
    public boolean isUserInRole(String role) {
      return false;
    }

    @Override
    public Principal getUserPrincipal() {
      return null;
    }

    @Override
    public String getRequestedSessionId() {
      return "";
    }

    @Override
    public String getRequestURI() {
      return path;
    }

    @Override
    public StringBuffer getRequestURL() {
      return null;
    }

    @Override
    public String getServletPath() {
      return path;
    }

    @Override
    public HttpSession getSession(boolean create) {
      return null;
    }

    @Override
    public HttpSession getSession() {
      return null;
    }

    @Override
    public String changeSessionId() {
      return "";
    }

    @Override
    public boolean isRequestedSessionIdValid() {
      return false;
    }

    @Override
    public boolean isRequestedSessionIdFromCookie() {
      return false;
    }

    @Override
    public boolean isRequestedSessionIdFromURL() {
      return false;
    }

    @Override
    public boolean isRequestedSessionIdFromUrl() {
      return false;
    }

    @Override
    public boolean authenticate(HttpServletResponse response) throws IOException, ServletException {
      return false;
    }

    @Override
    public void login(String username, String password) throws ServletException {}

    @Override
    public void logout() throws ServletException {}

    @Override
    public Collection<Part> getParts() throws IOException, ServletException {
      return List.of();
    }

    @Override
    public Part getPart(String name) throws IOException, ServletException {
      return null;
    }

    @Override
    public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass)
        throws IOException, ServletException {
      return null;
    }

    @Override
    public Object getAttribute(String name) {
      return null;
    }

    @Override
    public Enumeration<String> getAttributeNames() {
      return null;
    }

    @Override
    public String getCharacterEncoding() {
      return "";
    }

    @Override
    public void setCharacterEncoding(String env) throws UnsupportedEncodingException {}

    @Override
    public int getContentLength() {
      return 0;
    }

    @Override
    public long getContentLengthLong() {
      return 0;
    }

    @Override
    public String getContentType() {
      return "application/json";
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
      return new ServletInputStream() {
        @Override
        public boolean isFinished() {
          return true;
        }

        @Override
        public boolean isReady() {
          return true;
        }

        @Override
        public void setReadListener(ReadListener readListener) {}

        @Override
        public int read() {
          return 0;
        }
      };
    }

    @Override
    public String getParameter(String name) {
      return "";
    }

    @Override
    public Enumeration<String> getParameterNames() {
      return null;
    }

    @Override
    public String[] getParameterValues(String name) {
      return new String[0];
    }

    @Override
    public Map<String, String[]> getParameterMap() {
      return Map.of();
    }

    @Override
    public String getProtocol() {
      return "";
    }

    @Override
    public String getScheme() {
      return "";
    }

    @Override
    public String getServerName() {
      return "";
    }

    @Override
    public int getServerPort() {
      return 0;
    }

    @Override
    public BufferedReader getReader() throws IOException {
      return null;
    }

    @Override
    public String getRemoteAddr() {
      return "";
    }

    @Override
    public String getRemoteHost() {
      return "";
    }

    @Override
    public void setAttribute(String name, Object o) {}

    @Override
    public void removeAttribute(String name) {}

    @Override
    public Locale getLocale() {
      return null;
    }

    @Override
    public Enumeration<Locale> getLocales() {
      return null;
    }

    @Override
    public boolean isSecure() {
      return false;
    }

    @Override
    public RequestDispatcher getRequestDispatcher(String path) {
      return null;
    }

    @Override
    public String getRealPath(String path) {
      return "";
    }

    @Override
    public int getRemotePort() {
      return 0;
    }

    @Override
    public String getLocalName() {
      return "";
    }

    @Override
    public String getLocalAddr() {
      return "";
    }

    @Override
    public int getLocalPort() {
      return 0;
    }

    @Override
    public ServletContext getServletContext() {
      return null;
    }

    @Override
    public AsyncContext startAsync() throws IllegalStateException {
      return null;
    }

    @Override
    public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse)
        throws IllegalStateException {
      return null;
    }

    @Override
    public boolean isAsyncStarted() {
      return false;
    }

    @Override
    public boolean isAsyncSupported() {
      return false;
    }

    @Override
    public AsyncContext getAsyncContext() {
      return null;
    }

    @Override
    public DispatcherType getDispatcherType() {
      return null;
    }
  }

  public static class TestResponse implements HttpServletResponse {

    @Override
    public void addCookie(Cookie cookie) {}

    @Override
    public boolean containsHeader(String name) {
      return false;
    }

    @Override
    public String encodeURL(String url) {
      return "";
    }

    @Override
    public String encodeRedirectURL(String url) {
      return "";
    }

    @Override
    public String encodeUrl(String url) {
      return "";
    }

    @Override
    public String encodeRedirectUrl(String url) {
      return "";
    }

    @Override
    public void sendError(int sc, String msg) throws IOException {}

    @Override
    public void sendError(int sc) throws IOException {}

    @Override
    public void sendRedirect(String location) throws IOException {}

    @Override
    public void setDateHeader(String name, long date) {}

    @Override
    public void addDateHeader(String name, long date) {}

    @Override
    public void setHeader(String name, String value) {}

    @Override
    public void addHeader(String name, String value) {}

    @Override
    public void setIntHeader(String name, int value) {}

    @Override
    public void addIntHeader(String name, int value) {}

    @Override
    public void setStatus(int sc) {}

    @Override
    public void setStatus(int sc, String sm) {}

    @Override
    public int getStatus() {
      return 0;
    }

    @Override
    public String getHeader(String name) {
      return "";
    }

    @Override
    public Collection<String> getHeaders(String name) {
      return List.of();
    }

    @Override
    public Collection<String> getHeaderNames() {
      return List.of();
    }

    @Override
    public String getCharacterEncoding() {
      return "";
    }

    @Override
    public String getContentType() {
      return "";
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
      return new ServletOutputStream() {
        @Override
        public boolean isReady() {
          return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {}

        @Override
        public void write(int b) {}
      };
    }

    @Override
    public PrintWriter getWriter() throws IOException {
      return null;
    }

    @Override
    public void setCharacterEncoding(String charset) {}

    @Override
    public void setContentLength(int len) {}

    @Override
    public void setContentLengthLong(long len) {}

    @Override
    public void setContentType(String type) {}

    @Override
    public void setBufferSize(int size) {}

    @Override
    public int getBufferSize() {
      return 0;
    }

    @Override
    public void flushBuffer() throws IOException {}

    @Override
    public void resetBuffer() {}

    @Override
    public boolean isCommitted() {
      return true;
    }

    @Override
    public void reset() {}

    @Override
    public void setLocale(Locale loc) {}

    @Override
    public Locale getLocale() {
      return null;
    }
  }
}

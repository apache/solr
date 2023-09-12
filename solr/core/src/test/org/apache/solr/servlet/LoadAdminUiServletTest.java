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

import static org.apache.solr.servlet.LoadAdminUiServlet.SYSPROP_CSP_CONNECT_SRC_URLS;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.CoreContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class LoadAdminUiServletTest extends SolrTestCaseJ4 {

  @InjectMocks private LoadAdminUiServlet servlet;
  @Mock private HttpServletRequest mockRequest;
  @Mock private HttpServletResponse mockResponse;
  @Mock private CoreContainer coreContainer;
  @Mock private ServletConfig servletConfig;
  @Mock private ServletContext mockServletContext;
  @Mock private ServletOutputStream mockOutputStream;

  private static final Set<String> CSP_URLS =
      Set.of(
          "http://example1.com/token",
          "https://example2.com/path/uri1",
          "http://example3.com/oauth2/uri2");

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    MockitoAnnotations.openMocks(this);
    when(mockRequest.getRequestURI()).thenReturn("/path/URI");
    when(mockRequest.getContextPath()).thenReturn("/path");
    when(mockRequest.getAttribute("org.apache.solr.CoreContainer")).thenReturn(coreContainer);
    when(servletConfig.getServletContext()).thenReturn(mockServletContext);
    when(mockResponse.getOutputStream()).thenReturn(mockOutputStream);
    InputStream mockInputStream =
        new ByteArrayInputStream("mock content".getBytes(StandardCharsets.UTF_8));
    when(mockServletContext.getResourceAsStream(anyString())).thenReturn(mockInputStream);
  }

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Test
  public void testDefaultCSPHeaderSet() throws IOException {
    System.setProperty(SYSPROP_CSP_CONNECT_SRC_URLS, String.join(",", CSP_URLS));
    ArgumentCaptor<String> headerNameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> headerValueCaptor = ArgumentCaptor.forClass(String.class);
    servlet.doGet(mockRequest, mockResponse);

    verify(mockResponse).setHeader(headerNameCaptor.capture(), headerValueCaptor.capture());
    assertEquals("Content-Security-Policy", headerNameCaptor.getValue());
    String cspValue = headerValueCaptor.getValue();
    for (String endpoint : CSP_URLS) {
      assertTrue("Expected CSP value to contain " + endpoint, cspValue.contains(endpoint));
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
}

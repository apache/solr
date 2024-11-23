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
package org.apache.solr.cli;

import java.net.SocketException;
import java.net.URISyntaxException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.junit.Test;

public class CLIUtilsTest extends SolrCloudTestCase {

  @Test
  public void testDefaultSolrUrlWithNoProperties() {
    System.clearProperty("solr.url.scheme");
    System.clearProperty("solr.tool.host");
    System.clearProperty("jetty.port");
    assertEquals(
        "Default Solr URL should match with no properties set.",
        "http://localhost:8983",
        CLIUtils.getDefaultSolrUrl());
  }

  @Test
  public void testDefaultSolrUrlWithProperties() {
    System.setProperty("solr.url.scheme", "https");
    System.setProperty("solr.tool.host", "other.local");
    System.setProperty("jetty.port", "1234");
    assertEquals(
        "Default Solr URL should match with custom properties set.",
        "https://other.local:1234",
        CLIUtils.getDefaultSolrUrl());
  }

  @Test
  public void testCommunicationErrors() {
    // communication errors
    Exception serverException = new Exception(new SolrServerException(""));
    assertTrue(
        "SolrServerException should be communication error",
        CLIUtils.checkCommunicationError(serverException));

    Exception socketException = new RuntimeException(new Exception(new SocketException()));
    assertTrue(
        "SocketException should be communication error",
        CLIUtils.checkCommunicationError(socketException));

    // TODO See if this should be a communication error or not
    //    Exception parentException = new SolrServerException(new Exception());
    //    assertTrue(
    //        "SolrServerException with different root cause should be communication error",
    //        CLIUtils.checkCommunicationError(parentException));

    Exception rootException = new SolrServerException("");
    assertTrue(
        "SolrServerException with no cause should be communication error",
        CLIUtils.checkCommunicationError(rootException));

    // non-communication errors
    Exception exception1 = new NullPointerException();
    assertFalse(
        "NullPointerException should not be communication error",
        CLIUtils.checkCommunicationError(exception1));

    Exception exception2 = new RuntimeException(new Exception());
    assertFalse(
        "Exception should not be communication error",
        CLIUtils.checkCommunicationError(exception2));
  }

  @Test
  public void testCodeForAuthError() throws SolrException {
    // auth errors
    assertThrows(
        "Forbidden (403) should throw SolrException",
        SolrException.class,
        () -> CLIUtils.checkCodeForAuthError(SolrException.ErrorCode.FORBIDDEN.code));
    assertThrows(
        "Unauthorized (401) should throw SolrException",
        SolrException.class,
        () -> CLIUtils.checkCodeForAuthError(SolrException.ErrorCode.UNAUTHORIZED.code));

    // non auth errors
    CLIUtils.checkCodeForAuthError(SolrException.ErrorCode.BAD_REQUEST.code);
    CLIUtils.checkCodeForAuthError(SolrException.ErrorCode.CONFLICT.code);
    CLIUtils.checkCodeForAuthError(SolrException.ErrorCode.SERVER_ERROR.code);
    CLIUtils.checkCodeForAuthError(0); // Unknown
    CLIUtils.checkCodeForAuthError(200); // HTTP OK
  }

  @Test
  public void testResolveSolrUrl() {
    assertEquals(CLIUtils.normalizeSolrUrl("http://localhost:8983/solr"), "http://localhost:8983");
    assertEquals(CLIUtils.normalizeSolrUrl("http://localhost:8983/solr/"), "http://localhost:8983");
    assertEquals(CLIUtils.normalizeSolrUrl("http://localhost:8983/"), "http://localhost:8983");
    assertEquals(CLIUtils.normalizeSolrUrl("http://localhost:8983"), "http://localhost:8983");
    assertEquals(
        CLIUtils.normalizeSolrUrl("http://localhost:8983/solr/", false), "http://localhost:8983");
  }

  @Test
  public void testPortExtraction() throws URISyntaxException {
    assertEquals(
        "Should extract explicit port from valid URL",
        8983,
        CLIUtils.portFromUrl("http://localhost:8983"));

    assertEquals(
        "Should extract explicit port from valid URL with trailing slash",
        1234,
        CLIUtils.portFromUrl("http://localhost:1234/"));

    assertEquals(
        "Should extract implicit HTTP port (80)", 80, CLIUtils.portFromUrl("http://localhost"));

    assertEquals(
        "Should extract implicit HTTPS port (443)", 443, CLIUtils.portFromUrl("https://localhost"));

    // TODO See if we could be more lenient and fallback to defaults instead.
    assertThrows(
        "Should throw NullpointerException if no scheme provided",
        NullPointerException.class,
        () -> CLIUtils.portFromUrl("localhost"));

    // Note that a bunch of invalid URIs like "http::example.com", "http:/example.com" and
    // "//example.com" are not throwing URISyntaxException. This however is an issue of
    // java.lang.URI, which is very lenient.
  }
}

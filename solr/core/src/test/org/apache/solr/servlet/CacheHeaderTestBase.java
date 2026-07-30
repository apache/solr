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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.SolrJettyTestRule;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.junit.ClassRule;
import org.junit.Test;

public abstract class CacheHeaderTestBase extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  protected String getSelectUrl(String... params) {
    StringBuilder sb = new StringBuilder();
    sb.append(solrTestRule.getBaseUrl());
    sb.append("/");
    sb.append(DEFAULT_TEST_COLLECTION_NAME);
    sb.append("/select?");

    if (params.length == 0) {
      sb.append("q=solr&qt=standard");
    } else {
      for (int i = 0; i < params.length / 2; i++) {
        if (i > 0) sb.append("&");
        sb.append(params[i * 2]);
        sb.append("=");
        sb.append(params[i * 2 + 1]);
      }
    }

    return sb.toString();
  }

  protected HttpClient getHttpClient() {
    return solrTestRule.getJetty().getSolrClient().getHttpClient();
  }

  protected void checkResponseBody(String method, ContentResponse resp) throws Exception {
    String responseBody = resp.getContentAsString();

    if ("GET".equals(method)) {
      switch (resp.getStatus()) {
        case 200:
          assertTrue(
              "Response body was empty for method " + method,
              responseBody != null && responseBody.length() > 0);
          break;
        case 304:
          assertTrue(
              "Response body was not empty for method " + method,
              responseBody == null || responseBody.length() == 0);
          break;
        case 412:
          assertTrue(
              "Response body was not empty for method " + method,
              responseBody == null || responseBody.length() == 0);
          break;
        default:
          System.err.println(responseBody);
          assertEquals("Unknown request response", 0, resp.getStatus());
      }
    }
    if ("HEAD".equals(method)) {
      assertTrue(
          "Response body was not empty for method " + method,
          responseBody == null || responseBody.length() == 0);
    }
  }

  // The tests
  @Test
  public void testLastModified() throws Exception {
    doLastModified("GET");
    doLastModified("HEAD");
  }

  @Test
  public void testEtag() throws Exception {
    doETag("GET");
    doETag("HEAD");
  }

  @Test
  public void testCacheControl() throws Exception {
    doCacheControl("GET");
    doCacheControl("HEAD");
    doCacheControl("POST");
  }

  protected abstract void doCacheControl(String method) throws Exception;

  protected abstract void doETag(String method) throws Exception;

  protected abstract void doLastModified(String method) throws Exception;
}

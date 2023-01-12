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

import java.util.HashSet;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/** Test the LBHttp2SolrClient. */
public class LBHttp2SolrClientTest extends SolrTestCase {

  /**
   * Test method for {@link LBHttp2SolrClient.Builder} that validates that the query param keys
   * passed in by the base <code>Http2SolrClient
   * </code> instance are used by the LBHttp2SolrClient.
   *
   * <p>TODO: Eliminate the addQueryParams aspect of test as it is not compatible with goal of
   * immutable SolrClient
   */
  @Test
  public void testLBHttp2SolrClientSetQueryParams() {
    String url = "http://127.0.0.1:8080";
    Set<String> urlParamNames = new HashSet<>(2);
    urlParamNames.add("param1");

    try (Http2SolrClient http2SolrClient =
            new Http2SolrClient.Builder(url).withTheseParamNamesInTheUrl(urlParamNames).build();
        LBHttp2SolrClient testClient =
            new LBHttp2SolrClient.Builder(http2SolrClient, url).build()) {

      assertArrayEquals(
          "Wrong urlParamNames found in lb client.",
          urlParamNames.toArray(),
          testClient.getUrlParamNames().toArray());
      assertArrayEquals(
          "Wrong urlParamNames found in base client.",
          urlParamNames.toArray(),
          http2SolrClient.getUrlParamNames().toArray());

      testClient.addQueryParams("param2");
      urlParamNames.add("param2");
      assertArrayEquals(
          "Wrong urlParamNames found in lb client.",
          urlParamNames.toArray(),
          testClient.getUrlParamNames().toArray());
      assertArrayEquals(
          "Wrong urlParamNames found in base client.",
          urlParamNames.toArray(),
          http2SolrClient.getUrlParamNames().toArray());
    }
  }
}

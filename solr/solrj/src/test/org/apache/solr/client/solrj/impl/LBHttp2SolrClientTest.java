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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.junit.Test;

/** Test the LBHttp2SolrClient. */
public class LBHttp2SolrClientTest extends SolrTestCase {

  /**
   * Test method for {@link LBHttp2SolrClient#setParser(ResponseParser)}.
   *
   * <p>Validate that the parser passed in is used in the base <code>Http2SolrClient</code>
   * instance.
   */
  @Test
  public void testLBHttp2SolrClientSetRequestParser() throws IOException {
    String url = "http://127.0.0.1:8080";
    try (Http2SolrClient http2Client = new Http2SolrClient.Builder(url).build();
        LBHttp2SolrClient testClient = new LBHttp2SolrClient(http2Client, url)) {
      testClient.setParser(null);
      assertNull("Generated lb client should have null parser.", testClient.getParser());
      assertNull("Generated base client should have null parser.", http2Client.getParser());

      ResponseParser parser = new NoOpResponseParser("json");
      testClient.setParser(parser);
      assertEquals("Wrong parser found in lb client.", parser, testClient.getParser());
      assertEquals("Wrong parser found in base client.", parser, http2Client.getParser());
    }
  }

  /**
   * Test method for {@link LBHttp2SolrClient#setRequestWriter(RequestWriter)}.
   *
   * <p>Validate that the requestWriter passed in is used in the base <code>Http2SolrClient</code>
   * instance.
   */
  @Test
  public void testLBHttp2SolrClientSetRequestWriter() throws IOException {
    String url = "http://127.0.0.1:8080";
    try (Http2SolrClient http2Client = new Http2SolrClient.Builder(url).build();
        LBHttp2SolrClient testClient = new LBHttp2SolrClient(http2Client, url)) {
      testClient.setRequestWriter(null);
      assertNull("Generated lb client should have null writer.", testClient.getRequestWriter());
      assertNull("Generated base client should have null writer.", http2Client.getRequestWriter());

      RequestWriter writer = new BinaryRequestWriter();
      testClient.setRequestWriter(writer);
      assertEquals("Wrong writer found in lb client.", writer, testClient.getRequestWriter());
      assertEquals("Wrong writer found in base client.", writer, http2Client.getRequestWriter());
    }
  }

  /**
   * Test method for {@link LBHttp2SolrClient#setQueryParams(Set)} and {@link
   * LBHttp2SolrClient#addQueryParams(String)}.
   *
   * <p>Validate that the query params passed in are used in the base <code>Http2SolrClient</code>
   * instance.
   */
  @Test
  public void testLBHttp2SolrClientSetQueryParams() throws IOException {
    String url = "http://127.0.0.1:8080";
    try (Http2SolrClient http2Client = new Http2SolrClient.Builder(url).build();
        LBHttp2SolrClient testClient = new LBHttp2SolrClient(http2Client, url)) {
      Set<String> queryParams = new HashSet<>(2);
      queryParams.add("param1=this");
      testClient.setQueryParams(new HashSet<>(queryParams));
      assertArrayEquals(
          "Wrong queryParams found in lb client.",
          queryParams.toArray(),
          testClient.getQueryParams().toArray());
      assertArrayEquals(
          "Wrong queryParams found in base client.",
          queryParams.toArray(),
          http2Client.getQueryParams().toArray());

      testClient.addQueryParams("param2=that");
      queryParams.add("param2=that");
      assertArrayEquals(
          "Wrong queryParams found in lb client.",
          queryParams.toArray(),
          testClient.getQueryParams().toArray());
      assertArrayEquals(
          "Wrong queryParams found in base client.",
          queryParams.toArray(),
          http2Client.getQueryParams().toArray());
    }
  }
}

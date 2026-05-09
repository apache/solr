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
package org.apache.solr.client.solrj.request;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.junit.Test;

public class QueryRequestTest extends SolrTestCase {

  @Test
  public void testQtWithSlashBecomesPath() {
    SolrQuery query = new SolrQuery("*:*");
    query.setRequestHandler("/custom");

    QueryRequest request = new QueryRequest(query);

    assertEquals("/custom", request.getPath());
    assertNull("qt parameter should be removed from request params", request.getParams().get("qt"));
    assertEquals("*:*", request.getParams().get("q"));
  }

  @Test
  public void testQtWithoutSlashDefaultsToSelect() {
    SolrQuery query = new SolrQuery("*:*");
    query.setRequestHandler("custom");

    QueryRequest request = new QueryRequest(query);

    assertEquals("/select", request.getPath());
    assertNull("qt parameter shouldn't be sent to server", request.getParams().get("qt"));
  }

  @Test
  public void testDefaultPathToSelect() {
    SolrQuery query = new SolrQuery("*:*");

    QueryRequest request = new QueryRequest(query);

    assertEquals("/select", request.getPath());
    assertNull(request.getParams().get("qt"));
  }

  @Test
  public void testExplicitPath() {
    SolrQuery query = new SolrQuery("*:*");
    QueryRequest request = new QueryRequest("/custom", query);

    assertEquals("/custom", request.getPath());
    assertNull("qt parameter must not be present", request.getParams().get("qt"));
    assertEquals("*:*", request.getParams().get("q"));
  }

  @Test
  public void testExplicitPathWithMethod() {
    SolrQuery query = new SolrQuery("*:*");
    QueryRequest request = new QueryRequest("/spell", query, SolrRequest.METHOD.POST);

    assertEquals("/spell", request.getPath());
    assertEquals(SolrRequest.METHOD.POST, request.getMethod());
    assertNull(request.getParams().get("qt"));
  }
}

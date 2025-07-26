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
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

public class QueryRequestQtTest extends SolrTestCase {

  @Test
  public void testQtParameterRemovedFromRequest() {
    SolrQuery query = new SolrQuery("*:*");
    query.setRequestHandler("/custom");

    QueryRequest request = new QueryRequest(query);
    // The path should be extracted from qt
    assertEquals("/custom", request.getPath());

    SolrParams params = request.getParams();
    assertNull("qt parameter should be removed from request params", params.get(CommonParams.QT));
    assertEquals("*:*", params.get(CommonParams.Q));
  }

  @Test
  public void testQtParameterWithoutSlashUsesSelect() {
    SolrQuery query = new SolrQuery("*:*");
    query.setRequestHandler("custom");

    QueryRequest request = new QueryRequest(query);
    assertEquals("/select", request.getPath());

    SolrParams params = request.getParams();
    assertNull("qt parameter should be removed from request params", params.get(CommonParams.QT));
  }

  @Test
  public void testNoQtParameter() {
    SolrQuery query = new SolrQuery("*:*");
    QueryRequest request = new QueryRequest(query);
    assertEquals("/select", request.getPath());

    // qt should still be removed from parameters
    SolrParams params = request.getParams();
    assertNull("qt parameter should not be present", params.get(CommonParams.QT));
    assertEquals("*:*", params.get(CommonParams.Q));
  }
}

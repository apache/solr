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

/**
 * Test that verifies SOLR-17715: qt parameter should not be sent to Solr server
 */
public class QueryRequestQtTest extends SolrTestCase {

  @Test
  public void testQtParameterRemovedFromRequest() {
    SolrQuery query = new SolrQuery("*:*");
    query.setRequestHandler("/custom");
    
    QueryRequest request = new QueryRequest(query);
    
    // The path should be extracted from qt
    assertEquals("/custom", request.getPath());
    
    // But qt should not be in the final parameters
    SolrParams params = request.getParams();
    assertNull("qt parameter should be removed from request params", 
               params.get(CommonParams.QT));
    assertEquals("*:*", params.get(CommonParams.Q));
  }
  
  @Test
  public void testQtParameterWithoutSlashUsesSelect() {
    SolrQuery query = new SolrQuery("*:*");
    query.setRequestHandler("custom"); // no leading slash
    
    QueryRequest request = new QueryRequest(query);
    
    // Should default to /select when qt doesn't start with /
    assertEquals("/select", request.getPath());
    
    // qt should still be removed from parameters
    SolrParams params = request.getParams();
    assertNull("qt parameter should be removed from request params", 
               params.get(CommonParams.QT));
  }
  
  @Test
  public void testNoQtParameter() {
    SolrQuery query = new SolrQuery("*:*");
    // Don't set any request handler
    
    QueryRequest request = new QueryRequest(query);
    
    // Should default to /select
    assertEquals("/select", request.getPath());
    
    // Should not have qt parameter
    SolrParams params = request.getParams();
    assertNull("qt parameter should not be present", 
               params.get(CommonParams.QT));
    assertEquals("*:*", params.get(CommonParams.Q));
  }
}

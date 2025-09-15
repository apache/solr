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
package org.apache.solr.handler.component;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.junit.BeforeClass;
import org.junit.Test;

// modelled on
// solr/core/src/test/org/apache/solr/handler/component/CustomHighlightComponentTest.java
public class CompoundQueryComponentTest extends SolrCloudTestCase {

  private static String COLLECTION;

  @BeforeClass
  public static void setupCluster() throws Exception {

    // decide collection name ...
    COLLECTION = "collection" + (1 + random().nextInt(100));
    // ... and shard/replica/node numbers
    final int numShards = random().nextBoolean() ? 1 : 3;
    final int numReplicas = 2;
    final int nodeCount = numShards * numReplicas;

    // create and configure cluster
    configureCluster(nodeCount).addConfig("conf", configset("cloud-dynamic")).configure();

    // create an empty collection
    CollectionAdminRequest.createCollection(COLLECTION, "conf", numShards, numReplicas)
        .process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        COLLECTION, cluster.getZkStateReader(), false, true, DEFAULT_TIMEOUT);
  }

  @Test
  public void test() throws Exception {

    // determine search handler name (the exact name should not matter)
    final String compoundSearchHandlerName = "/compound_select" + random().nextInt();

    // component
    cluster
        .getSolrClient()
        .request(
            new ConfigRequest(
                "{\n"
                    + "  'add-searchcomponent': {\n"
                    + "    'name': '"
                    + CompoundQueryComponent.COMPONENT_NAME
                    + "',\n"
                    + "    'class': '"
                    + CompoundQueryComponent.class.getName()
                    + "'\n"
                    + "  }\n"
                    + "}"),
            COLLECTION);
    // handler
    cluster
        .getSolrClient()
        .request(
            new ConfigRequest(
                "{\n"
                    + "  'add-requesthandler': {\n"
                    + "    'name' : '"
                    + compoundSearchHandlerName
                    + "',\n"
                    + "    'class' : 'org.apache.solr.handler.component.CompoundSearchHandler',\n"
                    + "    'invariants' : { "
                    + "      'shortCircuit' : 'false' "
                    + " },\n"
                    + "    'components' : [ '"
                    + CompoundQueryComponent.COMPONENT_NAME
                    + "' ]\n"
                    + "  }\n"
                    + "}"),
            COLLECTION);

    // add some documents
    final String id = "id";
    final String t1 = "a_t";
    {
      new UpdateRequest()
          .add(sdoc(id, "a", t1, "alfalfa"))
          .add(sdoc(id, "b", t1, "borage"))
          .add(sdoc(id, "c", t1, "clover"))
          .add(sdoc(id, "1", t1, "solitary bee"))
          .add(sdoc(id, "10", t1, "bumble bee"))
          .add(sdoc(id, "1000", t1, "honey bee"))
          .commit(cluster.getSolrClient(), COLLECTION);
    }

    final String q_bee_yes = "+" + t1 + ":bee";
    final String q_bee_no = "-" + t1 + ":bee";

    // search for the documents in a single query
    for (String q : new String[] {null, q_bee_yes, q_bee_no}) {
      // compose the query
      final SolrQuery solrQuery = new SolrQuery(q == null ? "*:*" : q);
      solrQuery.set("sort", "id asc");

      // make the query
      final QueryResponse queryResponse =
          new QueryRequest(solrQuery).process(cluster.getSolrClient(), COLLECTION);

      // analyse the response
      SolrDocumentList documentList = queryResponse.getResults();
      assertTrue(documentList.getNumFoundExact());
      assertEquals(documentList.getNumFound(), documentList.size());
      if (q == null) {
        assertEquals(6, documentList.size());
        assertEquals("1", documentList.get(0).getFieldValue("id"));
        assertEquals("10", documentList.get(1).getFieldValue("id"));
        assertEquals("1000", documentList.get(2).getFieldValue("id"));
        assertEquals("a", documentList.get(3).getFieldValue("id"));
        assertEquals("b", documentList.get(4).getFieldValue("id"));
        assertEquals("c", documentList.get(5).getFieldValue("id"));
      } else {
        assertEquals(3, documentList.size());
      }
    }

    // search for the documents via two fused queries
    {
      // compose the query
      final SolrQuery solrQuery = new SolrQuery("id:0");
      solrQuery.set("rrf", true);
      solrQuery.set("rrf.q.1", "{!sort='id desc'}" + q_bee_yes);
      solrQuery.set("rrf.q.2", "{!sort='id asc'}" + q_bee_no);
      solrQuery.setRequestHandler(compoundSearchHandlerName);

      // make the query
      final QueryResponse queryResponse =
          new QueryRequest(solrQuery).process(cluster.getSolrClient(), COLLECTION);

      // analyse the response
      SolrDocumentList documentList = queryResponse.getResults();
      assertFalse(documentList.getNumFoundExact());
      assertEquals(3, documentList.getNumFound());
      assertEquals(6, documentList.size());
      assertEquals("1000", documentList.get(0).getFieldValue("id"));
      assertEquals("a", documentList.get(1).getFieldValue("id"));
      assertEquals("10", documentList.get(2).getFieldValue("id"));
      assertEquals("b", documentList.get(3).getFieldValue("id"));
      assertEquals("1", documentList.get(4).getFieldValue("id"));
      assertEquals("c", documentList.get(5).getFieldValue("id"));
    }
  }
}

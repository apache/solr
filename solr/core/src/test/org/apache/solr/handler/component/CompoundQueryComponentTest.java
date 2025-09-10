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
    final String t2 = "b_t";
    {
      new UpdateRequest()
          .add(sdoc(id, "0", t1, "bumble bee", t2, "bumble bee"))
          .add(sdoc(id, "1", t1, "honey bee", t2, "honey bee"))
          .add(sdoc(id, "2", t1, "solitary bee", t2, "solitary bee"))
          .commit(cluster.getSolrClient(), COLLECTION);
    }

    // search for the documents
    for (boolean one_two : new boolean[] {false, true}) {
      final String id1 = (one_two ? "1" : "2");
      final String id2 = (one_two ? "2" : "1");
      // compose the query
      final SolrQuery solrQuery = new SolrQuery("id:0");
      solrQuery.set("fl", "id,a_t,b_t");
      solrQuery.set("rrf", true);
      solrQuery.set("rrf.q.1", "id:" + id1);
      solrQuery.set("rrf.q.2", "id:" + id2);
      solrQuery.setRequestHandler(compoundSearchHandlerName);

      // make the query
      final QueryResponse queryResponse =
          new QueryRequest(solrQuery).process(cluster.getSolrClient(), COLLECTION);

      // analyse the response
      SolrDocumentList documentList = queryResponse.getResults();
      assertFalse(documentList.getNumFoundExact());
      assertEquals(2, documentList.getNumFound());
      assertEquals(2, documentList.size());
      assertEquals(id1, documentList.get(0).getFieldValue("id"));
      assertEquals(id2, documentList.get(1).getFieldValue("id"));
    }
  }
}

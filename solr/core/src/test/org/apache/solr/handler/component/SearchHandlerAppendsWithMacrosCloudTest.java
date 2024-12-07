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
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class SearchHandlerAppendsWithMacrosCloudTest extends SolrCloudTestCase {

  private static String COLLECTION;
  private static int NUM_SHARDS;
  private static int NUM_REPLICAS;

  @BeforeClass
  public static void setupCluster() throws Exception {

    // decide collection name ...
    COLLECTION = "collection" + (1 + random().nextInt(100));
    // ... and shard/replica/node numbers
    NUM_SHARDS = (2 + random().nextInt(2)); // 0..2
    NUM_REPLICAS = (1 + random().nextInt(2)); // 0..2

    // create and configure cluster
    configureCluster(NUM_SHARDS * NUM_REPLICAS /* nodeCount */)
        .addConfig("conf", configset("cloud-dynamic"))
        .configure();

    // create an empty collection
    CollectionAdminRequest.createCollection(COLLECTION, "conf", NUM_SHARDS, NUM_REPLICAS)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        COLLECTION, ZkStateReader.from(cluster.getSolrClient()), false, true, DEFAULT_TIMEOUT);
  }

  @Test
  public void test() throws Exception {

    // field names
    final String id = "id";
    final String bee_si = "bee_sI";
    final String forage_t = "forage_t";
    final String handlerName = "/custom-select";

    // add custom handlers (the exact custom handler names should not matter)
    cluster
        .getSolrClient()
        .request(
            new ConfigRequest(
                "{\n"
                    + "  'add-requesthandler': {\n"
                    + "    'name' : '"
                    + handlerName
                    + "',\n"
                    + "    'class' : 'org.apache.solr.handler.component.SearchHandler',\n"
                    + "    'appends' : { 'fq' : '{!collapse tag=collapsing field="
                    + bee_si
                    + " sort=\"${collapseSort}\" }' }, \n"
                    + "  }\n"
                    + "}"),
            COLLECTION);

    // add some documents
    {
      new UpdateRequest()
          .add(sdoc(id, 1, bee_si, "bumble bee", forage_t, "nectar"))
          .add(sdoc(id, 2, bee_si, "honey bee", forage_t, "propolis"))
          .add(sdoc(id, 3, bee_si, "solitary bee", forage_t, "pollen"))
          .commit(cluster.getSolrClient(), COLLECTION);
    }

    // compose the query
    final SolrQuery solrQuery = new SolrQuery(bee_si + ":bee");
    solrQuery.setParam(CommonParams.QT, handlerName);
    solrQuery.setParam(CommonParams.SORT, "id desc");
    solrQuery.setParam("collapseSort", "id asc");

    // make the query
    // the query wouold break with macros in shard response
    final QueryResponse queryResponse =
        new QueryRequest(solrQuery).process(cluster.getSolrClient(), COLLECTION);
    assertNotNull(queryResponse);
  }
}

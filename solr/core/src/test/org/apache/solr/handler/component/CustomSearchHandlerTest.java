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

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class CustomSearchHandlerTest extends SolrCloudTestCase {

  /** A custom search handler that uses a custom response builder. */
  public static class CustomSearchHandler extends SearchHandler {
    @Override
    protected ResponseBuilder newResponseBuilder(
        SolrQueryRequest req, SolrQueryResponse rsp, List<SearchComponent> components) {
      return new CustomResponseBuilder(req, rsp, components);
    }
  }

  /** A custom response builder that uses a custom shards info container. */
  private static class CustomResponseBuilder extends ResponseBuilder {

    public CustomResponseBuilder(
        SolrQueryRequest req, SolrQueryResponse rsp, List<SearchComponent> components) {
      super(req, rsp, components);
    }

    @Override
    protected ShardsInfoContainer newShardsInfoContainer() {
      return new CustomShardsInfoContainer();
    }
  }

  /**
   * A container for recording shard to shard info mappings, retaining only the "shardAddress" part
   * of the shard info.
   *
   * @see org.apache.solr.common.params.ShardParams#SHARDS_INFO
   */
  private static class CustomShardsInfoContainer extends ShardsInfoContainer {

    private final List<Object> container = new ArrayList<>();

    @Override
    public void accept(String shardInfoName, NamedList<Object> shardInfoValue) {
      final Object shardAddress = shardInfoValue.remove("shardAddress");
      if (shardAddress != null) {
        container.add(shardAddress);
      }
    }

    @Override
    public Object get() {
      return container;
    }
  }

  private static String COLLECTION;
  private static int NUM_SHARDS;

  @BeforeClass
  public static void setupCluster() throws Exception {

    // decide collection name ...
    COLLECTION = "collection" + (1 + random().nextInt(100));
    // ... and shard/replica/node numbers
    NUM_SHARDS = 1 + random().nextInt(3);
    final int numReplicas = 2;
    final int nodeCount = NUM_SHARDS * numReplicas;

    // create and configure cluster
    configureCluster(nodeCount).addConfig("conf", configset("cloud-dynamic")).configure();

    // create an empty collection
    CollectionAdminRequest.createCollection(COLLECTION, "conf", NUM_SHARDS, numReplicas)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        COLLECTION, cluster.getSolrClient().getZkStateReader(), false, true, DEFAULT_TIMEOUT);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testShardsInfoContainerCustomisation() throws Exception {

    // determine custom search handler name (the exact name should not matter)
    final String customSearchHandlerName = "/custom_select" + random().nextInt();

    // add custom handler
    {
      cluster
          .getSolrClient()
          .request(
              new ConfigRequest(
                  "{\n"
                      + "  'add-requesthandler': {\n"
                      + "    'name' : '"
                      + customSearchHandlerName
                      + "',\n"
                      + "    'class' : '"
                      + CustomSearchHandler.class.getName()
                      + "',\n"
                      + "    'components' : [ '"
                      + QueryComponent.COMPONENT_NAME
                      + "' ]\n"
                      + "  }\n"
                      + "}"),
              COLLECTION);
    }

    // add some documents
    final String id = "id";
    final String t1 = "a_t";
    final String t2 = "b_t";
    {
      new UpdateRequest()
          .add(sdoc(id, 1, t1, "bumble bee", t2, "bumble bee"))
          .add(sdoc(id, 2, t1, "honey bee", t2, "honey bee"))
          .add(sdoc(id, 3, t1, "solitary bee", t2, "solitary bee"))
          .commit(cluster.getSolrClient(), COLLECTION);
    }

    // search for the documents (without or with the custom handler)
    for (String qt : new String[] {null, customSearchHandlerName}) {
      // compose the query
      final SolrQuery solrQuery = new SolrQuery(t1 + ":bee");
      if (qt != null) {
        solrQuery.setRequestHandler(qt);
      }
      solrQuery.add("shards.info", "true");

      // make the query
      final QueryResponse queryResponse =
          new QueryRequest(solrQuery).process(cluster.getSolrClient(), COLLECTION);

      // analyse the response
      Object shardsInfoObj = queryResponse.getResponse().get("shards.info");
      if (qt == null) {
        // default shards info
        NamedList<Object> shardsInfo = (NamedList<Object>) shardsInfoObj;
        assertEquals(NUM_SHARDS, shardsInfo.size());
        for (Object shard : shardsInfo) {
          assertFalse(shard instanceof String);
        }
      } else {
        // custom shards info
        List<Object> shardsInfo = (List<Object>) shardsInfoObj;
        assertEquals(NUM_SHARDS, shardsInfo.size());
        for (Object shard : shardsInfo) {
          assertTrue(shard instanceof String);
        }
      }
    }
  }
}

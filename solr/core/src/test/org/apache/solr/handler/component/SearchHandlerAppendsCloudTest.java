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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.solr.common.params.ShardParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class SearchHandlerAppendsCloudTest extends SolrCloudTestCase {

  private static String COLLECTION;
  private static int NUM_SHARDS;
  private static int NUM_REPLICAS;

  @BeforeClass
  public static void setupCluster() throws Exception {

    // decide collection name ...
    COLLECTION = "collection" + (1 + random().nextInt(100));
    // ... and shard/replica/node numbers
    NUM_SHARDS = (1 + random().nextInt(3)); // 1..3
    NUM_REPLICAS = (1 + random().nextInt(2)); // 1..2

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
    final String bee_t = "bee_t";
    final String forage_t = "forage_t";

    // add custom handlers (the exact custom handler names should not matter)
    final String[] searchHandlerNames =
        new String[] {
          null, "/custom_select" + random().nextInt(), "/custom_select" + random().nextInt()
        };
    final String[] searchHandlerAppends =
        new String[] {
          null,
          "    'appends' : { 'fq' : '-" + forage_t + ":pollen' }, \n",
          "    'appends' : { 'fq' : '-" + forage_t + ":nectar' }, \n",
        };
    for (int ii = 0; ii < searchHandlerNames.length; ++ii) {
      if (searchHandlerNames[ii] != null) {
        cluster
            .getSolrClient()
            .request(
                new ConfigRequest(
                    "{\n"
                        + "  'add-requesthandler': {\n"
                        + "    'name' : '"
                        + searchHandlerNames[ii]
                        + "',\n"
                        + "    'class' : 'org.apache.solr.handler.component.SearchHandler',\n"
                        + searchHandlerAppends[ii]
                        + "  }\n"
                        + "}"),
                COLLECTION);
      }
    }

    // add some documents
    {
      new UpdateRequest()
          .add(sdoc(id, 1, bee_t, "bumble bee", forage_t, "nectar"))
          .add(sdoc(id, 2, bee_t, "honey bee", forage_t, "propolis"))
          .add(sdoc(id, 3, bee_t, "solitary bee", forage_t, "pollen"))
          .commit(cluster.getSolrClient(), COLLECTION);
    }

    // search (with all possible search handler combinations)
    for (boolean shortCircuit : new boolean[] {false, true}) {
      for (boolean withFilterQuery : new boolean[] {false, true}) {
        for (int ii = 0; ii < searchHandlerNames.length; ++ii) {
          for (int jj = 0; jj < searchHandlerNames.length; ++jj) {

            final int numAppendedFilterQueries;
            if (searchHandlerNames[ii] != null
                && searchHandlerNames[jj] != null
                && !searchHandlerNames[ii].equals(searchHandlerNames[jj])) {
              numAppendedFilterQueries = 2; // both handlers appended their filter query
            } else if (searchHandlerNames[ii] != null || searchHandlerNames[jj] != null) {
              numAppendedFilterQueries =
                  1; // one filter query from one handler or the same filter query from both
              // handlers
            } else {
              numAppendedFilterQueries = 0; // no custom handlers, no appended filter queries
            }

            // compose the query
            final SolrQuery solrQuery = new SolrQuery(bee_t + ":bee");
            if (searchHandlerNames[ii] != null) {
              solrQuery.setParam(CommonParams.QT, searchHandlerNames[ii]);
            }
            if (searchHandlerNames[jj] != null) {
              solrQuery.setParam(ShardParams.SHARDS_QT, searchHandlerNames[jj]);
            }
            if (withFilterQuery) {
              solrQuery.setFilterQueries("-" + forage_t + ":water");
            }
            solrQuery.setParam("shortCircuit", shortCircuit);
            solrQuery.setParam(CommonParams.DEBUG, CommonParams.QUERY);

            // make the query
            final QueryResponse queryResponse =
                new QueryRequest(solrQuery).process(cluster.getSolrClient(), COLLECTION);

            // analyse the response
            final StringBuilder contextInfo = new StringBuilder();
            contextInfo.append("COLLECTION=" + COLLECTION);
            contextInfo.append(",NUM_SHARDS=" + NUM_SHARDS);
            contextInfo.append(",NUM_REPLICAS=" + NUM_REPLICAS);
            contextInfo.append(",shortCircuit=" + shortCircuit);
            contextInfo.append(",withFilterQuery=" + withFilterQuery);
            contextInfo.append(",ii=" + ii);
            contextInfo.append(",jj=" + jj);

            final int expectedNumFilterQueries =
                (withFilterQuery ? 1 : 0) + numAppendedFilterQueries;

            assertNotNull(queryResponse);
            final Map<String, Object> debugMap = queryResponse.getDebugMap();
            assertNotNull(debugMap);
            final List<?> filterQueriesList =
                (List<?>) debugMap.getOrDefault("filter_queries", Collections.EMPTY_LIST);
            final Set<?> filterQueriesSet = new HashSet<>(filterQueriesList);

            contextInfo.append(",filterQueriesList=" + filterQueriesList.toString());

            if (searchHandlerNames[ii] != null
                && (searchHandlerNames[jj] == null
                    || searchHandlerNames[jj].equals(searchHandlerNames[ii]))) {
              assertEquals(
                  contextInfo.toString(), expectedNumFilterQueries, filterQueriesSet.size());
              // SOLR-10059: sometimes (but not always) filterQueriesList contains duplicates
              continue;
            }

            assertEquals(
                contextInfo.toString(), expectedNumFilterQueries, filterQueriesList.size());
            assertEquals(contextInfo.toString(), filterQueriesSet.size(), filterQueriesList.size());
          }
        }
      }
    }
  }
}

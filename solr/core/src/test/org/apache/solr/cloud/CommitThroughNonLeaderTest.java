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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A commit sent to a non-leader replica must be forwarded to the leader and distributed back to the
 * replicas, so that every replica can see the committed documents. The forwarded requests carry the
 * string values {@code "leaders"} and {@code "replicas"} in the {@code commit_end_point} parameter;
 * reading that parameter as a boolean breaks the forwarding.
 */
public class CommitThroughNonLeaderTest extends SolrCloudTestCase {

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = DEBUG_LABEL + "_collection";

  private static CloudSolrClient COLLECTION_CLIENT;

  @BeforeClass
  public static void beforeClass() throws Exception {
    final String configName = DEBUG_LABEL + "_config-set";
    final Path configDir = TEST_HOME().resolve("collection1").resolve("conf");

    configureCluster(2).addConfig(configName, configDir).configure();

    Map<String, String> collectionProperties = new LinkedHashMap<>();
    collectionProperties.put("config", "solrconfig.xml");
    collectionProperties.put("schema", "schema.xml");
    CollectionAdminRequest.createCollection(COLLECTION_NAME, configName, 1, 2)
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());

    COLLECTION_CLIENT = cluster.getSolrClient(COLLECTION_NAME);
    AbstractFullDistribZkTestBase.waitForRecoveriesToFinish(
        COLLECTION_NAME, ZkStateReader.from(COLLECTION_CLIENT), false, true, 330);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (null != COLLECTION_CLIENT) {
      COLLECTION_CLIENT.close();
      COLLECTION_CLIENT = null;
    }
  }

  @Test
  public void testCommitSentToNonLeaderIsSeenByAllReplicas() throws Exception {
    DocCollection collection =
        ZkStateReader.from(cluster.getSolrClient())
            .getClusterState()
            .getCollection(COLLECTION_NAME);
    Slice slice = collection.getSlices().iterator().next();
    Replica leader = slice.getLeader();
    Replica notLeader = null;
    for (Replica replica : slice.getReplicas()) {
      if (!replica.getName().equals(leader.getName())) {
        notLeader = replica;
      }
    }
    assertNotNull("expected a non-leader replica in " + slice, notLeader);

    COLLECTION_CLIENT.add(new SolrInputDocument("id", "1", "title", "committed elsewhere"));

    // the config used here has no auto commit, so nothing may be visible yet
    for (Replica replica : slice.getReplicas()) {
      assertEquals(
          "doc visible on " + replica.getName() + " before any commit", 0, queryCore(replica));
    }

    try (HttpSolrClient nonLeaderClient = HttpSolrClient.builder(notLeader.getCoreUrl()).build()) {
      nonLeaderClient.commit();
    }

    for (Replica replica : slice.getReplicas()) {
      assertEquals(
          "doc not visible on " + replica.getName() + " after a commit sent to a non-leader",
          1,
          queryCore(replica));
    }
  }

  private static long queryCore(Replica replica) throws Exception {
    try (HttpSolrClient client = HttpSolrClient.builder(replica.getCoreUrl()).build()) {
      return client.query(new SolrQuery("*:*")).getResults().getNumFound();
    }
  }
}

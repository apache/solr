/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.admin.api;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.NodeApi;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.StrUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration tests for {@link DeleteNode} using the V2 API */
public class DeleteNodeAPITest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(6)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
  }

  @After
  public void clearCollections() throws Exception {
    cluster.deleteAllCollections();
  }

  @Test
  public void testDeleteNode() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    String coll = "deletenodetest_coll";
    Set<String> liveNodes = cloudClient.getClusterStateProvider().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    CollectionAdminRequest.Create create =
        pickRandom(
            CollectionAdminRequest.createCollection(coll, "conf1", 5, 2, 0, 0),
            CollectionAdminRequest.createCollection(coll, "conf1", 5, 1, 1, 0),
            CollectionAdminRequest.createCollection(coll, "conf1", 5, 0, 1, 1),
            // check RF=1
            CollectionAdminRequest.createCollection(coll, "conf1", 5, 1, 0, 0),
            CollectionAdminRequest.createCollection(coll, "conf1", 5, 0, 1, 0));
    create.setCreateNodeSet(StrUtils.join(l, ','));
    cloudClient.request(create);
    String nodeToBeDecommissioned = l.get(0);

    // check what replicas are on the node, and whether the call should fail
    boolean shouldFail = false;
    DocCollection docColl = cloudClient.getClusterStateProvider().getCollection(coll);
    log.info("#### DocCollection: {}", docColl);
    List<Replica> replicas = docColl.getReplicasOnNode(nodeToBeDecommissioned);
    for (Replica replica : replicas) {
      String shard = replica.getShard();
      Slice slice = docColl.getSlice(shard);
      boolean hasOtherNonPullReplicas = false;
      for (Replica r : slice.getReplicas()) {
        if (!r.getName().equals(replica.getName())
            && !r.getNodeName().equals(nodeToBeDecommissioned)
            && r.getType().leaderEligible) {
          hasOtherNonPullReplicas = true;
          break;
        }
      }
      if (!hasOtherNonPullReplicas) {
        shouldFail = true;
        break;
      }
    }

    var request = new NodeApi.DeleteNode(nodeToBeDecommissioned);
    SubResponseAccumulatingJerseyResponse response = request.process(cloudClient);

    if (log.isInfoEnabled()) {
      log.info(
          "####### DocCollection after: {}", cloudClient.getClusterStateProvider().getClusterState().getCollection(coll));
    }

    if (shouldFail) {
      assertNotNull(
          "Expected request to fail, there should be failures sent back",
          response.failedSubResponsesByNodeName);
    } else {
      assertNull(
          "Expected request to not fail, there should be no failures sent back",
          response.failedSubResponsesByNodeName);
    }
  }

  @Test
  public void testDeleteNodeAsync() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    String coll = "deletenodetest_coll_async";
    Set<String> liveNodes = cloudClient.getClusterStateProvider().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    CollectionAdminRequest.Create create =
        pickRandom(
            CollectionAdminRequest.createCollection(coll, "conf1", 5, 2, 0, 0),
            CollectionAdminRequest.createCollection(coll, "conf1", 5, 1, 1, 0),
            CollectionAdminRequest.createCollection(coll, "conf1", 5, 0, 1, 1),
            // check RF=1
            CollectionAdminRequest.createCollection(coll, "conf1", 5, 1, 0, 0),
            CollectionAdminRequest.createCollection(coll, "conf1", 5, 0, 1, 0));
    create.setCreateNodeSet(StrUtils.join(l, ','));
    cloudClient.request(create);
    String nodeToBeDecommissioned = l.get(0);

    // check what replicas are on the node, and whether the call should fail
    boolean shouldFail = false;
    DocCollection docColl = cloudClient.getClusterStateProvider().getCollection(coll);
    log.info("#### DocCollection: {}", docColl);
    List<Replica> replicas = docColl.getReplicasOnNode(nodeToBeDecommissioned);
    for (Replica replica : replicas) {
      String shard = replica.getShard();
      Slice slice = docColl.getSlice(shard);
      boolean hasOtherNonPullReplicas = false;
      for (Replica r : slice.getReplicas()) {
        if (!r.getName().equals(replica.getName())
            && !r.getNodeName().equals(nodeToBeDecommissioned)
            && r.getType().leaderEligible) {
          hasOtherNonPullReplicas = true;
          break;
        }
      }
      if (!hasOtherNonPullReplicas) {
        shouldFail = true;
        break;
      }
    }

    String asyncId = "003";
    var request = new NodeApi.DeleteNode(nodeToBeDecommissioned);
    request.setAsync(asyncId);
    SubResponseAccumulatingJerseyResponse response = request.process(cloudClient);
    assertNull(
        "Expected request to not fail, any failures will be returned in the async status response",
        response.failedSubResponsesByNodeName);

    // Wait for the async request to complete
    CollectionAdminRequest.RequestStatusResponse rsp = waitForAsyncClusterRequest(asyncId, Duration.ofSeconds(5));

    if (log.isInfoEnabled()) {
      log.info(
          "####### DocCollection after: {}", cloudClient.getClusterStateProvider().getClusterState().getCollection(coll));
    }

    if (shouldFail) {
      assertSame(String.valueOf(rsp), RequestStatusState.FAILED, rsp.getRequestStatus());
    } else {
      assertNotSame(String.valueOf(rsp), RequestStatusState.FAILED, rsp.getRequestStatus());
    }
  }
}

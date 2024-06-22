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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.StrUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteNodeTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(6)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
  }

  @Test
  public void test() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    String coll = "deletenodetest_coll";
    ClusterState state = cloudClient.getClusterState();
    Set<String> liveNodes = state.getLiveNodes();
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
    state = cloudClient.getClusterState();
    String nodeToBeDecommissioned = l.get(0);
    // check what replicas are on the node, and whether the call should fail
    boolean shouldFail = false;
    DocCollection docColl = state.getCollection(coll);
    log.info("#### DocCollection: {}", docColl);
    List<Replica> replicas = docColl.getReplicas(nodeToBeDecommissioned);
    if (replicas != null) {
      for (Replica replica : replicas) {
        String shard =
            docColl.getShardId(
                nodeToBeDecommissioned, replica.getStr(ZkStateReader.CORE_NAME_PROP));
        Slice slice = docColl.getSlice(shard);
        boolean hasOtherNonPullReplicas = false;
        for (Replica r : slice.getReplicas()) {
          if (!r.getName().equals(replica.getName())
              && !r.getNodeName().equals(nodeToBeDecommissioned)
              && r.getType() != Replica.Type.PULL) {
            hasOtherNonPullReplicas = true;
            break;
          }
        }
        if (!hasOtherNonPullReplicas) {
          shouldFail = true;
          break;
        }
      }
    }
    new CollectionAdminRequest.DeleteNode(nodeToBeDecommissioned).processAsync("003", cloudClient);
    CollectionAdminRequest.RequestStatus requestStatus =
        CollectionAdminRequest.requestStatus("003");
    CollectionAdminRequest.RequestStatusResponse rsp = null;
    for (int i = 0; i < 200; i++) {
      rsp = requestStatus.process(cloudClient);
      if (rsp.getRequestStatus() == RequestStatusState.FAILED
          || rsp.getRequestStatus() == RequestStatusState.COMPLETED) {
        break;
      }
      Thread.sleep(50);
    }
    if (log.isInfoEnabled()) {
      log.info(
          "####### DocCollection after: {}", cloudClient.getClusterState().getCollection(coll));
    }
    if (shouldFail) {
      assertSame(String.valueOf(rsp), rsp.getRequestStatus(), RequestStatusState.FAILED);
    } else {
      assertNotSame(String.valueOf(rsp), rsp.getRequestStatus(), RequestStatusState.FAILED);
    }
  }
}

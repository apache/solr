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
package org.apache.solr.handler.admin.api;

import java.util.concurrent.TimeUnit;
import org.apache.solr.client.api.model.DeleteClusterCommandStatusResponse;
import org.apache.solr.client.api.model.GetClusterCommandStatusResponse;
import org.apache.solr.client.api.model.GetClusterCommandStatusResponse.CommandStatus.State;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.ClusterApi;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * HTTP tests for {@code GET/DELETE /api/cluster/commands} via the generated SolrJ client classes.
 */
public class ClusterCommandsTest extends SolrCloudTestCase {

  public static final int MAX_WAIT_TIMEOUT = 30;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Test
  public void testGetCommandStatusNotFound() throws Exception {
    GetClusterCommandStatusResponse rsp =
        new ClusterApi.GetClusterCommandStatus("does-not-exist").process(cluster.getSolrClient());

    assertNotNull(rsp);
    assertNull(rsp.error);
    assertEquals(State.NOT_FOUND, rsp.status.state);
    assertEquals("Did not find [does-not-exist] in any tasks queue", rsp.status.msg);
  }

  @Test
  public void testGetAndDeleteSingleCommandStatus() throws Exception {
    final SolrClient client = cluster.getSolrClient();
    final String collection = "cluster-commands-single";
    final String asyncId =
        CollectionAdminRequest.createCollection(collection, "conf1", 1, 1).processAsync(client);

    GetClusterCommandStatusResponse getRsp = waitForCompleted(asyncId, client);
    assertEquals(State.COMPLETED, getRsp.status.state);
    assertEquals("found [" + asyncId + "] in completed tasks", getRsp.status.msg);
    assertTrue(
        "completed create should include sub-responses from the original command",
        getRsp.unknownProperties().containsKey("success")
            || getRsp.unknownProperties().containsKey("failure"));

    DeleteClusterCommandStatusResponse deleteRsp =
        new ClusterApi.DeleteClusterCommandStatus(asyncId).process(client);
    assertEquals("successfully removed stored response for [" + asyncId + "]", deleteRsp.status);

    GetClusterCommandStatusResponse afterDelete =
        new ClusterApi.GetClusterCommandStatus(asyncId).process(client);
    assertEquals(State.NOT_FOUND, afterDelete.status.state);
  }

  @Test
  public void testDeleteUnknownCommandStatus() throws Exception {
    DeleteClusterCommandStatusResponse rsp =
        new ClusterApi.DeleteClusterCommandStatus("foo").process(cluster.getSolrClient());
    assertEquals("[foo] not found in stored responses", rsp.status);
  }

  @Test
  public void testDeleteAllCommandStatuses() throws Exception {
    final SolrClient client = cluster.getSolrClient();
    final String id1 =
        CollectionAdminRequest.createCollection("cluster-commands-flush-1", "conf1", 1, 1)
            .processAsync(client);
    final String id2 =
        CollectionAdminRequest.createCollection("cluster-commands-flush-2", "conf1", 1, 1)
            .processAsync(client);

    waitForCompleted(id1, client);
    waitForCompleted(id2, client);

    DeleteClusterCommandStatusResponse flushRsp =
        new ClusterApi.DeleteAllClusterCommandStatuses().process(client);
    assertEquals("successfully cleared stored collection api responses", flushRsp.status);

    assertEquals(
        State.NOT_FOUND, new ClusterApi.GetClusterCommandStatus(id1).process(client).status.state);
    assertEquals(
        State.NOT_FOUND, new ClusterApi.GetClusterCommandStatus(id2).process(client).status.state);
  }

  private static GetClusterCommandStatusResponse waitForCompleted(String id, SolrClient client)
      throws Exception {
    GetClusterCommandStatusResponse rsp = null;
    long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(MAX_WAIT_TIMEOUT);
    while (System.nanoTime() < endTime) {
      rsp = new ClusterApi.GetClusterCommandStatus(id).process(client);
      State state = rsp.status.state;
      assumeTrue("Error creating collection - skipping test", state != State.FAILED);
      if (state == State.COMPLETED) {
        return rsp;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    assumeTrue(
        "Timed out waiting for async request " + id,
        rsp != null && State.COMPLETED.equals(rsp.status.state));
    return rsp;
  }
}

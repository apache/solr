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
package org.apache.solr.cloud.api.collections;

import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.DistributedLock;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkDistributedCollectionLockFactory;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CollectionParams.LockLevel;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;

/** With Overseer disabled, a command naming an alias locks the collection the alias resolves to. */
public class AliasCollectionApiLockTest extends SolrCloudTestCase {
  private static final String COLLECTION = "aliasLockColl";
  private static final String ALIAS = "aliasLockAlias";
  private static final long WAIT_SECONDS = 30;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .withOverseer(false)
        .configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 2);
    CollectionAdminRequest.createAlias(ALIAS, COLLECTION).process(cluster.getSolrClient());
  }

  @Test
  public void testAliasedCommandWaitsForCollectionLock() throws Exception {
    Slice slice = getCollectionState(COLLECTION).getSlices().iterator().next();
    String replica = slice.getReplicas().iterator().next().getName();
    String asyncId = "deleteReplicaViaAlias";
    DistributedLock held =
        new ZkDistributedCollectionLockFactory(
                cluster.getZkClient(),
                DistributedCollectionConfigSetCommandRunner.ZK_COLLECTION_LOCKS)
            .createLock(true, LockLevel.COLLECTION, COLLECTION, null, null, null);
    held.waitUntilAcquired();
    try {
      new GenericSolrRequest(
              SolrRequest.METHOD.GET,
              "/admin/collections",
              params(
                  CoreAdminParams.ACTION,
                  CollectionAction.DELETEREPLICA.toLower(),
                  CollectionAdminParams.COLLECTION,
                  ALIAS,
                  CoreAdminParams.SHARD,
                  slice.getName(),
                  CoreAdminParams.REPLICA,
                  replica,
                  CollectionAdminParams.FOLLOW_ALIASES,
                  "true",
                  CommonAdminParams.ASYNC,
                  asyncId))
          .process(cluster.getSolrClient());

      String lockDir = held.getLockId().substring(0, held.getLockId().lastIndexOf('/'));
      new TimeOut(WAIT_SECONDS, TimeUnit.SECONDS, TimeSource.NANO_TIME)
          .waitFor(
              "DELETEREPLICA neither waited for nor ran past the lock",
              () -> state(asyncId) != RequestStatusState.SUBMITTED || waiters(lockDir) > 0);
      assertEquals(
          "DELETEREPLICA on " + ALIAS + " ran while " + COLLECTION + " was locked",
          RequestStatusState.SUBMITTED,
          state(asyncId));
    } finally {
      held.release();
    }
    assertEquals(
        RequestStatusState.COMPLETED,
        CollectionAdminRequest.requestStatus(asyncId)
            .waitFor(cluster.getSolrClient(), WAIT_SECONDS));
    assertNull(getCollectionState(COLLECTION).getReplica(replica));
  }

  private static RequestStatusState state(String asyncId) {
    try {
      return CollectionAdminRequest.requestStatus(asyncId)
          .process(cluster.getSolrClient())
          .getRequestStatus();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static int waiters(String lockDir) {
    try {
      return cluster.getZkClient().getChildren(lockDir, null).size() - 1;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

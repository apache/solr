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

import java.util.List;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.DistributedLock;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkDistributedCollectionLockFactory;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CollectionParams.LockLevel;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * With the Overseer disabled, deleting a collection removes the persistent lock dirs that older
 * versions left for it.
 */
public class DeleteCollectionLockCleanupTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .withOverseer(false)
        .configure();
  }

  @Test
  public void testDeleteRemovesLegacyLockDirs() throws Exception {
    String collection = "lockCleanup";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 1);
    String shard = getCollectionState(collection).getSlices().iterator().next().getName();

    SolrZkClient zkClient = cluster.getZkClient();
    ZkDistributedCollectionLockFactory factory =
        new ZkDistributedCollectionLockFactory(
            zkClient, DistributedCollectionConfigSetCommandRunner.ZK_COLLECTION_LOCKS);
    String collLockDir =
        lockDir(factory.createLock(true, LockLevel.COLLECTION, collection, null, null, null));
    String shardLockDir =
        lockDir(factory.createLock(true, LockLevel.SHARD, collection, shard, null, null));
    String collDir = parent(collLockDir);

    zkClient.clean(collDir);
    for (String dir : List.of(collLockDir, shardLockDir)) {
      zkClient.makePath(dir, false);
    }

    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
    assertFalse(collDir + " should have been removed", zkClient.exists(collDir));
  }

  private static String lockDir(DistributedLock lock) {
    lock.release();
    return parent(lock.getLockId());
  }

  private static String parent(String path) {
    return path.substring(0, path.lastIndexOf('/'));
  }
}

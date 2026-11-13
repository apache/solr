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
package org.apache.solr.client.solrj.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

public class ZkClientClusterStateProviderTest extends SolrTestCaseJ4 {

  private record ReadAttempt(String path, String failure) {}

  private static class RecordingSolrZkClient extends SolrZkClient {
    private final List<ReadAttempt> readAttempts = new CopyOnWriteArrayList<>();

    RecordingSolrZkClient(Builder builder) {
      super(builder);
    }

    @Override
    public byte[] getData(String path, Watcher watcher, Stat stat)
        throws KeeperException, InterruptedException {
      try {
        byte[] data = super.getData(path, watcher, stat);
        readAttempts.add(new ReadAttempt(path, null));
        return data;
      } catch (KeeperException | InterruptedException e) {
        readAttempts.add(new ReadAttempt(path, e.getClass().getSimpleName()));
        throw e;
      }
    }

    List<ReadAttempt> getReadAttempts() {
      return List.copyOf(readAttempts);
    }

    void clearReadAttempts() {
      readAttempts.clear();
    }
  }

  @Test
  public void testKnownAliasDoesNotReadAliasCollectionState() throws Exception {
    String collection = "target_" + getSaferTestName();
    String alias = collection + "_alias";
    String aliasCollectionPath = DocCollection.getCollectionPath(alias);
    ZkTestServer server = new ZkTestServer(createTempDir("alias-read-signal"));
    RecordingSolrZkClient zkClient = null;
    ZkStateReader reader = null;
    try {
      server.run();
      zkClient =
          new RecordingSolrZkClient(
              new SolrZkClient.Builder()
                  .withUrl(server.getZkAddress())
                  .withTimeout(30, TimeUnit.SECONDS));
      ZkController.createClusterZkNodes(zkClient);
      zkClient.setData(
          ZkStateReader.ALIASES, Utils.toJSON(Map.of("collection", Map.of(alias, collection))));

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      assertTrue(reader.getAliases().hasAlias(alias));
      ZkClientClusterStateProvider provider = new ZkClientClusterStateProvider(reader);

      // Positive control: this client-specific recorder observes the exact missing path and
      // distinguishes the failed ZooKeeper read from a successful read.
      zkClient.clearReadAttempts();
      RecordingSolrZkClient observedClient = zkClient;
      KeeperException.NoNodeException missing =
          expectThrows(
              KeeperException.NoNodeException.class,
              () -> observedClient.getData(aliasCollectionPath, null, null));
      assertTrue(
          "Unexpected missing path: " + missing.getPath(),
          missing.getPath().endsWith(aliasCollectionPath));
      assertEquals(
          List.of(new ReadAttempt(aliasCollectionPath, "NoNodeException")),
          zkClient.getReadAttempts());

      // With the known-alias guard present, probing the alias must not read its collection-state
      // path. Removing the guard makes this assertion fail with the exact NoNode path recorded.
      zkClient.clearReadAttempts();
      assertNull(provider.getState(alias));
      assertFalse(
          "Known alias triggered a read of "
              + aliasCollectionPath
              + ": "
              + zkClient.getReadAttempts(),
          zkClient.getReadAttempts().stream().anyMatch(a -> aliasCollectionPath.equals(a.path())));
    } finally {
      if (reader != null) {
        reader.close();
      }
      if (zkClient != null) {
        zkClient.close();
      }
      server.shutdown();
    }
  }
}

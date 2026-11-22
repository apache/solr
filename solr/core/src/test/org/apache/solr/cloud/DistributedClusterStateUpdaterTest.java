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

import java.util.Map;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.Compressor;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ZLibCompressor;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Verifies that {@link DistributedClusterStateUpdater} (used when the Overseer is disabled)
 * compresses {@code state.json} above the configured size threshold, the same way {@link
 * org.apache.solr.cloud.overseer.ZkStateWriter} does for the Overseer path. See {@code
 * ZkStateWriterTest#testSingleExternalCollectionCompressedState} for the Overseer equivalent.
 */
public class DistributedClusterStateUpdaterTest extends SolrTestCase {

  private static final int MIN_STATE_BYTE_LEN_FOR_COMPRESSION = 10_000;

  private static ZkTestServer server;
  private static SolrZkClient zkClient;

  @BeforeClass
  public static void startZkServer() throws Exception {
    server = new ZkTestServer(createTempDir("DistributedClusterStateUpdaterTest"));
    server.run();
    zkClient = new SolrZkClient.Builder().withUrl(server.getZkAddress()).build();
    ZkController.createClusterZkNodes(zkClient);
  }

  @AfterClass
  public static void stopZkServer() throws Exception {
    IOUtils.closeWhileHandlingException(zkClient);
    if (server != null) {
      server.shutdown();
    }
  }

  public void testDistributedStateUpdateCompressesLargeStateJson() throws Exception {
    CloudSolrClient cloudSolrClient = null;

    // default compressor impl
    Compressor compressor = new ZLibCompressor();

    try (ZkStateReader reader = new ZkStateReader(zkClient)) {
      reader.createClusterStateWatchersAndUpdate();

      cloudSolrClient =
          new CloudSolrClient.Builder(new ZkClientClusterStateProvider(reader)).build();
      SolrCloudManager scm = new SolrClientCloudManager(cloudSolrClient, null);

      DistributedClusterStateUpdater updater =
          new DistributedClusterStateUpdater(true, MIN_STATE_BYTE_LEN_FOR_COMPRESSION, compressor);

      // small collection: state.json stays under the threshold and is not compressed.
      String smallCollection = "small";
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + smallCollection, true);
      updater.doSingleStateUpdate(
          DistributedClusterStateUpdater.MutatingCommand.ClusterCreateCollection,
          new ZkNodeProps(CommonParams.NAME, smallCollection, ZkStateReader.NUM_SHARDS_PROP, "1"),
          scm,
          reader);

      byte[] smallStateJson =
          zkClient
              .getCuratorFramework()
              .getData()
              // make sure the Curator doesn't decompress it automatically
              .undecompressed()
              .forPath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + smallCollection + "/state.json");

      assertFalse(
          "small state.json should not be compressed",
          compressor.isCompressedBytes(smallStateJson));
      Map<?, ?> smallMap = (Map<?, ?>) Utils.fromJSON(smallStateJson);
      assertNotNull(smallMap.get(smallCollection));

      // large collection: enough replicas added in one batch to push state.json past the
      // compression threshold.
      String bigCollection = "big";
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + bigCollection, true);
      updater.doSingleStateUpdate(
          DistributedClusterStateUpdater.MutatingCommand.ClusterCreateCollection,
          new ZkNodeProps(CommonParams.NAME, bigCollection, ZkStateReader.NUM_SHARDS_PROP, "1"),
          scm,
          reader);

      DistributedClusterStateUpdater.StateChangeRecorder recorder =
          updater.createStateChangeRecorder(bigCollection, false);
      for (int i = 0; i < 300; i++) {
        recorder.record(
            DistributedClusterStateUpdater.MutatingCommand.SliceAddReplica,
            new ZkNodeProps(
                ZkStateReader.COLLECTION_PROP,
                bigCollection,
                ZkStateReader.SHARD_ID_PROP,
                "shard1",
                ZkStateReader.CORE_NODE_NAME_PROP,
                "core_node" + i,
                ZkStateReader.CORE_NAME_PROP,
                "core_node" + i,
                ZkStateReader.NODE_NAME_PROP,
                "127.0.0.1:8983_solr",
                ZkStateReader.STATE_PROP,
                Replica.State.ACTIVE.toString(),
                ZkStateReader.REPLICA_TYPE,
                Replica.Type.NRT.toString()));
      }
      recorder.executeStateUpdates(scm, reader);

      byte[] bigStateJsonRaw =
          zkClient
              .getCuratorFramework()
              .getData()
              .undecompressed()
              .forPath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + bigCollection + "/state.json");
      assertTrue(
          "big state.json should have been compressed by DistributedClusterStateUpdater",
          compressor.isCompressedBytes(bigStateJsonRaw));

      Map<?, ?> bigMap = (Map<?, ?>) Utils.fromJSON(compressor.decompressBytes(bigStateJsonRaw));
      assertNotNull(bigMap.get(bigCollection));

      // reading through the normal (automatically decompressing) path yields the same, uncompressed
      // JSON.
      byte[] bigStateJsonDecoded =
          zkClient.getData(
              ZkStateReader.COLLECTIONS_ZKNODE + "/" + bigCollection + "/state.json", null, null);
      assertFalse(compressor.isCompressedBytes(bigStateJsonDecoded));
      bigMap = (Map<?, ?>) Utils.fromJSON(bigStateJsonDecoded);
      assertNotNull(bigMap.get(bigCollection));
    } finally {
      IOUtils.closeWhileHandlingException(cloudSolrClient);
    }
  }

  public void testDistributedStateUpdateDisabledCompression() throws Exception {
    CloudSolrClient cloudSolrClient = null;

    Compressor compressor = new ZLibCompressor();

    try (ZkStateReader reader = new ZkStateReader(zkClient)) {
      reader.createClusterStateWatchersAndUpdate();

      cloudSolrClient =
          new CloudSolrClient.Builder(new ZkClientClusterStateProvider(reader)).build();
      SolrCloudManager scm = new SolrClientCloudManager(cloudSolrClient, null);

      // negative threshold: compression must never kick in, no matter the state.json size.
      DistributedClusterStateUpdater updater =
          new DistributedClusterStateUpdater(true, -1, compressor);

      String bigCollection = "bigNoCompression";
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + bigCollection, true);
      updater.doSingleStateUpdate(
          DistributedClusterStateUpdater.MutatingCommand.ClusterCreateCollection,
          new ZkNodeProps(CommonParams.NAME, bigCollection, ZkStateReader.NUM_SHARDS_PROP, "1"),
          scm,
          reader);

      DistributedClusterStateUpdater.StateChangeRecorder recorder =
          updater.createStateChangeRecorder(bigCollection, false);
      for (int i = 0; i < 300; i++) {
        recorder.record(
            DistributedClusterStateUpdater.MutatingCommand.SliceAddReplica,
            new ZkNodeProps(
                ZkStateReader.COLLECTION_PROP,
                bigCollection,
                ZkStateReader.SHARD_ID_PROP,
                "shard1",
                ZkStateReader.CORE_NODE_NAME_PROP,
                "core_node" + i,
                ZkStateReader.CORE_NAME_PROP,
                "core_node" + i,
                ZkStateReader.NODE_NAME_PROP,
                "127.0.0.1:8983_solr",
                ZkStateReader.STATE_PROP,
                Replica.State.ACTIVE.toString(),
                ZkStateReader.REPLICA_TYPE,
                Replica.Type.NRT.toString()));
      }
      recorder.executeStateUpdates(scm, reader);

      byte[] bigStateJsonRaw =
          zkClient
              .getCuratorFramework()
              .getData()
              // make sure the Curator doesn't decompress it automatically
              .undecompressed()
              .forPath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + bigCollection + "/state.json");
      assertFalse(
          "state.json must stay uncompressed when minStateByteLenForCompression is negative,"
              + " regardless of size",
          compressor.isCompressedBytes(bigStateJsonRaw));

      Map<?, ?> bigMap = (Map<?, ?>) Utils.fromJSON(bigStateJsonRaw);
      assertNotNull(bigMap.get(bigCollection));
    } finally {
      IOUtils.closeWhileHandlingException(cloudSolrClient);
    }
  }
}

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for SolrCloud tests that use embedded ZooKeeper running in quorum mode
 *
 * <p>This class extends {@link SolrCloudTestCase} to provide a test cluster where each Solr node
 * runs its own embedded ZooKeeper server, forming a ZK quorum. This tests the embedded ZK quorum
 * functionality and ensures proper resource management.
 *
 * <p>Derived tests should call {@link #configureClusterWithEmbeddedZkQuorum(int)} in a {@code
 * BeforeClass} static method:
 *
 * <pre>
 *   <code>
 *   {@literal @}BeforeClass
 *   public static void setupCluster() throws Exception {
 *     cluster = configureClusterWithEmbeddedZkQuorum(3)
 *        .addConfig("configname", pathToConfig)
 *        .build();
 *   }
 *   </code>
 * </pre>
 */
public class SolrCloudWithEmbeddedZkQuorumTestCase extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Configure a cluster where each node runs embedded ZooKeeper in quorum mode.
   *
   * <p>This method sets up a SolrCloud cluster using {@link MiniSolrCloudCluster} with embedded ZK
   * quorum mode enabled. Each Solr node will run its own embedded ZooKeeper server, and together
   * they form a quorum.
   *
   * <p>The ZK client port for each node will be (Solr port + 1000), and the quorum will be
   * established based on the zkHost string containing all nodes.
   *
   * @param nodeCount the number of nodes in the cluster (should be odd: 3, 5, 7, etc.)
   * @return a Builder for further configuration
   */
  protected static MiniSolrCloudCluster.Builder configureClusterWithEmbeddedZkQuorum(
      int nodeCount) {
    if (nodeCount < 3) {
      throw new IllegalArgumentException(
          "ZooKeeper quorum requires at least 3 nodes, got: " + nodeCount);
    }
    if (nodeCount % 2 == 0) {
      log.warn(
          "ZooKeeper quorum works best with odd number of nodes. You specified: {}", nodeCount);
    }

    configurePrsDefault();

    return new MiniSolrCloudCluster.Builder(nodeCount, createTempDir()).withEmbeddedZkQuorum();
  }
}

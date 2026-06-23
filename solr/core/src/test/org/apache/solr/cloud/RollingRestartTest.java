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
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.Nightly
public class RollingRestartTest extends SolrCloudBridgeTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long MAX_WAIT_TIME = TimeUnit.NANOSECONDS.convert(15, TimeUnit.SECONDS);

  @BeforeClass
  public static void beforeRollingRestartTest() {
    sliceCount = 2;
    numJettys = TEST_NIGHTLY ? 16 : 2;
  }

  @Test
  public void test() throws Exception {
    restartTest();
  }

  public void restartTest() throws Exception {
    String leader = OverseerCollectionConfigSetProcessor
        .getLeaderNode(cloudClient.getZkStateReader().getZkClient());
    assertNotNull(leader);
    log.info("Current overseer leader = {}", leader);

    int numRestarts = 1 + random().nextInt(TEST_NIGHTLY ? 12 : 2);
    for (int i = 0; i < numRestarts; i++) {
      log.info("Rolling restart #{}", i + 1);
      for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
        log.info("Restarting {}", jetty.getNodeName());
        jetty.stop();
        cloudClient.getZkStateReader().updateLiveNodes();

        leader = OverseerCollectionConfigSetProcessor
            .getLeaderNode(cloudClient.getZkStateReader().getZkClient());
        if (leader == null) {
          try {
            log.error("NOOVERSEER election queue is : {}",
                OverseerCollectionConfigSetProcessor.getSortedElectionNodes(
                    cloudClient.getZkStateReader().getZkClient(),
                    Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE));
          } catch (KeeperException.NoNodeException e) {
            log.error("NOOVERSEER election node not yet present: {}", e.getMessage());
          }
        }

        jetty.start();

        leader = OverseerCollectionConfigSetProcessor
            .getLeaderNode(cloudClient.getZkStateReader().getZkClient());
        if (leader == null) {
          try {
            log.error("NOOVERSEER election queue is :{}",
                OverseerCollectionConfigSetProcessor.getSortedElectionNodes(
                    cloudClient.getZkStateReader().getZkClient(),
                    Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE));
          } catch (KeeperException.NoNodeException e) {
            log.error("NOOVERSEER election node not yet present: {}", e.getMessage());
          }
          fail("No overseer leader found after restart #" + (i + 1) + ": " + leader);
        }

        cloudClient.getZkStateReader().updateLiveNodes();
      }
    }

    leader = OverseerCollectionConfigSetProcessor
        .getLeaderNode(cloudClient.getZkStateReader().getZkClient());
    assertNotNull(leader);
    log.info("Current overseer leader (after restart) = {}", leader);
  }
}

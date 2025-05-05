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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollingRestartTest extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long MAX_WAIT_TIME = TimeUnit.NANOSECONDS.convert(300, TimeUnit.SECONDS);

  public RollingRestartTest() {
    sliceCount = 2;
    fixShardCount(TEST_NIGHTLY ? 16 : 2);
  }

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    useFactory("solr.StandardDirectoryFactory");
  }

  @Test
  public void test() throws Exception {
    if (new CollectionAdminRequest.RequestApiDistributedProcessing()
        .process(cloudClient)
        .getIsCollectionApiDistributed()) {
      log.info("Skipping test because Collection API is distributed");
      return;
    }

    waitForRecoveriesToFinish(false);

    restartWithRolesTest();

    waitForRecoveriesToFinish(false);
  }

  public void restartWithRolesTest() throws Exception {
    ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);
    String leader = OverseerCollectionConfigSetProcessor.getLeaderNode(zkStateReader.getZkClient());
    assertNotNull(leader);
    log.info("Current overseer leader = {}", leader);

    zkStateReader.getZkClient().printLayoutToStream(System.out);

    int numDesignateOverseers = TEST_NIGHTLY ? 16 : 2;
    numDesignateOverseers = Math.max(getShardCount(), numDesignateOverseers);
    List<String> designates = new ArrayList<>();
    List<CloudJettyRunner> designateJettys = new ArrayList<>();
    for (int i = 0; i < numDesignateOverseers; i++) {
      int n = random().nextInt(getShardCount());
      String nodeName = cloudJettys.get(n).nodeName;
      log.info("Chose {} as overseer designate", nodeName);
      CollectionAdminRequest.addRole(nodeName, "overseer").process(cloudClient);
      designates.add(nodeName);
      designateJettys.add(cloudJettys.get(n));
    }

    waitUntilOverseerDesignateIsLeader(zkStateReader.getZkClient(), designates);

    zkStateReader.getZkClient().printLayoutToStream(System.out);

    boolean sawLiveDesignate = false;
    int numRestarts = 1 + random().nextInt(TEST_NIGHTLY ? 12 : 2);
    for (int i = 0; i < numRestarts; i++) {
      log.info("Rolling restart #{}", i + 1); // nowarn
      for (CloudJettyRunner cloudJetty : designateJettys) {
        log.info("Restarting {}", cloudJetty);
        chaosMonkey.stopJetty(cloudJetty);
        zkStateReader.updateLiveNodes();
        boolean liveDesignates =
            zkStateReader.getClusterState().getLiveNodes().stream().anyMatch(designates::contains);
        if (liveDesignates) {
          boolean success =
              waitUntilOverseerDesignateIsLeader(zkStateReader.getZkClient(), designates);
          if (!success) {
            leader =
                OverseerCollectionConfigSetProcessor.getLeaderNode(zkStateReader.getZkClient());
            if (leader == null)
              log.error(
                  "NOOVERSEER election queue is : {}",
                  OverseerCollectionConfigSetProcessor.getSortedElectionNodes(
                      zkStateReader.getZkClient(), "/overseer_elect/election"));
            fail("No overseer designate as leader found after restart #" + (i + 1) + ": " + leader);
          }
        }
        cloudJetty.jetty.start();
        boolean success =
            waitUntilOverseerDesignateIsLeader(zkStateReader.getZkClient(), designates);
        if (!success) {
          leader = OverseerCollectionConfigSetProcessor.getLeaderNode(zkStateReader.getZkClient());
          if (leader == null)
            log.error(
                "NOOVERSEER election queue is :{}",
                OverseerCollectionConfigSetProcessor.getSortedElectionNodes(
                    zkStateReader.getZkClient(), "/overseer_elect/election"));
          fail("No overseer leader found after restart #" + (i + 1) + ": " + leader);
        }

        zkStateReader.updateLiveNodes();
        sawLiveDesignate =
            zkStateReader.getClusterState().getLiveNodes().stream().anyMatch(designates::contains);
      }
    }

    assertTrue("Test may not be working if we never saw a live designate", sawLiveDesignate);

    leader = OverseerCollectionConfigSetProcessor.getLeaderNode(zkStateReader.getZkClient());
    assertNotNull(leader);
    log.info("Current overseer leader (after restart) = {}", leader);

    zkStateReader.getZkClient().printLayoutToStream(System.out);
  }

  static boolean waitUntilOverseerDesignateIsLeader(
      SolrZkClient testZkClient, List<String> overseerDesignates)
      throws KeeperException, InterruptedException {
    long now = System.nanoTime();
    // the maximum amount of time we're willing to wait to see the designate as leader
    long maxTimeout = now + RollingRestartTest.MAX_WAIT_TIME;
    long timeout = now + TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
    boolean firstTime = true;
    int stableCheckTimeout = 2000;
    String oldleader = null;
    while (System.nanoTime() < timeout && System.nanoTime() < maxTimeout) {
      String newLeader = OverseerCollectionConfigSetProcessor.getLeaderNode(testZkClient);
      if (newLeader != null && !newLeader.equals(oldleader)) {
        // the leaders have changed, let's move the timeout further
        timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
        log.info(
            "oldLeader={} newLeader={} - Advancing timeout to: {}", oldleader, newLeader, timeout);
        oldleader = newLeader;
      }
      if (!overseerDesignates.contains(newLeader)) {
        Thread.sleep(500);
      } else {
        if (firstTime) {
          firstTime = false;
          Thread.sleep(stableCheckTimeout);
        } else {
          return true;
        }
      }
    }
    if (System.nanoTime() < maxTimeout) {
      log.error("Max wait time exceeded");
    }
    return false;
  }
}

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

import static org.apache.solr.cloud.OverseerCollectionConfigSetProcessor.getLeaderNode;
import static org.apache.solr.cloud.OverseerTaskProcessor.getSortedElectionNodes;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverseerRolesTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    // SolrCloudTestCase randomises solr.cloud.overseer.enabled; this test is about the Overseer
    // election, so pin it on rather than skipping half the runs.
    configureCluster(4)
        .withOverseer(true)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  public static void waitForNewOverseer(
      int seconds, Predicate<String> state, boolean failOnIntermediateTransition) throws Exception {
    TimeOut timeout = new TimeOut(seconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    String current = null;
    while (timeout.hasTimedOut() == false) {
      String prev = current;
      current = OverseerCollectionConfigSetProcessor.getLeaderNode(zkClient());
      if (state.test(current)) return;
      else if (failOnIntermediateTransition) {
        if (prev != null && current != null && !current.equals(prev)) {
          fail(
              "There was an intermediate transition, previous: "
                  + prev
                  + ", intermediate transition: "
                  + current);
        }
      }
      Thread.sleep(100);
    }
    fail("Timed out waiting for overseer state change. The current overseer is: " + current);
  }

  public static void waitForNewOverseer(
      int seconds, String expected, boolean failOnIntermediateTransition) throws Exception {
    log.info("Expecting node: {}", expected);
    waitForNewOverseer(seconds, s -> Objects.equals(s, expected), failOnIntermediateTransition);
  }

  private JettySolrRunner getOverseerJetty() throws Exception {
    String overseer = getLeaderNode(zkClient());
    URI overseerUrl = URI.create("http://" + overseer.substring(0, overseer.indexOf('_')));
    int hostPort = overseerUrl.getPort();
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      try {
        if (jetty.getBaseUrl().getPort() == hostPort) return jetty;
      } catch (IllegalStateException e) {

      }
    }
    fail("Couldn't find overseer node " + overseer);
    return null; // to keep the compiler happy
  }

  private void logOverseerState() throws KeeperException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info("Overseer: {}", getLeaderNode(zkClient()));
      log.info(
          "Election queue: {}",
          getSortedElectionNodes(zkClient(), "/overseer_elect/election")); // nowarn
    }
  }

  /**
   * A node started with {@code -Dsolr.node.roles=overseer:preferred} must become the Overseer
   * without waiting for the current Overseer to restart.
   */
  @Test
  public void testPreferredOverseerNodeRoleTakesOver() throws Exception {
    assertFalse(
        "the Overseer must be enabled for this test",
        new CollectionAdminRequest.RequestApiDistributedProcessing()
            .process(cluster.getSolrClient())
            .getIsCollectionApiDistributed());
    logOverseerState();
    final String overseerBefore = getLeaderNode(zkClient());
    assertNotNull("no Overseer to start from", overseerBefore);

    final JettySolrRunner preferred;
    System.setProperty(NodeRoles.NODE_ROLES_PROP, "data:on,overseer:preferred");
    try {
      preferred = cluster.startJettySolrRunner();
    } finally {
      System.clearProperty(NodeRoles.NODE_ROLES_PROP);
    }
    final String preferredNodeName = preferred.getNodeName();
    log.info("Started {} as a preferred overseer", preferredNodeName);

    assertEquals(
        "the new node did not take the preferred overseer role",
        NodeRoles.MODE_PREFERRED,
        preferred.getCoreContainer().nodeRoles.getRoleMode(NodeRoles.Role.OVERSEER));

    // the node published its role and nudged the Overseer, so it must take over
    waitForNewOverseer(30, preferredNodeName, false);
    logOverseerState();
    assertEquals(
        "the preferred node should be the Overseer", preferredNodeName, getLeaderNode(zkClient()));

    // and it must still be reachable as an ordinary node
    assertTrue(
        cluster.getZkStateReader().getClusterState().getLiveNodes().contains(preferredNodeName));

    cluster.stopJettySolrRunner(preferred);
    cluster.waitForJettyToStop(preferred);
    waitForNewOverseer(30, s -> s != null && !s.equals(preferredNodeName), false);
    logOverseerState();
  }
}

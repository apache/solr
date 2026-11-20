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

import java.util.List;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.ZookeeperEnsembleApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

/** Exercises ensemble retirement over HTTP using the generated v2 client. */
@SuppressSSL
public class ZooKeeperEnsembleAPITest extends SolrCloudTestCase {
  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("zookeeper.jmx.log4j.disable", "true");
    configureCluster(3).withEmbeddedZkQuorum().configure();
  }

  @Test
  public void testRetirement() throws Exception {
    var client = cluster.getJettySolrRunner(0).getSolrClient();
    var initial = new ZookeeperEnsembleApi.ListZooKeeperMembers().process(client);
    assertEquals(3, initial.members.size());
    var fourth = cluster.addEmbeddedZkQuorumNode();
    var fifth = cluster.addEmbeddedZkQuorumNode();
    var current = new ZookeeperEnsembleApi.ListZooKeeperMembers().process(client);
    assertEquals(5, current.members.size());
    for (var runner : List.of(fifth, fourth)) {
      var target =
          current.members.stream()
              .filter(m -> m.quorumPort == runner.getLocalPort() + 1010)
              .findFirst()
              .orElseThrow();
      var removal = new ZookeeperEnsembleApi.DeleteZooKeeperMember(target.id.toString());
      removal.setVersion(current.version);
      var updated = removal.process(client);
      assertEquals(current.members.size() - 1, updated.members.size());
      assertNotEquals(current.version, updated.version);
      assertTrue(runner.isRunning());
      assertFalse(updated.members.stream().anyMatch(m -> m.id.equals(target.id)));
      assertEquals(
          updated.version, new ZookeeperEnsembleApi.ListZooKeeperMembers().process(client).version);
      current = updated;
    }
    assertEquals(3, current.members.size());
  }

  @Test
  public void testValidation() throws Exception {
    var client = cluster.getJettySolrRunner(0).getSolrClient();
    assertEquals(
        400,
        expectThrows(
                RemoteSolrException.class,
                () -> new ZookeeperEnsembleApi.DeleteZooKeeperMember("no").process(client))
            .code());
    assertEquals(
        404,
        expectThrows(
                RemoteSolrException.class,
                () -> new ZookeeperEnsembleApi.DeleteZooKeeperMember("2147483647").process(client))
            .code());
    var stale = new ZookeeperEnsembleApi.DeleteZooKeeperMember("1");
    stale.setVersion("0");
    assertEquals(409, expectThrows(RemoteSolrException.class, () -> stale.process(client)).code());
  }
}
